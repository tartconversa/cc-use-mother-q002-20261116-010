from __future__ import annotations

import json

from .landing import Landing
from .ledger import Ledger
from .model import CORRECTION_ISSUE_CODES, LakeError, RawEvent
from .store import Store


class Corrections:
    """Applies weigh corrections as an append-only supersession chain.

    Original weighing facts are never deleted or rewritten: every accepted
    correction inserts a replacement fact, marks its predecessor as
    ``superseded`` and records one ``weigh_corrections`` chain node. Corrections
    whose predecessor is missing or belongs to a competing chain are
    quarantined (never silently dropped) and are replayed automatically once the
    chain heals.
    """

    def __init__(self, store: Store, ledger: Ledger, landing: Landing):
        self.store, self.ledger, self.landing = store, ledger, landing

    # ------------------------------------------------------------------ apply
    def apply(self, ingestion_id: int, event: RawEvent, created: bool, trigger_heal: bool = True):
        try:
            with self.store.write() as conn:
                row = self.store.one(conn, "SELECT * FROM ingestions WHERE id=?", (ingestion_id,))
                if row is None:
                    raise LakeError("unknown_ingestion", "ingestion not found", 404)
                if row["state"] == "normalized":
                    return {"ingestion_id": ingestion_id, "fact_id": row["normalized_id"],
                            "state": "normalized", "created": False}
                result = self._apply_locked(conn, ingestion_id, event)
            result["created"] = created
            if trigger_heal:
                result["healed"] = self.heal_pending()
            return result
        except LakeError as error:
            self.landing.quarantine(ingestion_id, error)
            return {"ingestion_id": ingestion_id, "state": "quarantined", "created": created,
                    "issue": {"code": error.code, "detail": error.detail}}

    def _apply_locked(self, conn, ingestion_id: int, event: RawEvent):
        target = self._resolve_predecessor(conn, event)
        if target["event_type"] not in {"weigh_in", "weigh_out"}:
            raise LakeError("correction_target_invalid",
                            "corrections only apply to scale weigh_in/weigh_out tickets", 422)
        if target["channel"] != event.channel:
            raise LakeError("correction_target_invalid",
                            "correction channel must match the original weighing channel", 422)
        if event.weight_kg == target["weight_kg"] and event.batch_id == target["batch_id"]:
            raise LakeError("correction_unchanged",
                            "corrected weighing is identical to the active ticket", 422)

        root_fact_id = target["root_fact_id"] or target["id"]
        replacement = self._replacement_event(event, target)
        dims = json.loads(target["dimensions_json"])
        # Re-resolve dimensions when the correction moves the weighing to a
        # different batch; otherwise keep the dimension versions frozen on the
        # original ticket so an effective-dimension change cannot alter history.
        if replacement.batch_id != target["batch_id"]:
            dims = self.ledger.dimensions.resolve_event(conn, replacement)
        previous_nodes = self.store.all(
            conn, "SELECT id FROM weigh_corrections WHERE root_fact_id=?", (root_fact_id,))
        sequence_no = len(previous_nodes) + 1
        fact_id = self.ledger.insert_fact(
            conn, ingestion_id, replacement, dims, state="active",
            supersedes_fact_id=target["id"], root_fact_id=root_fact_id,
            external_id=target["external_id"])
        updated = conn.execute(
            "UPDATE facts SET state='superseded' WHERE id=? AND state='active'",
            (target["id"],))
        if updated.rowcount != 1:
            raise LakeError("correction_chain_diverged",
                            "predecessor was superseded concurrently", 409)
        received_at = self.store.one(
            conn, "SELECT received_at FROM ingestions WHERE id=?", (ingestion_id,))["received_at"]
        conn.execute(
            "INSERT INTO weigh_corrections(ingestion_id,replacement_fact_id,superseded_fact_id,"
            "root_fact_id,source,corrects_external_id,correction_external_id,corrected_at,"
            "received_at,sequence_no,state) VALUES(?,?,?,?,?,?,?,?,?,?, 'active')",
            (ingestion_id, fact_id, target["id"], root_fact_id, event.source,
             event.corrects_external_id, event.external_id, event.corrected_at,
             received_at, sequence_no))
        conn.execute(
            "UPDATE ingestions SET state='normalized', issue=NULL, normalized_id=? WHERE id=?",
            (fact_id, ingestion_id))
        self.store.audit(conn, "weigh_correction_applied", str(fact_id),
                         {"ingestion_id": ingestion_id, "superseded_fact_id": target["id"],
                          "root_fact_id": root_fact_id, "sequence_no": sequence_no,
                          "corrected_at": event.corrected_at})
        return {"ingestion_id": ingestion_id, "fact_id": fact_id, "state": "normalized",
                "superseded_fact_id": target["id"], "root_fact_id": root_fact_id,
                "sequence_no": sequence_no}

    def _resolve_predecessor(self, conn, event: RawEvent) -> dict:
        """Find the active fact this correction supersedes.

        Two predecessor styles are supported:

        * ``corrects_external_id`` names the original scale ticket: valid only
          while that ticket is still the active head. Once superseded, a second
          document naming the same ticket is a competing branch and is
          quarantined as ``correction_chain_diverged`` instead of forking.
        * it names a prior correction document: follow that document's
          replacement fact; if the document has not arrived yet the chain is
          broken (``correction_chain_broken``) and the correction stays
          quarantined until the predecessor lands, then heals automatically.
        """
        referenced = self.store.one(
            conn,
            "SELECT * FROM facts WHERE source=? AND external_id=? ORDER BY id LIMIT 1",
            (event.source, event.corrects_external_id))
        if referenced is not None:
            if referenced["state"] == "superseded":
                raise LakeError("correction_chain_diverged",
                                "original ticket already superseded; new corrections must name the "
                                "current correction document", 409)
            return referenced
        node = self.store.one(
            conn,
            "SELECT * FROM weigh_corrections WHERE source=? AND correction_external_id=?",
            (event.source, event.corrects_external_id))
        if node is None:
            raise LakeError("correction_chain_broken",
                            f"predecessor correction {event.corrects_external_id} is not normalized",
                            409)
        target = self.store.one(conn, "SELECT * FROM facts WHERE id=?",
                                (node["replacement_fact_id"],))
        if target["state"] == "superseded":
            head = self.store.one(
                conn, "SELECT * FROM facts WHERE root_fact_id=? AND state='active' ORDER BY id DESC",
                (target["root_fact_id"] or target["id"],))
            head_node = self.store.one(
                conn, "SELECT * FROM weigh_corrections WHERE replacement_fact_id=?",
                (head["id"],)) if head else None
            if head is None or head_node is None:
                raise LakeError("correction_chain_broken",
                                "predecessor chain has no active head", 409)
            if head_node["correction_external_id"] != event.external_id:
                raise LakeError("correction_chain_diverged",
                                "predecessor correction was already superseded by a different document",
                                409)
            target = head
        return target

    @staticmethod
    def _replacement_event(event: RawEvent, target: dict) -> RawEvent:
        return RawEvent(
            source=target["source"], external_id=target["external_id"],
            event_type=target["event_type"], happened_at=target["happened_at"],
            batch_id=event.batch_id, material_id=target["material_id"],
            device_id=target["device_id"], station_id=target["station_id"],
            actor_id=target["actor_id"], weight_kg=event.weight_kg,
            payout_yuan=target["payout_yuan"], channel=target["channel"],
            metadata=event.metadata or {},
        )

    # ------------------------------------------------------------------ heal
    def heal_pending(self) -> list[dict]:
        """Replay quarantined corrections whose chain may now be complete.

        Nodes are retried in dependency order: a correction whose predecessor
        is still quarantined stays pending and gets another pass once the
        predecessor heals. Runs until a full pass makes no progress so chains
        of any depth heal in one call, and genuinely broken chains never block.
        """
        healed = []
        while True:
            with self.store.connect() as conn:
                pending = self.store.all(
                    conn,
                    "SELECT i.id AS ingestion_id, i.issue AS issue FROM ingestions i "
                    "WHERE i.state='quarantined' AND json_extract(i.raw_json,'$.event_type')='weigh_correction' "
                    "ORDER BY i.id")
            progressed = False
            for item in pending:
                issue = json.loads(item["issue"]) if item["issue"] else {}
                if issue.get("code") not in CORRECTION_ISSUE_CODES:
                    continue
                raw = self.store.ingestion(item["ingestion_id"])["raw"]
                event = RawEvent.parse(raw)
                outcome = self.apply(item["ingestion_id"], event, created=False, trigger_heal=False)
                if outcome["state"] == "normalized":
                    healed.append(outcome)
                    progressed = True
            if not progressed:
                break
        if healed:
            self._rechain_sequences()
        return healed

    def _rechain_sequences(self):
        """Renumber chain positions by document issue time after late healing."""
        with self.store.write() as conn:
            roots = [row["root_fact_id"] for row in self.store.all(
                conn, "SELECT DISTINCT root_fact_id FROM weigh_corrections ORDER BY root_fact_id")]
            for root in roots:
                nodes = self.store.all(
                    conn,
                    "SELECT id FROM weigh_corrections WHERE root_fact_id=? "
                    "ORDER BY corrected_at, ingestion_id", (root,))
                for position, node in enumerate(nodes, start=1):
                    conn.execute("UPDATE weigh_corrections SET sequence_no=? WHERE id=?",
                                 (position, node["id"]))

    # ----------------------------------------------------------------- query
    def chain(self, fact_id: int):
        fact = self.store.fact(fact_id)
        root_id = fact["root_fact_id"] or fact["id"]
        root = self.store.fact(root_id)
        with self.store.connect() as conn:
            nodes = self.store.all(
                conn,
                "SELECT wc.*, i.received_at AS landing_received_at FROM weigh_corrections wc "
                "JOIN ingestions i ON i.id=wc.ingestion_id "
                "WHERE wc.root_fact_id=? ORDER BY wc.sequence_no, wc.id", (root_id,))
        with self.store.connect() as conn:
            head = self.store.one(
                conn, "SELECT * FROM facts WHERE root_fact_id=? AND state='active' ORDER BY id DESC",
                (root_id,))
        versions = [{
            "fact_id": root["id"], "external_id": root["external_id"],
            "event_type": root["event_type"], "weight_kg": root["weight_kg"],
            "batch_id": root["batch_id"], "happened_at": root["happened_at"],
            "version": 0, "state": root["state"],
            "supersedes_fact_id": root["supersedes_fact_id"],
        }]
        for node in nodes:
            replacement = self.store.fact(node["replacement_fact_id"])
            versions.append({
                "fact_id": replacement["id"], "external_id": replacement["external_id"],
                "event_type": replacement["event_type"], "weight_kg": replacement["weight_kg"],
                "batch_id": replacement["batch_id"], "happened_at": replacement["happened_at"],
                "version": node["sequence_no"], "state": replacement["state"],
                "supersedes_fact_id": replacement["supersedes_fact_id"],
                "correction": {
                    "ingestion_id": node["ingestion_id"],
                    "correction_external_id": node["correction_external_id"],
                    "corrects_external_id": node["corrects_external_id"],
                    "corrected_at": node["corrected_at"],
                    "received_at": node["landing_received_at"],
                },
            })
        return {"root_fact_id": root_id, "current_fact_id": head["id"] if head else None,
                "current_weight_kg": head["weight_kg"] if head else None,
                "current_state": head["state"] if head else None,
                "versions": versions}

    def pending(self):
        with self.store.connect() as conn:
            rows = self.store.all(
                conn,
                "SELECT i.id AS ingestion_id, i.issue AS issue, "
                "json_extract(i.raw_json,'$.source') AS source, "
                "json_extract(i.raw_json,'$.external_id') AS correction_external_id, "
                "json_extract(i.raw_json,'$.corrects_external_id') AS corrects_external_id, "
                "json_extract(i.raw_json,'$.corrected_at') AS corrected_at "
                "FROM ingestions i WHERE i.state='quarantined' AND "
                "json_extract(i.raw_json,'$.event_type')='weigh_correction' ORDER BY i.id")
        for row in rows:
            row["issue"] = json.loads(row["issue"]) if row["issue"] else None
        return rows
