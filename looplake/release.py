from __future__ import annotations

import json

from .mart import Mart
from .model import LakeError, digest, key, now
from .reconcile import Reconciliation
from .store import Store


class Release:
    def __init__(self, store: Store, mart: Mart, reconciliation: Reconciliation, inventory=None):
        self.store, self.mart, self.reconciliation = store, mart, reconciliation
        self.inventory = inventory

    def _corrections_impacting_month(self, conn, month: str) -> list[int]:
        pending_docs = self.store.all(
            conn,
            "SELECT id, source, external_id, "
            "json_extract(raw_json,'$.corrects_external_id') AS corrects_external_id "
            "FROM ingestions WHERE state='quarantined' AND "
            "json_extract(raw_json,'$.event_type')='weigh_correction'")
        scale_fact_months = self.store.all(
            conn, "SELECT DISTINCT source, substr(day,1,7) AS month FROM facts "
                  "WHERE event_type IN ('weigh_in','weigh_out')")
        sources_in_month = {row["source"] for row in scale_fact_months if row["month"] == month}
        # Every fact external id per source, used to terminate the walk.
        fact_tickets = {}
        for row in self.store.all(conn, "SELECT source, external_id, substr(day,1,7) AS month FROM facts"):
            fact_tickets.setdefault((row["source"], row["external_id"]), row["month"])
        # Pending documents keyed by their own document id (out-of-order hops).
        pending_by_doc = {(row["source"], row["external_id"]): row for row in pending_docs}
        # Healed correction documents keyed by document id.
        healed_by_doc = {(row["source"], row["correction_external_id"]): row["corrects_external_id"]
                         for row in self.store.all(
                             conn, "SELECT source, correction_external_id, corrects_external_id "
                                   "FROM weigh_corrections")}
        impacting = []
        for row in pending_docs:
            if row["source"] not in sources_in_month:
                continue
            seen = set()
            ticket = row["corrects_external_id"]
            source = row["source"]
            while ticket and (source, ticket) not in seen:
                seen.add((source, ticket))
                if (source, ticket) in fact_tickets:
                    if fact_tickets[(source, ticket)] == month:
                        impacting.append(row["id"])
                    break
                if (source, ticket) in healed_by_doc:
                    ticket = healed_by_doc[(source, ticket)]
                    continue
                predecessor = pending_by_doc.get((source, ticket))
                if predecessor is None or predecessor["id"] == row["id"]:
                    # Unresolved chain on a source that owns scale tickets in
                    # this month: treat as impacting rather than silently ignore.
                    impacting.append(row["id"])
                    break
                ticket = predecessor["corrects_external_id"]
        return sorted(set(impacting))

    def preview(self, month: str):
        report = self.mart.month(month)
        checked = self.reconciliation.month(month)
        inventory = self.inventory.month(month) if self.inventory else None
        with self.store.connect() as conn:
            quarantined = self.store.all(conn, "SELECT id FROM ingestions WHERE state='quarantined' AND substr(json_extract(raw_json,'$.happened_at'),1,7)=?",
                                         (month,))
            # A correction can arrive in a later month yet target a ticket of
            # the month being closed: such pending chain breaks must block too.
            blocking = sorted({row["id"] for row in quarantined}
                              | set(self._corrections_impacting_month(conn, month)))
        return {"month": month, "ready": report["ready"] and checked["ready"]
                and (inventory is None or inventory["ready"]) and not blocking,
                "mart": report, "reconciliation": checked, "inventory": inventory,
                "quarantined_ingestion_ids": blocking,
                "fingerprint": digest({"mart": report, "reconciliation": checked,
                                       "inventory": inventory, "quarantined": blocking})}

    def publish(self, name: str, month: str, expected_fingerprint: str):
        name = key(name, "name")
        if not isinstance(expected_fingerprint, str) or len(expected_fingerprint) != 64:
            raise LakeError("invalid_fingerprint", "expected_fingerprint must be SHA-256")
        preview = self.preview(month)
        if not preview["ready"]:
            raise LakeError("unresolved_month", "month has unresolved material batches", 409)
        if expected_fingerprint != preview["fingerprint"]:
            raise LakeError("stale_preview", "month changed since preview", 409)
        with self.store.write() as conn:
            if self.preview(month)["fingerprint"] != expected_fingerprint:
                raise LakeError("stale_preview", "month changed during release", 409)
            previous = self.store.one(conn, "SELECT * FROM releases WHERE name=?", (name,))
            if previous:
                if previous["fingerprint"] != preview["fingerprint"]:
                    raise LakeError("release_conflict", "release name already has different content", 409)
                return {"release_id": previous["id"], "name": name,
                        "fingerprint": previous["fingerprint"], "created": False}
            cursor = conn.execute("INSERT INTO releases(name,project_month,fingerprint,content,created_at) VALUES(?,?,?,?,?)",
                                  (name, month, preview["fingerprint"],
                                   json.dumps(preview, ensure_ascii=False, sort_keys=True), now()))
            self.store.audit(conn, "release_published", name, {"id": cursor.lastrowid,
                                                                  "fingerprint": preview["fingerprint"]})
        return {"release_id": cursor.lastrowid, "name": name,
                "fingerprint": preview["fingerprint"], "created": True}

    def get(self, identifier: int | str):
        return self.store.release(identifier)

    def compare(self, left: int | str, right: int | str):
        first, second = self.get(left), self.get(right)
        before = {(row["street_id"], row["material_id"]): row
                  for row in first["content"]["mart"]["rows"]}
        after = {(row["street_id"], row["material_id"]): row
                 for row in second["content"]["mart"]["rows"]}
        changes = []
        for key_tuple in sorted(set(before) | set(after)):
            a, b = before.get(key_tuple), after.get(key_tuple)
            if a != b:
                changes.append({"street_id": key_tuple[0], "material_id": key_tuple[1],
                                "before": a, "after": b})
        return {"left_id": first["id"], "right_id": second["id"],
                "left_fingerprint": first["fingerprint"], "right_fingerprint": second["fingerprint"],
                "changes": changes}
