from __future__ import annotations

from decimal import Decimal
import json

from .dimensions import Dimensions
from .landing import Landed, Landing
from .model import LakeError, RawEvent, now
from .store import Store


class Ledger:
    def __init__(self, store: Store, dimensions: Dimensions, landing: Landing, corrections=None):
        self.store, self.dimensions, self.landing = store, dimensions, landing
        self.corrections = corrections

    def ingest(self, data: dict):
        landed = self.landing.accept(data)
        if not landed.created and landed.state == "normalized":
            return {"ingestion_id": landed.ingestion_id, "fact_id": landed.normalized_id,
                    "state": "normalized", "created": False}
        if not landed.created and landed.state == "quarantined":
            stored = self.store.ingestion(landed.ingestion_id)
            return {"ingestion_id": landed.ingestion_id, "state": "quarantined",
                    "created": False, "issue": json.loads(stored["issue"])}
        return self.normalize(landed.ingestion_id, landed.event, landed.created)

    def normalize(self, ingestion_id: int, event: RawEvent | None = None, created: bool = False):
        if event is None:
            retried = self.landing.retry(ingestion_id)
            if isinstance(retried, dict):
                return {**retried, "created": False}
            event = retried
        try:
            return self._normalize(ingestion_id, event, created)
        except LakeError as error:
            self.landing.quarantine(ingestion_id, error)
            return {"ingestion_id": ingestion_id, "state": "quarantined", "created": created,
                    "issue": {"code": error.code, "detail": error.detail}}

    def _normalize(self, ingestion_id: int, event: RawEvent, created: bool):
        if event.event_type == "weigh_correction":
            return self.corrections.apply(ingestion_id, event, created)
        with self.store.write() as conn:
            row = self.store.one(conn, "SELECT * FROM ingestions WHERE id=?", (ingestion_id,))
            if row is None:
                raise LakeError("unknown_ingestion", "ingestion not found", 404)
            if row["state"] == "normalized":
                return {"ingestion_id": ingestion_id, "fact_id": row["normalized_id"],
                        "state": "normalized", "created": False}
            dims = self.dimensions.resolve_event(conn, event)
            if event.event_type == "deposit" and "community" not in dims:
                raise LakeError("missing_location", "deposit device must map to a community")
            if event.event_type == "payout" and "actor" not in dims:
                raise LakeError("missing_actor", "payout actor is unknown")
            if event.event_type == "weigh_out" and "station" not in dims:
                raise LakeError("missing_station", "outbound weighing needs station")
            if event.event_type == "transfer" and "station" not in dims:
                raise LakeError("missing_station", "transfer needs station")
            cursor = conn.execute("INSERT INTO facts(ingestion_id,source,external_id,event_type,happened_at,day,batch_id,material_id,device_id,station_id,actor_id,street_id,community_id,weight_kg,payout_yuan,channel,dimensions_json,state,supersedes_fact_id,root_fact_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'active',NULL,NULL)",
                                  (ingestion_id, event.source, event.external_id, event.event_type,
                                   event.happened_at, event.happened_at[:10], event.batch_id,
                                   event.material_id, event.device_id, event.station_id, event.actor_id,
                                   dims.get("street", {}).get("external_id"), dims.get("community", {}).get("external_id"),
                                   event.weight_kg, event.payout_yuan, event.channel,
                                   json.dumps(dims, ensure_ascii=False, sort_keys=True)))
            fact_id = cursor.lastrowid
            conn.execute("UPDATE facts SET root_fact_id=? WHERE id=?", (fact_id, fact_id))
            conn.execute("UPDATE ingestions SET state='normalized', issue=NULL,normalized_id=? WHERE id=?",
                         (fact_id, ingestion_id))
            self.store.audit(conn, "normalized", str(fact_id), {"ingestion_id": ingestion_id,
                                                                 "dimension_versions": {k: v["version"] for k, v in dims.items()}})
        return {"ingestion_id": ingestion_id, "fact_id": fact_id, "state": "normalized", "created": created}

    def insert_fact(self, conn, ingestion_id: int, event: RawEvent, dims: dict,
                    state: str = "active", supersedes_fact_id: int | None = None,
                    root_fact_id: int | None = None, external_id: str | None = None) -> int:
        cursor = conn.execute("INSERT INTO facts(ingestion_id,source,external_id,event_type,happened_at,day,batch_id,material_id,device_id,station_id,actor_id,street_id,community_id,weight_kg,payout_yuan,channel,dimensions_json,state,supersedes_fact_id,root_fact_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                              (ingestion_id, event.source, external_id or event.external_id,
                               "weigh_in" if event.event_type == "weigh_correction" else event.event_type,
                               event.happened_at, event.happened_at[:10], event.batch_id,
                               event.material_id, event.device_id, event.station_id, event.actor_id,
                               dims.get("street", {}).get("external_id"),
                               dims.get("community", {}).get("external_id"),
                               event.weight_kg, event.payout_yuan, event.channel,
                               json.dumps(dims, ensure_ascii=False, sort_keys=True),
                               state, supersedes_fact_id, root_fact_id))
        return cursor.lastrowid

    def batch(self, batch_id: str, include_superseded: bool = False):
        from .model import key
        batch_id = key(batch_id, "batch_id")
        where = "WHERE batch_id=?" + ("" if include_superseded else " AND state='active'")
        return self.store.facts(where, (batch_id,))

    def period(self, month: str, include_superseded: bool = False):
        from .model import calendar_day
        calendar_day(month + "-01", "month")
        where = "WHERE substr(day,1,7)=?" + ("" if include_superseded else " AND state='active'")
        return self.store.facts(where, (month,))

    def trace(self, fact_id: int):
        fact = self.store.fact(fact_id)
        ingestion = self.store.ingestion(fact["ingestion_id"])
        return {"fact": fact, "landing": ingestion,
                "dimension_versions": fact["dimensions"]}

    def batch_balances(self, batch_id: str):
        facts = self.batch(batch_id)
        by_type = {}
        for fact in facts:
            kind = fact["event_type"]
            if kind not in by_type:
                by_type[kind] = {"count": 0, "weight_kg": Decimal("0"), "payout_yuan": Decimal("0")}
            by_type[kind]["count"] += 1
            by_type[kind]["weight_kg"] += Decimal(fact["weight_kg"] or "0")
            by_type[kind]["payout_yuan"] += Decimal(fact["payout_yuan"] or "0")
        return {kind: {field: format(value, "f") if isinstance(value, Decimal) else value
                       for field, value in measures.items()} for kind, measures in by_type.items()}
