import tempfile
import threading
import unittest
from pathlib import Path

from looplake.fixtures import balanced_batch, correction, event, seed_dimensions
from looplake.model import LakeError
from looplake.service import LakeService


class CorrectionChainTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.service = LakeService(Path(self.temp.name) / "lake.sqlite3")
        seed_dimensions(self.service)
        self.outcomes = [self.service.ingest(row) for row in balanced_batch()]
        self.original_weigh_fact = self.outcomes[1]["fact_id"]

    def correct(self, external_id, corrects, weight="9.5", **changes):
        return self.service.ingest(correction(external_id, corrects, weight, **changes))

    # ------------------------------------------------------------- two timelines
    def test_correction_keeps_both_timelines(self):
        result = self.correct(
            "corr-1", "a-scale", "9.5",
            corrected_at="2026-07-20T10:00:00+08:00",
            happened_at="2026-08-02T09:00:00+08:00")
        self.assertEqual(result["state"], "normalized")
        chain = self.service.correction_chain(self.original_weigh_fact)
        original, revised = chain["versions"]
        # occurrence timeline: replacement keeps the original weighing instant
        self.assertEqual(revised["happened_at"], original["happened_at"])
        # arrival timeline: correction carries its own issue and landing times
        self.assertEqual(revised["correction"]["corrected_at"], "2026-07-20T02:00:00Z")
        self.assertTrue(revised["correction"]["received_at"].startswith("2026-"))
        self.assertNotEqual(revised["correction"]["received_at"], revised["happened_at"])

    def test_original_fact_is_never_deleted(self):
        self.correct("corr-1", "a-scale", "9.5",
                     corrected_at="2026-07-20T10:00:00+08:00",
                     happened_at="2026-08-02T09:00:00+08:00")
        original = self.service.store.fact(self.original_weigh_fact)
        self.assertEqual(original["state"], "superseded")
        self.assertEqual(original["weight_kg"], "10")
        head_id = self.service.correction_chain(self.original_weigh_fact)["current_fact_id"]
        replacement = self.service.store.fact(head_id)
        self.assertEqual(replacement["state"], "active")
        self.assertEqual(replacement["weight_kg"], "9.5")
        self.assertEqual(replacement["external_id"], "a-scale")
        self.assertEqual(replacement["happened_at"], original["happened_at"])

    def test_chain_records_supersession_links(self):
        result = self.correct("corr-1", "a-scale", "9.5",
                              corrected_at="2026-07-20T10:00:00+08:00",
                              happened_at="2026-08-02T09:00:00+08:00")
        self.assertEqual(result["superseded_fact_id"], self.original_weigh_fact)
        self.assertEqual(result["root_fact_id"], self.original_weigh_fact)
        chain = self.service.correction_chain(self.original_weigh_fact)
        self.assertEqual(chain["current_fact_id"], result["fact_id"])
        self.assertEqual([v["version"] for v in chain["versions"]], [0, 1])
        self.assertEqual(chain["versions"][1]["supersedes_fact_id"], self.original_weigh_fact)

    # ------------------------------------------------------------- no double count
    def test_current_views_count_each_weighing_once(self):
        self.correct("corr-1", "a-scale", "9.5",
                     corrected_at="2026-07-20T10:00:00+08:00",
                     happened_at="2026-08-02T09:00:00+08:00")
        active = self.service.store.facts(
            "WHERE batch_id=? AND event_type='weigh_in' AND state='active'", ("batch-1",))
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0]["weight_kg"], "9.5")
        batch = self.service.batch_status("batch-1")
        self.assertEqual(batch["weigh_in_kg"], "9.5")
        self.assertEqual(batch["correction_count"], 1)
        inventory = self.service.inventory.station_day("station-1", "foam", "2026-07-31")
        self.assertEqual(inventory["closing_kg"], "9.5")

    def test_superseded_history_is_still_readable(self):
        self.correct("corr-1", "a-scale", "9.5",
                     corrected_at="2026-07-20T10:00:00+08:00",
                     happened_at="2026-08-02T09:00:00+08:00")
        full = self.service.ledger.batch("batch-1", include_superseded=True)
        self.assertEqual(len(full), 4)  # deposit + 2 weigh versions + payout
        current = self.service.ledger.batch("batch-1")
        self.assertEqual(len(current), 3)

    def test_second_correction_extends_chain(self):
        self.correct("corr-1", "a-scale", "9.5",
                     corrected_at="2026-07-20T10:00:00+08:00",
                     happened_at="2026-08-02T09:00:00+08:00")
        second = self.correct("corr-2", "corr-1", "9.1",
                              corrected_at="2026-08-03T10:00:00+08:00",
                              happened_at="2026-08-04T09:00:00+08:00")
        self.assertEqual(second["state"], "normalized")
        chain = self.service.correction_chain(self.original_weigh_fact)
        self.assertEqual([v["weight_kg"] for v in chain["versions"]], ["10", "9.5", "9.1"])
        self.assertEqual(chain["current_weight_kg"], "9.1")
        self.assertEqual(len([v for v in chain["versions"] if v["state"] == "active"]), 1)

    # ------------------------------------------------------------- duplicate delivery
    def test_duplicate_correction_delivery_is_idempotent(self):
        payload = correction("corr-1", "a-scale", "9.5",
                             corrected_at="2026-07-20T10:00:00+08:00",
                             happened_at="2026-08-02T09:00:00+08:00")
        first = self.service.ingest(payload)
        second = self.service.ingest(dict(payload))
        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertEqual(first["fact_id"], second["fact_id"])
        active = self.service.store.facts(
            "WHERE batch_id=? AND event_type='weigh_in' AND state='active'", ("batch-1",))
        self.assertEqual(len(active), 1)

    def test_concurrent_duplicate_deliveries_create_one_fact(self):
        payload = correction("corr-1", "a-scale", "9.5",
                             corrected_at="2026-07-20T10:00:00+08:00",
                             happened_at="2026-08-02T09:00:00+08:00")
        results, errors = [], []

        def deliver():
            try:
                results.append(LakeService(Path(self.temp.name) / "lake.sqlite3").ingest(dict(payload)))
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        threads = [threading.Thread(target=deliver) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        normalized = {r["fact_id"] for r in results if r.get("state") == "normalized"}
        self.assertEqual(len(normalized), 1)
        active = self.service.store.facts(
            "WHERE batch_id=? AND event_type='weigh_in' AND state='active'", ("batch-1",))
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0]["id"], next(iter(normalized)))

    # ------------------------------------------------------------- broken / divergent
    def test_broken_chain_quarantines_without_silent_fallback(self):
        late = self.correct("corr-2", "corr-1", "9.1",
                            corrected_at="2026-08-03T10:00:00+08:00",
                            happened_at="2026-08-04T09:00:00+08:00")
        self.assertEqual(late["state"], "quarantined")
        self.assertEqual(late["issue"]["code"], "correction_chain_broken")
        # original weighing remains the single effective record
        batch = self.service.batch_status("batch-1")
        self.assertEqual(batch["weigh_in_kg"], "10")
        pending = self.service.pending_corrections()
        self.assertEqual([row["correction_external_id"] for row in pending], ["corr-2"])

    def test_broken_chain_heals_when_predecessor_arrives(self):
        self.correct("corr-2", "corr-1", "9.1",
                     corrected_at="2026-08-03T10:00:00+08:00",
                     happened_at="2026-08-04T09:00:00+08:00")
        first = self.correct("corr-1", "a-scale", "9.5",
                             corrected_at="2026-07-20T10:00:00+08:00",
                             happened_at="2026-08-02T09:00:00+08:00")
        self.assertEqual(first["state"], "normalized")
        self.assertEqual(len(first["healed"]), 1)
        chain = self.service.correction_chain(self.original_weigh_fact)
        self.assertEqual([v["weight_kg"] for v in chain["versions"]], ["10", "9.5", "9.1"])
        # document issue time drives sequence order despite arrival order
        self.assertEqual([v["version"] for v in chain["versions"]], [0, 1, 2])
        self.assertEqual(self.service.pending_corrections(), [])

    def test_competing_correction_of_superseded_ticket_diverges(self):
        self.correct("corr-1", "a-scale", "9.5",
                     corrected_at="2026-07-20T10:00:00+08:00",
                     happened_at="2026-08-02T09:00:00+08:00")
        rival = self.correct("corr-x", "a-scale", "8",
                             corrected_at="2026-08-05T10:00:00+08:00",
                             happened_at="2026-08-06T09:00:00+08:00")
        self.assertEqual(rival["state"], "quarantined")
        self.assertEqual(rival["issue"]["code"], "correction_chain_diverged")
        # head is untouched, rival created no fact
        chain = self.service.correction_chain(self.original_weigh_fact)
        self.assertEqual(chain["current_weight_kg"], "9.5")
        active = self.service.store.facts(
            "WHERE batch_id=? AND event_type='weigh_in' AND state='active'", ("batch-1",))
        self.assertEqual(len(active), 1)

    def test_identical_weight_is_rejected(self):
        result = self.correct("corr-1", "a-scale", "10",
                              corrected_at="2026-07-20T10:00:00+08:00",
                              happened_at="2026-08-02T09:00:00+08:00")
        self.assertEqual(result["state"], "quarantined")
        self.assertEqual(result["issue"]["code"], "correction_unchanged")
        self.assertEqual(self.service.batch_status("batch-1")["weigh_in_kg"], "10")

    def test_correction_of_non_scale_ticket_rejected(self):
        result = self.service.ingest(correction(
            "corr-bad", "a-deposit", "9",
            corrected_at="2026-07-20T10:00:00+08:00",
            happened_at="2026-08-02T09:00:00+08:00"))
        self.assertEqual(result["state"], "quarantined")
        self.assertEqual(result["issue"]["code"], "correction_chain_broken")

    def test_correction_requires_reference_fields(self):
        with self.assertRaises(LakeError) as failure:
            self.service.ingest({
                "source": "scale-feed", "external_id": "c1", "event_type": "weigh_correction",
                "happened_at": "2026-08-02T09:00:00+08:00", "batch_id": "batch-1",
                "channel": "scale", "weight_kg": "9.5"})
        self.assertEqual(failure.exception.code, "missing_corrects")

    # ------------------------------------------------------------- month close
    def test_frozen_release_keeps_original_numbers_after_correction(self):
        fingerprint = self.service.preview("2026-07")["fingerprint"]
        release = self.service.publish("july-final", "2026-07", fingerprint)
        self.correct("corr-1", "a-scale", "9.5",
                     corrected_at="2026-07-20T10:00:00+08:00",
                     happened_at="2026-08-02T09:00:00+08:00")
        frozen = self.service.releases.get(release["release_id"])
        self.assertEqual(frozen["fingerprint"], release["fingerprint"])
        self.assertIn(self.original_weigh_fact,
                      frozen["content"]["reconciliation"]["batches"][0]["fact_ids"])
        # current view moved on; frozen snapshot still explains the old number
        self.assertEqual(self.service.batch_status("batch-1")["weigh_in_kg"], "9.5")
        self.assertEqual(frozen["content"]["reconciliation"]["batches"][0]["weigh_in_kg"], "10")

    def test_stale_preview_blocks_release_when_correction_lands(self):
        fingerprint = self.service.preview("2026-07")["fingerprint"]
        self.correct("corr-1", "a-scale", "9.5",
                     corrected_at="2026-07-20T10:00:00+08:00",
                     happened_at="2026-08-02T09:00:00+08:00")
        with self.assertRaises(LakeError) as failure:
            self.service.publish("july-final", "2026-07", fingerprint)
        self.assertEqual(failure.exception.code, "stale_preview")

    def test_quarantined_correction_blocks_month_close(self):
        self.correct("corr-2", "corr-1", "9.1",
                     corrected_at="2026-08-03T10:00:00+08:00",
                     happened_at="2026-08-04T09:00:00+08:00")
        preview = self.service.preview("2026-07")
        self.assertFalse(preview["ready"])
        self.assertEqual(preview["quarantined_ingestion_ids"], [4])

    def test_correction_making_batch_unbalanced_blocks_close(self):
        fingerprint = self.service.preview("2026-07")["fingerprint"]
        self.service.publish("july-final", "2026-07", fingerprint)
        # correction outside tolerance of the 10 kg deposit
        self.correct("corr-1", "a-scale", "7",
                     corrected_at="2026-07-20T10:00:00+08:00",
                     happened_at="2026-08-02T09:00:00+08:00")
        current = self.service.preview("2026-07")
        self.assertFalse(current["ready"])
        issue_codes = [issue["code"] for issue in current["reconciliation"]["batches"][0]["issues"]]
        self.assertIn("deposit_scale_gap", issue_codes)
        # frozen release remains valid and unchanged
        frozen = self.service.releases.get("july-final")
        self.assertEqual(frozen["content"]["reconciliation"]["batches"][0]["weigh_in_kg"], "10")

    # ------------------------------------------------------------- weigh_out support
    def test_weigh_out_correction_updates_inventory(self):
        self.service.ingest(event("weigh_out", "out-1", weight="6",
                                  happened_at="2026-07-16T10:00:00+08:00"))
        result = self.service.ingest(correction(
            "out-corr-1", "out-1", "5.5",
            corrected_at="2026-07-21T10:00:00+08:00",
            happened_at="2026-08-02T10:00:00+08:00"))
        self.assertEqual(result["state"], "normalized")
        inventory = self.service.inventory.station_day("station-1", "foam", "2026-07-31")
        self.assertEqual(inventory["outbound_to_date_kg"], "5.5")
        self.assertEqual(inventory["closing_kg"], "4.5")


if __name__ == "__main__":
    unittest.main()
