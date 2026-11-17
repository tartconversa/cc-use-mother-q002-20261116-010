import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from looplake.api import create_app
from looplake.fixtures import balanced_batch, correction, seed_dimensions


class CorrectionApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.client = TestClient(create_app(Path(self.temp.name) / "api.sqlite3"))
        from looplake.service import LakeService
        self.service = LakeService(Path(self.temp.name) / "api.sqlite3")
        seed_dimensions(self.service)
        [self.service.ingest(row) for row in balanced_batch()]

    def test_correction_lifecycle_over_http(self):
        payload = correction("corr-1", "a-scale", "9.5",
                             corrected_at="2026-07-20T10:00:00+08:00",
                             happened_at="2026-08-02T09:00:00+08:00")
        response = self.client.post("/events", json=payload)
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body["state"], "normalized")
        fact_id = body["fact_id"]

        chain = self.client.get(f"/facts/{fact_id}/corrections")
        self.assertEqual(chain.status_code, 200)
        data = chain.json()
        self.assertEqual(data["current_weight_kg"], "9.5")
        self.assertEqual([v["weight_kg"] for v in data["versions"]], ["10", "9.5"])
        self.assertEqual(data["versions"][0]["state"], "superseded")

        # duplicate delivery over HTTP stays idempotent
        replay = self.client.post("/events", json=payload)
        self.assertEqual(replay.status_code, 200)
        self.assertFalse(replay.json()["created"])
        self.assertEqual(replay.json()["fact_id"], fact_id)

        # current batch reflects latest effective weighing
        batch = self.client.get("/batches/batch-1").json()
        self.assertEqual(batch["weigh_in_kg"], "9.5")
        self.assertEqual(batch["correction_count"], 1)

    def test_broken_chain_listed_and_healed(self):
        broken = self.client.post("/events", json=correction(
            "corr-2", "corr-1", "9.1",
            corrected_at="2026-08-03T10:00:00+08:00",
            happened_at="2026-08-04T09:00:00+08:00"))
        self.assertEqual(broken.json()["state"], "quarantined")
        pending = self.client.get("/corrections/pending")
        self.assertEqual([row["correction_external_id"] for row in pending.json()], ["corr-2"])

        self.client.post("/events", json=correction(
            "corr-1", "a-scale", "9.5",
            corrected_at="2026-07-20T10:00:00+08:00",
            happened_at="2026-08-02T09:00:00+08:00"))
        # auto-heal already ran; explicit endpoint returns nothing left
        healed = self.client.post("/corrections/heal")
        self.assertEqual(healed.status_code, 200)
        self.assertEqual(self.client.get("/corrections/pending").json(), [])

    def test_correction_requires_reference(self):
        payload = correction("corr-x", "a-scale", "9")
        del payload["corrects_external_id"]
        response = self.client.post("/events", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["code"], "missing_corrects")

    def test_chain_lookup_unknown_fact(self):
        response = self.client.get("/facts/999/corrections")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["code"], "unknown_fact")


if __name__ == "__main__":
    unittest.main()
