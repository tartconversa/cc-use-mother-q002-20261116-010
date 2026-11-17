"""端到端验收：地磅称重更正链（题 002）。

用一个合成的跨月场景串联全部要求：

1. 同批投递 + 地磅入站 + 兑付，7 月关账并发布冻结版；
2. 8 月收到带原单据号的净重更正，保留发生时间/到达时间两条线；
3. 批次守恒、站点库存、当月集市按最新有效称重计算，冻结版仍解释原数；
4. 断裂链、重复送达、旧更正晚到、与月结并行四类异常均无双计或静默回退。

运行：python -m looplake.acceptance
"""
from __future__ import annotations

import json
import tempfile
from decimal import Decimal
from pathlib import Path

from .fixtures import balanced_batch, correction, event, seed_dimensions
from .model import LakeError
from .service import LakeService


def _check(label: str, condition: bool, detail=None):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {label}" + (f" -> {detail}" if detail is not None else ""))
    if not condition:
        raise AssertionError(label)


def run_acceptance():
    with tempfile.TemporaryDirectory() as folder:
        service = LakeService(Path(folder) / "acceptance.sqlite3")
        seed_dimensions(service)

        print("== 1. 7 月同批投递、地磅入站与兑付 ==")
        outcomes = [service.ingest(row) for row in balanced_batch("batch-7", "july", weight="10")]
        _check("三事件全部规范化", all(row["state"] == "normalized" for row in outcomes))
        original_weigh_fact = outcomes[1]["fact_id"]

        print("== 2. 7 月关账并发布冻结版 ==")
        july_preview = service.preview("2026-07")
        _check("7 月预览就绪", july_preview["ready"])
        release = service.publish("2026-07-final", "2026-07", july_preview["fingerprint"])
        frozen = service.releases.get(release["release_id"])
        _check("冻结版记录原始净重 10kg",
               frozen["content"]["reconciliation"]["batches"][0]["weigh_in_kg"] == "10")

        print("== 3. 8 月 2 日收到带原单据号的更正（正确净重 9.5kg）==")
        correction_event = correction(
            "july-scale-corr-1", "july-scale", "9.5", batch_id="batch-7",
            corrected_at="2026-07-20T10:00:00+08:00",
            happened_at="2026-08-02T09:00:00+08:00")
        applied = service.ingest(correction_event)
        _check("更正规范化并生成替代链", applied["state"] == "normalized"
               and applied["superseded_fact_id"] == original_weigh_fact)

        print("== 4. 两条时间线同时可查 ==")
        chain = service.correction_chain(original_weigh_fact)
        original_version, revised_version = chain["versions"]
        _check("替代称重保留原发生时间",
               revised_version["happened_at"] == original_version["happened_at"])
        _check("更正保留自身签发时间与到达时间",
               revised_version["correction"]["corrected_at"] == "2026-07-20T02:00:00Z"
               and revised_version["correction"]["received_at"] > revised_version["happened_at"])

        print("== 5. 当前视图按最新有效称重计算且无双计 ==")
        batch = service.batch_status("batch-7")
        _check("批次守恒使用 9.5kg", batch["weigh_in_kg"] == "9.5" and batch["ready"])
        active_weighs = service.store.facts(
            "WHERE batch_id=? AND event_type='weigh_in' AND state='active'", ("batch-7",))
        _check("有效入站称重始终只有一条", len(active_weighs) == 1
               and active_weighs[0]["weight_kg"] == "9.5")
        closing = service.inventory.station_day("station-1", "foam", "2026-07-31")
        _check("站点库存按 9.5kg 结算", closing["closing_kg"] == "9.5")
        mart = service.month_report("2026-07")
        _check("月度集市仍可结算且只统计有效事实",
               mart["rows"][0]["deposit_kg"] == "10" and len(mart["rows"]) == 1)

        print("== 6. 已发布冻结版仍解释原数 ==")
        frozen_after = service.releases.get(release["release_id"])
        _check("冻结指纹不变", frozen_after["fingerprint"] == release["fingerprint"])
        _check("冻结版仍记录原始 10kg 与原事实 ID",
               frozen_after["content"]["reconciliation"]["batches"][0]["weigh_in_kg"] == "10"
               and original_weigh_fact
               in frozen_after["content"]["reconciliation"]["batches"][0]["fact_ids"])
        _check("原事实保留且标记 superseded",
               service.store.fact(original_weigh_fact)["state"] == "superseded")

        print("== 7. 异常一：更正链断裂 -> 隔离，不静默回退 ==")
        broken = service.ingest(correction(
            "july-scale-corr-3", "july-scale-corr-2", "9.8", batch_id="batch-7",
            corrected_at="2026-08-05T10:00:00+08:00",
            happened_at="2026-08-06T09:00:00+08:00"))
        _check("断裂链被隔离", broken["state"] == "quarantined"
               and broken["issue"]["code"] == "correction_chain_broken")
        _check("当前有效称重仍是 9.5kg",
               service.correction_chain(original_weigh_fact)["current_weight_kg"] == "9.5")
        _check("断裂链阻断 7 月月结", service.preview("2026-07")["ready"] is False)

        print("== 8. 异常二：重复送达 -> 幂等，无双计 ==")
        replay = service.ingest(dict(correction_event))
        _check("重复更正幂等返回同一事实",
               replay["created"] is False and replay["fact_id"] == applied["fact_id"])
        _check("有效入站称重仍只有一条",
               len(service.store.facts("WHERE batch_id=? AND event_type='weigh_in' AND state='active'",
                                       ("batch-7",))) == 1)

        print("== 9. 异常三：旧更正晚到 -> 到达后自动治愈断裂链 ==")
        middle = service.ingest(correction(
            "july-scale-corr-2", "july-scale-corr-1", "9.1", batch_id="batch-7",
            corrected_at="2026-08-03T10:00:00+08:00",
            happened_at="2026-08-04T09:00:00+08:00"))
        _check("中间更正生效并自动治愈后续断裂",
               middle["state"] == "normalized" and len(middle["healed"]) == 1)
        healed_chain = service.correction_chain(original_weigh_fact)
        _check("链按单据签发时间排序为 10 -> 9.5 -> 9.1 -> 9.8",
               [v["weight_kg"] for v in healed_chain["versions"]] == ["10", "9.5", "9.1", "9.8"])
        _check("当前有效净重为最晚更正 9.8kg", healed_chain["current_weight_kg"] == "9.8")
        _check("无遗留隔离更正", service.pending_corrections() == [])
        _check("治愈后 7 月月结恢复就绪", service.preview("2026-07")["ready"])

        print("== 10. 异常三续：分叉更正引用已替代原票 -> 隔离不合流 ==")
        # 用 8 月新批次承接分叉场景，避免干扰 7 月已关账批次的后续发布
        august_batch = balanced_batch("batch-8", "aug", weight="10",
                                      happened_at="2026-08-15T10:00:00+08:00")
        [service.ingest(row) for row in august_batch]
        service.ingest(correction(
            "aug-scale-corr-1", "aug-scale", "9.4", batch_id="batch-8",
            corrected_at="2026-08-16T10:00:00+08:00",
            happened_at="2026-08-17T09:00:00+08:00"))
        rival = service.ingest(correction(
            "aug-scale-rival", "aug-scale", "7", batch_id="batch-8",
            corrected_at="2026-08-18T10:00:00+08:00",
            happened_at="2026-08-19T09:00:00+08:00"))
        _check("分叉更正被判定链冲突",
               rival["state"] == "quarantined"
               and rival["issue"]["code"] == "correction_chain_diverged")
        _check("链头未被分叉影响",
               service.correction_chain(
                   [r for r in service.ledger.batch("batch-8") if r["event_type"] == "weigh_in"][0]["id"]
               )["current_weight_kg"] == "9.4")
        _check("8 月隔离不影响 7 月预览", service.preview("2026-07")["ready"])

        print("== 11. 异常四：更正与月结并行 -> 旧预览指纹失效，禁止双计发布 ==")
        # 干净批次上演示纯指纹失效：取预览后更正到达，旧指纹必须被拒
        [service.ingest(row) for row in balanced_batch("batch-7b", "july-b", weight="10")]
        clean_fp = service.preview("2026-07")["fingerprint"]
        service.ingest(correction(
            "july-b-corr-1", "july-b-scale", "9.6", batch_id="batch-7b",
            corrected_at="2026-08-09T10:00:00+08:00",
            happened_at="2026-08-10T09:00:00+08:00"))
        try:
            service.publish("2026-07-stale", "2026-07", clean_fp)
            raise AssertionError("stale preview should block publish")
        except LakeError as error:
            _check("旧预览指纹被拒绝 stale_preview", error.code == "stale_preview")
        # 已隔离的断裂更正同样阻止带新指纹的发布，绝不静默放行
        service.ingest(correction(
            "july-scale-corr-4", "july-scale-corr-3", "9.7", batch_id="batch-7",
            corrected_at="2026-08-11T10:00:00+08:00",
            happened_at="2026-08-12T09:00:00+08:00"))
        broken_again = service.ingest(correction(
            "july-scale-corr-orphan", "missing-ticket-x", "9.3", batch_id="batch-7",
            corrected_at="2026-08-13T10:00:00+08:00",
            happened_at="2026-08-14T09:00:00+08:00"))
        _check("新增断裂更正进入隔离",
               broken_again["state"] == "quarantined"
               and broken_again["issue"]["code"] == "correction_chain_broken")
        moving_fp = service.preview("2026-07")["fingerprint"]
        try:
            service.publish("2026-07-parallel", "2026-07", moving_fp)
            raise AssertionError("quarantined correction should block publish")
        except LakeError as error:
            _check("存在隔离更正时发布被拒 unresolved_month", error.code == "unresolved_month")
        _check("数据已变化，旧指纹与新预览不同", moving_fp != clean_fp)
        # 断裂更正无法自动治愈时，7 月保持不可发布；当前有效视图仍可通过预览解释
        fresh = service.preview("2026-07")
        _check("隔离未解除前禁止再次发布", fresh["ready"] is False)
        _check("预览仍按最新有效净重 9.7kg 计算批次守恒",
               fresh["reconciliation"]["batches"][0]["weigh_in_kg"] == "9.7")
        _check("首个冻结版仍保持 10kg 原数",
               service.releases.get(release["release_id"])
               ["content"]["reconciliation"]["batches"][0]["weigh_in_kg"] == "10")

        print("== 12. 出站称重更正同样更新库存 ==")
        service.ingest(event("weigh_out", "out-1", "batch-7", "foam", "5",
                             happened_at="2026-07-16T10:00:00+08:00"))
        service.ingest(correction(
            "out-corr-1", "out-1", "4.5", batch_id="batch-7",
            corrected_at="2026-07-21T10:00:00+08:00",
            happened_at="2026-08-02T11:00:00+08:00"))
        stock = service.inventory.station_day("station-1", "foam", "2026-07-31")
        expected_closing = format(Decimal(stock["inbound_to_date_kg"]) - Decimal("4.5"), "f")
        _check("出站按更正后 4.5kg 扣减，库存守恒",
               stock["outbound_to_date_kg"] == "4.5"
               and stock["closing_kg"] == expected_closing,
               f"closing={stock['closing_kg']}")

        result = {
            "result": "ok",
            "chain_versions": [
                {"version": v["version"], "weight_kg": v["weight_kg"], "state": v["state"]}
                for v in service.correction_chain(original_weigh_fact)["versions"]],
            "current_weight_kg": service.correction_chain(original_weigh_fact)["current_weight_kg"],
            "frozen_original_kg": "10",
            "release_kept": "2026-07-final",
        }
        print("\n验收结果：")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return result


if __name__ == "__main__":
    run_acceptance()
