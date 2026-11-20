"""业务仓测试：在临时目录构造资料，覆盖入仓、修订、分摊、收入与查询口径。

不依赖随附 data/ 的 catalog 摘要（该摘要与随附 CSV 字节存在既有不一致，
见 README「已知资料问题」）；测试自行生成字段一致的资料目录。
"""
from __future__ import annotations

import csv
import json
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from sourcepack.ingest import connect, receive
from sourcepack.warehouse import build_warehouse
from sourcepack import queries
from sourcepack.domain import (
    CargoLineage,
    allocate,
    chargeable_weight,
    d,
    money,
    tonne_km,
)

# 与 data/ 下随附资料同构的基线数据集
BASE_TABLES: dict[str, list[dict[str, str]]] = {
    "orders": [
        {"order_id": "O1", "customer": "C1", "total_kg": "100", "revenue_amount": "2000", "currency": "CNY"},
        {"order_id": "O2", "customer": "C2", "total_kg": "80", "revenue_amount": "1200", "currency": "CNY"},
        {"order_id": "O3", "customer": "C3", "total_kg": "40", "revenue_amount": "800", "currency": "CNY"},
    ],
    "cargo": [
        {"cargo_id": "G1", "parent": "", "weight_kg": "100", "volume_m3": "1", "split_at": ""},
        {"cargo_id": "G1A", "parent": "G1", "weight_kg": "60", "volume_m3": "0.6",
         "split_at": "2026-10-12T08:00:00+08:00"},
        {"cargo_id": "G1B", "parent": "G1", "weight_kg": "40", "volume_m3": "0.4",
         "split_at": "2026-10-12T08:00:00+08:00"},
        {"cargo_id": "G2", "parent": "", "weight_kg": "80", "volume_m3": "0.5", "split_at": ""},
        {"cargo_id": "G3", "parent": "", "weight_kg": "40", "volume_m3": "0.3", "split_at": ""},
    ],
    "orders_bridge": [
        {"bridge_id": "B1", "order_id": "O1", "cargo_id": "G1"},
        {"bridge_id": "B2", "order_id": "O2", "cargo_id": "G2"},
        {"bridge_id": "B3", "order_id": "O3", "cargo_id": "G3"},
    ],
    "legs": [
        {"leg_id": "L1", "mode": "road", "origin": "warehouse", "destination": "hub",
         "start": "2026-10-10T08:00:00+08:00", "end": "2026-10-11T08:00:00+08:00",
         "km": "100", "state": "actual"},
        {"leg_id": "L2", "mode": "air", "origin": "hub", "destination": "port",
         "start": "2026-10-13T08:00:00+08:00", "end": "2026-10-13T12:00:00+08:00",
         "km": "800", "state": "actual"},
        {"leg_id": "L3", "mode": "rail", "origin": "hub", "destination": "port",
         "start": "2026-10-13T08:00:00+08:00", "end": "2026-10-15T08:00:00+08:00",
         "km": "1000", "state": "actual"},
        {"leg_id": "L4", "mode": "road", "origin": "port", "destination": "client",
         "start": "2026-10-16T08:00:00+08:00", "end": "2026-10-16T18:00:00+08:00",
         "km": "80", "state": "actual"},
        {"leg_id": "L5", "mode": "air", "origin": "hub", "destination": "port",
         "start": "2026-10-12T08:00:00+08:00", "end": "2026-10-12T12:00:00+08:00",
         "km": "800", "state": "cancelled"},
    ],
    "actual_loads": [
        {"load_id": "A1", "leg_id": "L1", "cargo_id": "G1", "weight_kg": "100", "volume_m3": "1"},
        {"load_id": "A2", "leg_id": "L1", "cargo_id": "G2", "weight_kg": "80", "volume_m3": "0.5"},
        {"load_id": "A3", "leg_id": "L2", "cargo_id": "G1A", "weight_kg": "60", "volume_m3": "0.6"},
        {"load_id": "A4", "leg_id": "L2", "cargo_id": "G2", "weight_kg": "80", "volume_m3": "0.5"},
        {"load_id": "A5", "leg_id": "L3", "cargo_id": "G1B", "weight_kg": "40", "volume_m3": "0.4"},
        {"load_id": "A6", "leg_id": "L4", "cargo_id": "G1A", "weight_kg": "60", "volume_m3": "0.6"},
        {"load_id": "A7", "leg_id": "L4", "cargo_id": "G2", "weight_kg": "80", "volume_m3": "0.5"},
    ],
    "cost_scopes": [
        {"scope_id": "S1", "leg_id": "L1", "basis": "charge_weight", "volumetric_kg_m3": "167",
         "rate": "2", "minimum": "500", "currency": "CNY", "caused_order": ""},
        {"scope_id": "S2", "leg_id": "L2", "basis": "charge_weight", "volumetric_kg_m3": "167",
         "rate": "3", "minimum": "800", "currency": "USD", "caused_order": ""},
        {"scope_id": "S3", "leg_id": "L3", "basis": "volume", "volumetric_kg_m3": "167",
         "rate": "600", "minimum": "300", "currency": "CNY", "caused_order": ""},
        {"scope_id": "S4", "leg_id": "L5", "basis": "fixed", "volumetric_kg_m3": "167",
         "rate": "100", "minimum": "100", "currency": "USD", "caused_order": "O1"},
    ],
    "cost_documents": [
        {"document_id": "Q1", "scope_id": "S1", "kind": "quote", "amount": "500", "date": "2026-10-10"},
        {"document_id": "I1", "scope_id": "S1", "kind": "invoice", "amount": "520", "date": "2026-10-18"},
        {"document_id": "P1", "scope_id": "S1", "kind": "payment", "amount": "520", "date": "2026-10-20"},
        {"document_id": "Q2", "scope_id": "S2", "kind": "quote", "amount": "800", "date": "2026-10-10"},
        {"document_id": "I2", "scope_id": "S2", "kind": "invoice", "amount": "850", "date": "2026-10-20"},
        {"document_id": "Q3", "scope_id": "S3", "kind": "quote", "amount": "300", "date": "2026-10-12"},
        {"document_id": "I4", "scope_id": "S4", "kind": "invoice", "amount": "100", "date": "2026-10-18"},
    ],
    "delivery_receipts": [
        {"receipt_id": "D1", "order_id": "O1", "cargo_id": "G1A", "weight_kg": "60",
         "at": "2026-10-16T18:00:00+08:00"},
        {"receipt_id": "D2", "order_id": "O2", "cargo_id": "G2", "weight_kg": "80",
         "at": "2026-10-16T18:00:00+08:00"},
    ],
    "fx": [
        {"currency": "USD", "date": "2026-10-10", "cny_per_unit": "7.0"},
        {"currency": "USD", "date": "2026-10-12", "cny_per_unit": "7.02"},
        {"currency": "USD", "date": "2026-10-18", "cny_per_unit": "7.08"},
        {"currency": "USD", "date": "2026-10-20", "cny_per_unit": "7.1"},
    ],
}

def write_source(root: Path, tables: dict[str, list[dict[str, str]]]) -> Path:
    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    for name, rows in tables.items():
        if not rows:
            continue
        with (data_dir / f"{name}.csv").open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    # catalog 仅用于字段契约，测试不校验 sha256
    catalog = {
        "title": "test",
        "tables": [
            {"name": name, "columns": [{"name": k, "type": "string"} for k in rows[0]],
             "key": [list(rows[0])[0]], "path": f"data/{name}.csv", "row_count": len(rows),
             "sha256": "x"}
            for name, rows in tables.items() if rows
        ],
    }
    (data_dir / "catalog.json").write_text(json.dumps(catalog), encoding="utf-8")
    return root


class DomainTests(unittest.TestCase):
    def test_chargeable_weight(self):
        self.assertEqual(chargeable_weight(d("100"), d("1"), d("167")), d("167"))
        self.assertEqual(chargeable_weight(d("200"), d("1"), d("167")), d("200"))

    def test_tonne_km(self):
        self.assertEqual(tonne_km(d("100"), d("800")), d("80"))

    def test_allocation_tail_to_largest_and_sums_exact(self):
        result = allocate(d("520"), [("O1", d("167")), ("O2", d("83.5"))])
        self.assertEqual(sum(a for _, a in result), d("520"))
        # 最大权重项承接尾差
        amounts = dict(result)
        self.assertTrue(all(isinstance(v, Decimal) for v in amounts.values()))

    def test_allocation_equal_weights_key_order_for_tie(self):
        result = allocate(d("100"), [("b", d("1")), ("a", d("1")), ("c", d("1"))])
        total = sum(v for _, v in result)
        self.assertEqual(total, d("100"))
        # 尾差（如有）归业务键升序首项 a
        raw = d("100") / d("3")
        if money(raw) * 3 != d("100"):
            self.assertEqual(dict(result)["a"], money(raw) + (d("100") - money(raw) * 3))

    def test_lineage_split_conservation(self):
        lineage = CargoLineage(BASE_TABLES["cargo"])
        self.assertEqual(lineage.root_of("G1A"), "G1")
        self.assertEqual(lineage.leaves_under("G1"), ["G1A", "G1B"])
        self.assertEqual(lineage.validate_weights(), [])
        self.assertTrue(lineage.is_split_parent("G1"))


class WarehouseTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "src"
        write_source(self.root, BASE_TABLES)
        self.db = Path(self.tmp.name) / "wh.duckdb"
        self.con = connect(self.db)
        result = receive(self.con, self.root, archive_root=Path(self.tmp.name) / "runs",
                         now=datetime(2026, 10, 21, tzinfo=timezone.utc))
        self.assertEqual(result.status, "loaded", result.errors)
        build_warehouse(self.con)

    def tearDown(self):
        self.con.close()
        self.tmp.cleanup()


class IngestTests(WarehouseTestBase):
    def test_idempotent_duplicate_receive(self):
        again = receive(self.con, self.root, archive_root=Path(self.tmp.name) / "runs")
        self.assertEqual(again.status, "duplicate")

    def test_raw_history_append_only(self):
        n1 = self.con.execute("SELECT COUNT(*) FROM staging.raw_orders").fetchone()[0]
        receive(self.con, self.root, archive_root=Path(self.tmp.name) / "runs")
        # 重复接收不新增
        self.assertEqual(
            self.con.execute("SELECT COUNT(*) FROM staging.raw_orders").fetchone()[0], n1
        )

    def test_cancelled_leg_load_rejected_and_rolled_back(self):
        bad = json.loads(json.dumps(BASE_TABLES))
        bad["actual_loads"].append(
            {"load_id": "A8", "leg_id": "L5", "cargo_id": "G1A", "weight_kg": "60", "volume_m3": "0.6"}
        )
        bad_root = Path(self.tmp.name) / "bad"
        write_source(bad_root, bad)
        result = receive(self.con, bad_root, archive_root=Path(self.tmp.name) / "runs2")
        self.assertEqual(result.status, "rolled_back")
        self.assertTrue(any("取消段" in e for e in result.errors))
        # 仓内批次未增加
        self.assertEqual(
            self.con.execute("SELECT COUNT(*) FROM staging.receive_batches").fetchone()[0], 1
        )

    def test_overweight_load_rejected(self):
        bad = json.loads(json.dumps(BASE_TABLES))
        bad["actual_loads"][0]["weight_kg"] = "999"
        bad_root = Path(self.tmp.name) / "bad2"
        write_source(bad_root, bad)
        result = receive(self.con, bad_root, archive_root=Path(self.tmp.name) / "runs3")
        self.assertEqual(result.status, "rolled_back")
        self.assertTrue(any("装载超重" in e for e in result.errors))

    def test_missing_fx_rejected_for_foreign_invoice(self):
        bad = json.loads(json.dumps(BASE_TABLES))
        bad["fx"] = [r for r in bad["fx"] if r["date"] != "2026-10-20"]
        bad_root = Path(self.tmp.name) / "bad3"
        write_source(bad_root, bad)
        result = receive(self.con, bad_root, archive_root=Path(self.tmp.name) / "runs4")
        self.assertEqual(result.status, "rolled_back")
        self.assertTrue(any("汇率" in e for e in result.errors))

    def test_revision_replaces_row_and_keeps_raw_history(self):
        rev = json.loads(json.dumps(BASE_TABLES))
        rev["cost_documents"].append(
            {"document_id": "I3", "scope_id": "S3", "kind": "invoice",
             "amount": "290", "date": "2026-10-21"}
        )
        rev["cost_documents"].append(
            {"document_id": "P3", "scope_id": "S3", "kind": "payment",
             "amount": "290", "date": "2026-10-22"}
        )
        rev_root = Path(self.tmp.name) / "rev"
        write_source(rev_root, rev)
        result = receive(self.con, rev_root, archive_root=Path(self.tmp.name) / "runs5")
        self.assertEqual(result.status, "loaded", result.errors)
        build_warehouse(self.con)
        # raw 保留两批，current 只看修订后
        self.assertEqual(
            self.con.execute("SELECT COUNT(*) FROM staging.raw_cost_documents").fetchone()[0],
            7 + 9,
        )
        self.assertEqual(
            self.con.execute("SELECT COUNT(*) FROM staging.current_cost_documents").fetchone()[0],
            9,
        )
        bills = {b["scope_id"]: b for b in queries.bill_check(self.con)}
        self.assertEqual(bills["S3"]["cost_status"], "actual")
        self.assertEqual(bills["S3"]["invoice_cny"], 290.0)
        # 付款不重复成为成本
        self.assertEqual(bills["S3"]["total_cny"], 290.0)


class CostAllocationTests(WarehouseTestBase):
    def test_minimum_charge_applied_once_at_scope(self):
        # S1 理论额 = max(500, (167+83.5)*2=501)，最低收费只在范围层成立一次
        bills = {b["scope_id"]: b for b in queries.bill_check(self.con)}
        self.assertEqual(bills["S1"]["theoretical_origin"], 501.0)
        # 发票 520 分摊到两票，合计严格等于 520
        self.assertEqual(bills["S1"]["allocated_total_cny"], 520.0)
        self.assertTrue(bills["S1"]["allocation_balanced"])

    def test_shared_cost_not_double_minimum_per_order(self):
        rows = self.con.execute(
            "SELECT order_id, allocated_cny FROM warehouse.fact_cost_allocation "
            "WHERE scope_id='S1' ORDER BY order_id"
        ).fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(sum(r[1] for r in rows), 520.0)

    def test_cancellation_fee_attributed_directly(self):
        detail = queries.order_detail(self.con, "O1")
        cancel = detail["cancellation_charges"]
        self.assertEqual(len(cancel), 1)
        self.assertEqual(cancel[0]["scope_id"], "S4")
        self.assertEqual(cancel[0]["cancellation_fee_cny"], 708.0)
        # 取消费不出现在 O2
        o2 = queries.order_detail(self.con, "O2")
        self.assertNotIn("S4", [c["scope_id"] for c in o2["cost_recognition"]])

    def test_estimated_vs_actual_separated(self):
        margin = {m["order_id"]: m for m in queries.order_margin(self.con)}
        # S3 只有报价：O1 有预估成本，且不进 confirmed
        self.assertTrue(margin["O1"]["has_estimated_cost"])
        self.assertEqual(margin["O1"]["estimated_cost_cny"], 300.0)
        self.assertEqual(margin["O2"]["estimated_cost_cny"], 0.0)

    def test_fx_and_quantity_price_variance_split(self):
        bills = {b["scope_id"]: b for b in queries.bill_check(self.con)}
        s2 = bills["S2"]
        # 报价 800USD@7.00=5600；发票 850USD@7.10=6035，总差 435 拆为：
        # 汇率差 = 850×(7.10−7.00)=85；数量/计价差 = (850−800)×7.00=350
        self.assertEqual(float(s2["fx_variance_cny"]), 85.0)
        self.assertEqual(float(s2["quantity_price_variance_cny"]), 350.0)
        self.assertEqual(float(s2["invoice_cny"]), 6035.0)
        # 两项价差合计严格等于发票与报价的人民币差
        self.assertEqual(
            float(s2["fx_variance_cny"] + s2["quantity_price_variance_cny"]),
            float(s2["invoice_cny"]) - float(s2["quote_cny"]),
        )

    def test_payment_not_extra_cost_and_settlement_fx(self):
        bills = {b["scope_id"]: b for b in queries.bill_check(self.con)}
        s1 = bills["S1"]
        # 发票与付款同为 CNY，结算汇兑损益为 0；成本只算一次发票额
        self.assertEqual(s1["settlement_fx_gain_loss_cny"], 0.0)
        self.assertEqual(s1["total_cny"], 520.0)


class RevenueAndMarginTests(WarehouseTestBase):
    def test_partial_delivery_partial_revenue(self):
        detail = queries.order_detail(self.con, "O1")
        rev = detail["revenue_recognition"]
        self.assertEqual(len(rev), 1)
        self.assertAlmostEqual(float(rev[0]["revenue_share_ratio"]), 0.6)
        self.assertEqual(float(rev[0]["revenue_cny"]), 1200.0)
        margin = detail["order"]
        self.assertEqual(margin["delivery_status"], "部分交付")
        self.assertEqual(float(margin["undelivered_kg"]), 40.0)

    def test_full_delivery_full_revenue(self):
        margin = {m["order_id"]: m for m in queries.order_margin(self.con)}
        self.assertEqual(margin["O2"]["delivery_status"], "全部交付")
        self.assertEqual(margin["O2"]["recognized_revenue_cny"], 1200.0)

    def test_zero_delivery_zero_revenue_cost_retained(self):
        margin = {m["order_id"]: m for m in queries.order_margin(self.con)}
        self.assertEqual(margin["O3"]["delivery_status"], "未交付")
        self.assertEqual(margin["O3"]["recognized_revenue_cny"], 0.0)

    def test_confirmed_margin_excludes_estimate(self):
        margin = {m["order_id"]: m for m in queries.order_margin(self.con)}
        o1 = margin["O1"]
        # 确认毛利 = 确认收入 1200 - 确认成本（不含 S3 预估 300）
        self.assertEqual(
            o1["confirmed_gross_margin_cny"],
            money(d(str(o1["recognized_revenue_cny"])) - d(str(o1["confirmed_cost_cny"]))),
        )
        self.assertNotEqual(
            o1["confirmed_gross_margin_cny"],
            o1["projected_margin_including_estimate_cny"],
        )


class OperationsTests(WarehouseTestBase):
    def test_mode_volume_uses_actual_legs_only(self):
        modes = {m["mode"]: m for m in queries.mode_volume(self.con)}
        # 取消的 L5 是 air 但不计入航空承运量
        self.assertEqual(set(modes), {"air", "rail", "road"})
        # 航空只有 L2，140kg
        self.assertEqual(modes["air"]["carried_kg"], 140.0)
        self.assertEqual(modes["air"]["tonne_km"], 112.0)
        # 铁路承载改道的 G1B 40kg
        self.assertEqual(modes["rail"]["carried_kg"], 40.0)

    def test_tonne_km_not_duplicated_by_shared_leg(self):
        # L1 拼 G1/G2 两票，吨公里按各自实载分别计，段总量 18
        legs = {l["leg_id"]: l for l in queries.leg_carriage(self.con)}
        self.assertEqual(legs["L1"]["tonne_km"], 18.0)
        self.assertEqual(legs["L1"]["orders_on_leg"], 2)

    def test_order_carried_weight_dedup_by_root_cargo(self):
        margin = {m["order_id"]: m for m in queries.order_margin(self.con)}
        # G1A/G1B 同属 G1：跨段、拆分后承运存量仍是 100，不加成 200/260
        self.assertEqual(margin["O1"]["carried_kg"], 100.0)
        self.assertEqual(margin["O2"]["carried_kg"], 80.0)

    def test_route_reflects_diversion(self):
        detail = queries.order_detail(self.con, "O1")
        modes = [r["mode"] for r in detail["actual_route"]]
        # 实际路径含铁路改道，且不含取消的 L5
        self.assertIn("rail", modes)
        self.assertNotIn("L5", [r["leg_id"] for r in detail["actual_route"]])

    def test_connection_wait(self):
        waits = queries.connection_waits(self.con)
        g1a = [w for w in waits if w["cargo_id"] == "G1A"]
        self.assertEqual(len(g1a), 1)
        self.assertEqual(g1a[0]["wait_hours"], 68.0)
        # G1B 只走 L3 未接 L4，不产生等待
        self.assertFalse(any(w["cargo_id"] == "G1B" for w in waits))

    def test_cargo_status_in_transit_and_unshipped(self):
        g1b = queries.cargo_tracking(self.con, "G1B")
        self.assertEqual(g1b[0]["status"], "在途")
        self.assertEqual(g1b[0]["in_transit_or_pending_kg"], 40.0)
        g3 = queries.cargo_tracking(self.con, "G3")
        self.assertEqual(g3[0]["status"], "未发运")


class EmptyRepositioningTests(WarehouseTestBase):
    def setUp(self):
        super().setUp()
        ext = json.loads(json.dumps(BASE_TABLES))
        ext["legs"].append(
            {"leg_id": "L6", "mode": "road", "origin": "depot", "destination": "warehouse",
             "start": "2026-10-17T08:00:00+08:00", "end": "2026-10-17T18:00:00+08:00",
             "km": "120", "state": "actual"}
        )
        ext["cost_scopes"].append(
            {"scope_id": "S5", "leg_id": "L6", "basis": "fixed", "volumetric_kg_m3": "167",
             "rate": "0", "minimum": "400", "currency": "CNY", "caused_order": ""}
        )
        ext["cost_documents"].append(
            {"document_id": "I5", "scope_id": "S5", "kind": "invoice",
             "amount": "400", "date": "2026-10-19"}
        )
        ext_root = Path(self.tmp.name) / "ext"
        write_source(ext_root, ext)
        result = receive(self.con, ext_root, archive_root=Path(self.tmp.name) / "runsext")
        self.assertEqual(result.status, "loaded", result.errors)
        build_warehouse(self.con)

    def test_empty_leg_fee_pending_not_allocated(self):
        pending = queries.pending_costs(self.con)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["scope_id"], "S5")
        self.assertEqual(float(pending[0]["total_cny"]), 400.0)
        # 任何一票都不承担该费用
        for margin in queries.order_margin(self.con):
            detail = queries.order_detail(self.con, margin["order_id"])
            self.assertNotIn("S5", [c["scope_id"] for c in detail["cost_recognition"]])

    def test_empty_leg_kept_in_dimension_but_no_carriage(self):
        legs = {l["leg_id"] for l in queries.leg_carriage(self.con)}
        self.assertNotIn("L6", legs)
        kept = self.con.execute(
            "SELECT state FROM warehouse.dim_leg WHERE leg_id='L6'"
        ).fetchone()
        self.assertEqual(kept[0], "actual")


class AsOfTests(WarehouseTestBase):
    def test_cutoff_before_invoices(self):
        rows = {r["order_id"]: r for r in queries.margin_as_of(self.con, "2026-10-11")}
        # 截点早于所有发票：无确认成本、无确认收入
        self.assertEqual(rows["O1"]["confirmed_cost_cny"], 0.0)
        self.assertEqual(rows["O1"]["cost_not_yet_known_cny"], rows["O1"]["total_cost_cny"])

    def test_cutoff_after_partial_invoices(self):
        rows = {r["order_id"]: r for r in queries.margin_as_of(self.con, "2026-10-19")}
        # 10-19 时 S1/S4 发票已到、S2 未到
        self.assertGreater(rows["O1"]["confirmed_cost_cny"], 0.0)
        self.assertGreater(rows["O1"]["cost_not_yet_known_cny"], 0.0)
        # 交付发生在 10-16，收入已确认
        self.assertEqual(rows["O1"]["recognized_revenue_cny"], 1200.0)


if __name__ == "__main__":
    unittest.main()
