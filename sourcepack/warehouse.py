"""仓表构建：从 staging 持续装载的当前事实构建履约模型与集市。

分层：
  staging.current_*   来源当前版本（原始事实仍在 raw_* 中保留全部修订历史）
  warehouse.*         履约模型：谱系、实载、共享费用、分摊明细、交付收入、衔接等待
  marts.*             面向商务/运营的集市视图

构建方式为整层可重复重建（幂等），从不修改 staging 原始事实；预估与确认成本
分列保存，绝不把预估伪装成已确认毛利。
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping, Sequence

import duckdb

from .domain import allocate, chargeable_weight, d, money, tonne_km

SCHEMA_DDL = """
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS marts;

-- 货批谱系（递归闭合表，父子不重复计存量由入仓校验保证）
CREATE OR REPLACE TABLE warehouse.dim_cargo (
    cargo_id VARCHAR PRIMARY KEY,
    parent VARCHAR,
    root_cargo_id VARCHAR NOT NULL,
    weight_kg DECIMAL(18,4) NOT NULL,
    volume_m3 DECIMAL(18,6) NOT NULL,
    split_at TIMESTAMPTZ,
    is_leaf BOOLEAN NOT NULL,
    depth INTEGER NOT NULL
);

CREATE OR REPLACE TABLE warehouse.dim_order (
    order_id VARCHAR PRIMARY KEY,
    customer VARCHAR NOT NULL,
    total_kg DECIMAL(18,4) NOT NULL,
    revenue_amount DECIMAL(18,2) NOT NULL,
    currency VARCHAR NOT NULL,
    fx_cny_per_unit DECIMAL(18,8) NOT NULL
);

CREATE OR REPLACE TABLE warehouse.bridge_order_cargo (
    order_id VARCHAR NOT NULL,
    cargo_id VARCHAR NOT NULL,
    root_cargo_id VARCHAR NOT NULL,
    PRIMARY KEY (order_id, cargo_id)
);

CREATE OR REPLACE TABLE warehouse.dim_leg (
    leg_id VARCHAR PRIMARY KEY,
    mode VARCHAR NOT NULL,
    origin VARCHAR NOT NULL,
    destination VARCHAR NOT NULL,
    start_ts TIMESTAMPTZ NOT NULL,
    end_ts TIMESTAMPTZ NOT NULL,
    km DECIMAL(18,3) NOT NULL,
    state VARCHAR NOT NULL,
    is_cancelled BOOLEAN NOT NULL
);

-- 实载事实：每行 = 某段上某实物流的一次明确装载，吨公里只在此产生一次
CREATE OR REPLACE TABLE warehouse.fact_actual_load (
    load_id VARCHAR PRIMARY KEY,
    leg_id VARCHAR NOT NULL,
    cargo_id VARCHAR NOT NULL,
    root_cargo_id VARCHAR NOT NULL,
    order_id VARCHAR,
    mode VARCHAR NOT NULL,
    weight_kg DECIMAL(18,4) NOT NULL,
    volume_m3 DECIMAL(18,6) NOT NULL,
    km DECIMAL(18,3) NOT NULL,
    tonne_km DECIMAL(18,6) NOT NULL,
    start_ts TIMESTAMPTZ NOT NULL,
    end_ts TIMESTAMPTZ NOT NULL
);

-- 账单范围 × 每个承载货批的合同计费数量（最低收费不在此行套用）
CREATE OR REPLACE TABLE warehouse.fact_scope_quantity (
    scope_id VARCHAR NOT NULL,
    leg_id VARCHAR NOT NULL,
    cargo_id VARCHAR NOT NULL,
    order_id VARCHAR NOT NULL,
    basis VARCHAR NOT NULL,
    actual_kg DECIMAL(18,4) NOT NULL,
    volume_m3 DECIMAL(18,6) NOT NULL,
    charge_weight_kg DECIMAL(18,4) NOT NULL,
    basis_value DECIMAL(18,6) NOT NULL,
    PRIMARY KEY (scope_id, cargo_id)
);

-- 共享费用范围事实：合同理论额、报价、正式账单各自成立，差异可解释
CREATE OR REPLACE TABLE warehouse.fact_scope_cost (
    scope_id VARCHAR PRIMARY KEY,
    leg_id VARCHAR NOT NULL,
    basis VARCHAR NOT NULL,
    currency VARCHAR NOT NULL,
    caused_order VARCHAR,
    rate DECIMAL(18,6),
    minimum DECIMAL(18,2),
    theoretical_origin DECIMAL(18,2) NOT NULL,
    theoretical_cny DECIMAL(18,2) NOT NULL,
    quote_origin DECIMAL(18,2),
    quote_cny DECIMAL(18,2),
    invoice_origin DECIMAL(18,2),
    invoice_cny DECIMAL(18,2),
    payment_origin DECIMAL(18,2),
    payment_cny DECIMAL(18,2),
    settlement_fx_gain_loss_cny DECIMAL(18,2),
    fx_variance_cny DECIMAL(18,2),
    quantity_price_variance_cny DECIMAL(18,2),
    total_cny DECIMAL(18,2) NOT NULL,
    cost_status VARCHAR NOT NULL,
    fx_note VARCHAR
);

-- 范围总价向客户订单的分摊（最低收费在范围层只成立一次）
CREATE OR REPLACE TABLE warehouse.fact_cost_allocation (
    scope_id VARCHAR NOT NULL,
    order_id VARCHAR NOT NULL,
    allocation_basis VARCHAR NOT NULL,
    basis_value DECIMAL(18,6) NOT NULL,
    share_ratio DECIMAL(18,10) NOT NULL,
    allocated_cny DECIMAL(18,2) NOT NULL,
    cost_status VARCHAR NOT NULL,
    PRIMARY KEY (scope_id, order_id)
);

-- 交付与收入确认：按已交付重量占订单总重量确认，部分交付形成部分收入
CREATE OR REPLACE TABLE warehouse.fact_delivery_revenue (
    receipt_id VARCHAR PRIMARY KEY,
    order_id VARCHAR NOT NULL,
    cargo_id VARCHAR NOT NULL,
    delivered_kg DECIMAL(18,4) NOT NULL,
    delivered_at TIMESTAMPTZ NOT NULL,
    order_total_kg DECIMAL(18,4) NOT NULL,
    revenue_share_ratio DECIMAL(18,10) NOT NULL,
    revenue_origin DECIMAL(18,2) NOT NULL,
    currency VARCHAR NOT NULL,
    fx_cny_per_unit DECIMAL(18,8) NOT NULL,
    revenue_cny DECIMAL(18,2) NOT NULL
);

-- 订单视角的实际运输路径（来自实载，不读预订单/取消单）
CREATE OR REPLACE TABLE warehouse.order_route (
    order_id VARCHAR NOT NULL,
    leg_id VARCHAR NOT NULL,
    seq INTEGER NOT NULL,
    mode VARCHAR NOT NULL,
    origin VARCHAR NOT NULL,
    destination VARCHAR NOT NULL,
    start_ts TIMESTAMPTZ NOT NULL,
    end_ts TIMESTAMPTZ NOT NULL,
    cargo_ids VARCHAR NOT NULL,
    weight_kg DECIMAL(18,4) NOT NULL,
    tonne_km DECIMAL(18,6) NOT NULL,
    PRIMARY KEY (order_id, leg_id)
);

-- 中转站衔接等待：同一实物流相邻实际段之间的停留
CREATE OR REPLACE TABLE warehouse.connection_wait (
    cargo_id VARCHAR NOT NULL,
    from_leg VARCHAR NOT NULL,
    to_leg VARCHAR NOT NULL,
    connection_point VARCHAR NOT NULL,
    arrive_ts TIMESTAMPTZ NOT NULL,
    depart_ts TIMESTAMPTZ NOT NULL,
    wait_hours DECIMAL(18,4) NOT NULL,
    PRIMARY KEY (cargo_id, from_leg, to_leg)
);

-- 待核挂起费用：无承载订单可归属的共享费（如空载调拨固定费），显式挂起不摊平
CREATE OR REPLACE TABLE warehouse.pending_cost (
    scope_id VARCHAR PRIMARY KEY,
    total_cny DECIMAL(18,2) NOT NULL,
    cost_status VARCHAR NOT NULL,
    reason VARCHAR NOT NULL
);
"""

MARTS_DDL = """
-- 商务：订单毛利与履约状态（预估/确认分列）
CREATE OR REPLACE VIEW marts.mart_order_margin AS
WITH rev AS (
    SELECT order_id,
           SUM(revenue_cny) AS recognized_revenue_cny,
           SUM(revenue_origin) AS recognized_revenue_origin
    FROM warehouse.fact_delivery_revenue
    GROUP BY order_id
),
cost AS (
    SELECT order_id,
           SUM(CASE WHEN cost_status = 'actual' THEN allocated_cny ELSE 0 END) AS confirmed_cost_cny,
           SUM(CASE WHEN cost_status = 'estimated' THEN allocated_cny ELSE 0 END) AS estimated_cost_cny,
           SUM(allocated_cny) AS total_cost_cny
    FROM warehouse.fact_cost_allocation
    GROUP BY order_id
),
deliv AS (
    SELECT order_id, SUM(delivered_kg) AS delivered_kg
    FROM warehouse.fact_delivery_revenue
    GROUP BY order_id
),
carried AS (
    -- 承运存量按根货批去重（同一实物跨段、拆分后都只计一次）；
    -- 吨公里按实际承载区段逐段累加，同段不挂双单重复
    SELECT t.order_id,
           SUM(t.cargo_weight) AS carried_kg,
           SUM(t.cargo_tonne_km) AS tonne_km
    FROM (
        SELECT f.order_id, f.root_cargo_id,
               MAX(c.weight_kg) AS cargo_weight,
               SUM(f.tonne_km) AS cargo_tonne_km
        FROM warehouse.fact_actual_load f
        JOIN warehouse.dim_cargo c ON c.cargo_id = f.root_cargo_id
        WHERE f.order_id IS NOT NULL
        GROUP BY f.order_id, f.root_cargo_id
    ) t
    GROUP BY t.order_id
)
SELECT
    o.order_id,
    o.customer,
    o.total_kg,
    COALESCE(deliv.delivered_kg, 0) AS delivered_kg,
    o.total_kg - COALESCE(deliv.delivered_kg, 0) AS undelivered_kg,
    COALESCE(carried.carried_kg, 0) AS carried_kg,
    COALESCE(carried.tonne_km, 0) AS tonne_km,
    o.revenue_amount AS contracted_revenue_origin,
    o.currency,
    COALESCE(rev.recognized_revenue_origin, 0) AS recognized_revenue_origin,
    COALESCE(rev.recognized_revenue_cny, 0) AS recognized_revenue_cny,
    COALESCE(cost.confirmed_cost_cny, 0) AS confirmed_cost_cny,
    COALESCE(cost.estimated_cost_cny, 0) AS estimated_cost_cny,
    COALESCE(cost.total_cost_cny, 0) AS total_cost_cny,
    COALESCE(rev.recognized_revenue_cny, 0) - COALESCE(cost.confirmed_cost_cny, 0)
        AS confirmed_gross_margin_cny,
    COALESCE(rev.recognized_revenue_cny, 0) - COALESCE(cost.total_cost_cny, 0)
        AS projected_margin_including_estimate_cny,
    CASE
        WHEN COALESCE(deliv.delivered_kg, 0) = 0 THEN '未交付'
        WHEN deliv.delivered_kg >= o.total_kg THEN '全部交付'
        ELSE '部分交付'
    END AS delivery_status,
    CASE WHEN COALESCE(cost.estimated_cost_cny, 0) > 0 THEN TRUE ELSE FALSE END AS has_estimated_cost
FROM warehouse.dim_order o
LEFT JOIN rev USING (order_id)
LEFT JOIN cost USING (order_id)
LEFT JOIN deliv USING (order_id)
LEFT JOIN carried USING (order_id);

-- 运营：按实际运输段查承载实物与订单（拼箱/合单不重复计重）
CREATE OR REPLACE VIEW marts.mart_leg_carriage AS
SELECT
    l.leg_id,
    l.mode,
    l.origin,
    l.destination,
    l.start_ts,
    l.end_ts,
    l.km,
    COUNT(DISTINCT f.cargo_id) AS cargo_streams,
    COUNT(DISTINCT f.order_id) AS orders_on_leg,
    SUM(f.weight_kg) AS carried_kg,
    SUM(f.volume_m3) AS carried_volume_m3,
    SUM(f.tonne_km) AS tonne_km,
    STRING_AGG(DISTINCT f.cargo_id, ',' ORDER BY f.cargo_id) AS cargo_id_list,
    STRING_AGG(DISTINCT f.order_id, ',' ORDER BY f.order_id) FILTER (WHERE f.order_id IS NOT NULL)
        AS order_id_list
FROM warehouse.dim_leg l
JOIN warehouse.fact_actual_load f USING (leg_id)
WHERE l.is_cancelled = FALSE
GROUP BY l.leg_id, l.mode, l.origin, l.destination, l.start_ts, l.end_ts, l.km;

-- 运营：按真实运输方式的承运量（段内重量按实物流自然不重复，吨公里按段累加）
CREATE OR REPLACE VIEW marts.mart_mode_volume AS
WITH leg_agg AS (
    SELECT leg_id,
           SUM(weight_kg) AS carried_kg,
           SUM(tonne_km) AS tonne_km
    FROM warehouse.fact_actual_load
    GROUP BY leg_id
),
mode_agg AS (
    SELECT l.mode,
           COUNT(*) AS actual_legs,
           SUM(a.carried_kg) AS carried_kg,
           SUM(a.tonne_km) AS tonne_km
    FROM warehouse.dim_leg l
    JOIN leg_agg a USING (leg_id)
    WHERE l.is_cancelled = FALSE
    GROUP BY l.mode
),
mode_orders AS (
    SELECT l.mode, COUNT(DISTINCT f.order_id) AS orders_carried
    FROM warehouse.dim_leg l
    JOIN warehouse.fact_actual_load f USING (leg_id)
    WHERE l.is_cancelled = FALSE AND f.order_id IS NOT NULL
    GROUP BY l.mode
)
SELECT m.mode, m.actual_legs, m.carried_kg, m.tonne_km,
       COALESCE(o.orders_carried, 0) AS orders_carried
FROM mode_agg m
LEFT JOIN mode_orders o USING (mode);

-- 货批存量状态：已交付 / 在途（已装未交）/ 未发运
CREATE OR REPLACE VIEW marts.mart_cargo_status AS
WITH loaded AS (
    SELECT cargo_id, SUM(weight_kg) AS loaded_kg,
           MAX(end_ts) AS last_load_end
    FROM warehouse.fact_actual_load
    GROUP BY cargo_id
),
delivered AS (
    SELECT cargo_id, SUM(delivered_kg) AS delivered_kg
    FROM warehouse.fact_delivery_revenue
    GROUP BY cargo_id
)
SELECT
    c.cargo_id,
    c.root_cargo_id,
    c.weight_kg,
    COALESCE(l.loaded_kg, 0) AS loaded_kg,
    COALESCE(dl.delivered_kg, 0) AS delivered_kg,
    c.weight_kg - COALESCE(dl.delivered_kg, 0) AS in_transit_or_pending_kg,
    CASE
        WHEN COALESCE(dl.delivered_kg, 0) >= c.weight_kg THEN '已交付'
        WHEN COALESCE(l.loaded_kg, 0) > 0 THEN '在途'
        ELSE '未发运'
    END AS status
FROM warehouse.dim_cargo c
LEFT JOIN loaded l USING (cargo_id)
LEFT JOIN delivered dl USING (cargo_id)
WHERE c.is_leaf = TRUE;

-- 共享账单核对：理论合同额、暂估、发票、付款与价差分解
CREATE OR REPLACE VIEW marts.mart_bill_check AS
SELECT
    s.scope_id,
    s.leg_id,
    s.basis,
    s.currency,
    s.cost_status,
    s.theoretical_origin,
    s.theoretical_cny,
    s.quote_origin,
    s.quote_cny,
    s.invoice_origin,
    s.invoice_cny,
    s.payment_origin,
    s.payment_cny,
    s.settlement_fx_gain_loss_cny,
    s.fx_variance_cny,
    s.quantity_price_variance_cny,
    s.total_cny,
    (SELECT SUM(allocated_cny) FROM warehouse.fact_cost_allocation a
     WHERE a.scope_id = s.scope_id) AS allocated_total_cny,
    s.fx_note
FROM warehouse.fact_scope_cost s;

-- 取消与空载调拨一览（预订未实际发生的服务单独留痕）
CREATE OR REPLACE VIEW marts.mart_cancelled_legs AS
SELECT
    l.leg_id,
    l.mode,
    l.origin,
    l.destination,
    l.start_ts,
    s.scope_id,
    s.caused_order,
    s.invoice_origin AS cancellation_fee_origin,
    s.currency,
    s.total_cny AS cancellation_fee_cny,
    s.cost_status
FROM warehouse.dim_leg l
JOIN warehouse.fact_scope_cost s ON s.leg_id = l.leg_id
WHERE l.is_cancelled = TRUE;
"""


@dataclass
class _Doc:
    kind: str
    amount: Decimal
    date: str


def _rows(con: duckdb.DuckDBPyConnection, table: str) -> list[dict[str, Any]]:
    # 统一转 VARCHAR 读取：避免对 pytz 的隐式依赖，业务日期仍为 ISO 字符串，
    # 写回 TIMESTAMPTZ/DATE 列时由 DuckDB 按 ISO 解析。
    result = con.execute(
        f"SELECT * FROM staging.current_{table} LIMIT 0"
    )
    cols = [item[0] for item in result.description]
    select_list = ", ".join(f'CAST("{col}" AS VARCHAR) AS "{col}"' for col in cols)
    result = con.execute(f"SELECT {select_list} FROM staging.current_{table}")
    return [dict(zip(cols, row)) for row in result.fetchall()]


def _fx_lookup(fx_rows: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str], Decimal]:
    return {
        (r["currency"], str(r["date"])): d(r["cny_per_unit"])
        for r in fx_rows
    }


def _to_cny(
    amount: Decimal, currency: str, date: str, fx: Mapping[tuple[str, str], Decimal]
) -> tuple[Decimal, Decimal | None]:
    """按单据发生日汇率折人民币；CNY 汇率为 1，外币缺汇率直接报错（禁止插值）。"""
    if currency == "CNY":
        return money(amount), None
    rate = fx.get((currency, date))
    if rate is None:
        raise ValueError(f"缺少 {currency} 在 {date} 的汇率，禁止插值")
    return money(amount * rate), rate


def build_warehouse(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """整层重建 warehouse 与 marts；返回各表行数用于验收。"""
    orders = _rows(con, "orders")
    cargo = _rows(con, "cargo")
    bridge = _rows(con, "orders_bridge")
    legs = _rows(con, "legs")
    loads = _rows(con, "actual_loads")
    scopes = _rows(con, "cost_scopes")
    docs = _rows(con, "cost_documents")
    receipts = _rows(con, "delivery_receipts")
    fx_rows = _rows(con, "fx")
    fx = _fx_lookup(fx_rows)

    con.execute(SCHEMA_DDL)

    # ---- 货批谱系：递归求 root、depth、叶子标记 ----
    cargo_parent = {r["cargo_id"]: (r["parent"] or None) for r in cargo}

    def root_of(cid: str) -> str:
        cur = cid
        seen: set[str] = set()
        while cur not in seen:
            seen.add(cur)
            parent = cargo_parent.get(cur)
            if not parent:
                return cur
            cur = parent
        raise ValueError(f"货批谱系存在环：{cid}")

    def depth_of(cid: str) -> int:
        depth, cur = 0, cid
        while cargo_parent.get(cur):
            depth += 1
            cur = cargo_parent[cur]
        return depth

    children: dict[str, list[str]] = {}
    for cid, parent in cargo_parent.items():
        if parent:
            children.setdefault(parent, []).append(cid)

    con.execute("BEGIN")
    for r in cargo:
        cid = r["cargo_id"]
        con.execute(
            "INSERT INTO warehouse.dim_cargo VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                cid,
                cargo_parent[cid],
                root_of(cid),
                d(r["weight_kg"]),
                d(r["volume_m3"]),
                r["split_at"],
                cid not in children,
                depth_of(cid),
            ],
        )

    # ---- 订单维度（订单币种汇率：取该订单首张交付回执日期；无交付取最近 fx 日期）----
    order_by_id = {r["order_id"]: r for r in orders}
    receipt_dates: dict[str, list[str]] = {}
    for r in receipts:
        receipt_dates.setdefault(r["order_id"], []).append(str(r["delivered_at"])[:10])

    def order_fx(currency: str, order_id: str) -> Decimal:
        if currency == "CNY":
            return Decimal("1")
        dates = sorted(receipt_dates.get(order_id, []))
        if dates:
            rate = fx.get((currency, dates[0]))
            if rate is not None:
                return rate
        available = sorted(date for cur, date in fx if cur == currency)
        if not available:
            raise ValueError(f"币种 {currency} 无可用汇率")
        return fx[(currency, available[-1])]

    for r in orders:
        con.execute(
            "INSERT INTO warehouse.dim_order VALUES (?, ?, ?, ?, ?, ?)",
            [
                r["order_id"],
                r["customer"],
                d(r["total_kg"]),
                d(r["revenue_amount"]),
                r["currency"],
                order_fx(r["currency"], r["order_id"]),
            ],
        )

    # ---- 订单-货批桥（父流订单归属传给子流）----
    cargo_owner: dict[str, str] = {r["cargo_id"]: r["order_id"] for r in bridge}

    def owner_of(cid: str) -> str | None:
        cur = cid
        while cur:
            if cur in cargo_owner:
                return cargo_owner[cur]
            cur = cargo_parent.get(cur)  # type: ignore[assignment]
        return None

    seen_bridge: set[tuple[str, str]] = set()
    for r in cargo:
        cid = r["cargo_id"]
        owner = owner_of(cid)
        if owner is None:
            continue
        pair = (owner, cid)
        if pair in seen_bridge:
            continue
        seen_bridge.add(pair)
        con.execute(
            "INSERT INTO warehouse.bridge_order_cargo VALUES (?, ?, ?)",
            [owner, cid, root_of(cid)],
        )

    # ---- 段维度 ----
    for r in legs:
        con.execute(
            "INSERT INTO warehouse.dim_leg VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                r["leg_id"], r["mode"], r["origin"], r["destination"],
                r["start_ts"], r["end_ts"], d(r["km"]), r["state"],
                r["state"] == "cancelled",
            ],
        )

    leg_by_id = {r["leg_id"]: r for r in legs}

    # ---- 实载事实：吨公里只在实际有货区段产生，同货同段仅一行 ----
    for r in loads:
        cid, leg_id = r["cargo_id"], r["leg_id"]
        leg = leg_by_id[leg_id]
        owner = owner_of(cid)
        con.execute(
            "INSERT INTO warehouse.fact_actual_load VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                r["load_id"], leg_id, cid, root_of(cid), owner, leg["mode"],
                d(r["weight_kg"]), d(r["volume_m3"]), d(leg["km"]),
                tonne_km(d(r["weight_kg"]), d(leg["km"])),
                leg["start_ts"], leg["end_ts"],
            ],
        )

    # ---- 范围计费数量：按 scope 所在段的实载逐流计算 ----
    loads_by_leg: dict[str, list[dict[str, Any]]] = {}
    for r in loads:
        loads_by_leg.setdefault(r["leg_id"], []).append(r)

    scope_quantities: dict[str, list[dict[str, Any]]] = {}
    for s in scopes:
        scope_id, leg_id, basis = s["scope_id"], s["leg_id"], s["basis"]
        if basis == "fixed":
            continue  # 固定费/取消费不产生计费数量
        vol_factor = d(s["volumetric_kg_m3"])
        entries: list[dict[str, Any]] = []
        for lr in loads_by_leg.get(leg_id, []):
            cid = lr["cargo_id"]
            owner = owner_of(cid)
            if owner is None:
                continue
            actual_kg = d(lr["weight_kg"])
            volume = d(lr["volume_m3"])
            cw = chargeable_weight(actual_kg, volume, vol_factor)
            basis_value = cw if basis == "charge_weight" else volume
            entries.append(
                {
                    "scope_id": scope_id, "leg_id": leg_id, "cargo_id": cid,
                    "order_id": owner, "basis": basis,
                    "actual_kg": actual_kg, "volume_m3": volume,
                    "charge_weight_kg": cw, "basis_value": basis_value,
                }
            )
        scope_quantities[scope_id] = entries
        for e in entries:
            con.execute(
                "INSERT INTO warehouse.fact_scope_quantity VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    e["scope_id"], e["leg_id"], e["cargo_id"], e["order_id"],
                    e["basis"], e["actual_kg"], e["volume_m3"],
                    e["charge_weight_kg"], e["basis_value"],
                ],
            )

    # ---- 范围费用：理论额、报价、发票三段成立 + 价差分解 ----
    docs_by_scope: dict[str, list[_Doc]] = {}
    for r in docs:
        docs_by_scope.setdefault(r["scope_id"], []).append(
            _Doc(kind=r["kind"], amount=d(r["amount"]), date=str(r["date"]))
        )

    scope_cost_rows: list[dict[str, Any]] = []
    for s in scopes:
        scope_id = s["scope_id"]
        currency = s["currency"]
        entries = scope_quantities.get(scope_id, [])
        rate = d(s["rate"]) if s["rate"] not in (None, "") else Decimal()
        minimum = d(s["minimum"]) if s["minimum"] not in (None, "") else Decimal()
        theoretical_origin = minimum
        if entries:
            raw_total = sum(
                (e["basis_value"] * rate for e in entries), Decimal()
            )
            theoretical_origin = max(minimum, money(raw_total))

        def cny(amount: Decimal, date: str) -> tuple[Decimal, Decimal | None]:
            if currency == "CNY":
                return money(amount), None
            r2 = fx[(currency, date)]
            return money(amount * r2), r2

        # 理论合同额没有自身单据日期：取该范围最早单据日期，外币缺单据时取段起始日
        scope_doc_dates = sorted(x.date for x in docs_by_scope.get(scope_id, []))
        theoretical_date = scope_doc_dates[0] if scope_doc_dates else str(leg_by_id[s["leg_id"]]["start_ts"])[:10]
        theoretical_cny, _ = cny(theoretical_origin, theoretical_date)

        quote = next((x for x in docs_by_scope.get(scope_id, []) if x.kind == "quote"), None)
        invoice = next((x for x in docs_by_scope.get(scope_id, []) if x.kind == "invoice"), None)
        payment = next((x for x in docs_by_scope.get(scope_id, []) if x.kind == "payment"), None)

        quote_cny = quote_origin = None
        invoice_cny = invoice_origin = None
        payment_cny = payment_origin = None
        fx_note = None
        if quote:
            quote_origin = quote.amount
            quote_cny, qrate = cny(quote.amount, quote.date)
            if qrate is not None:
                fx_note = f"报价日汇率 {currency}/CNY={qrate}"
        if invoice:
            invoice_origin = invoice.amount
            invoice_cny, irate = cny(invoice.amount, invoice.date)
            if irate is not None:
                fx_note = (fx_note + "；" if fx_note else "") + f"发票日汇率 {currency}/CNY={irate}"
        if payment:
            payment_origin = payment.amount
            payment_cny, prate = cny(payment.amount, payment.date)
            if prate is not None:
                fx_note = (fx_note + "；" if fx_note else "") + f"付款日汇率 {currency}/CNY={prate}"

        # 付款不新增成本；与发票的人民币差为结算汇兑损益
        settlement_fx = (
            money(payment_cny - invoice_cny)
            if payment_cny is not None and invoice_cny is not None
            else Decimal()
        )

        if invoice is not None:
            cost_status = "actual"
            total_origin, total_cny = invoice.amount, invoice_cny
            # 价差分解（发票人民币 − 暂估人民币），两条路径二选一并可加回总差：
            #   数量/计价差 = 外币金额差 × 报价日汇率（同汇率下只看数量与单价）
            #   汇率差     = 发票外币额 × (发票日汇率 − 报价日汇率)
            if quote is not None and currency != "CNY":
                quote_rate = fx[(currency, quote.date)]
                invoice_rate = fx[(currency, invoice.date)]
                qp_variance = money((invoice.amount - quote.amount) * quote_rate)
                fx_variance = money(invoice.amount * (invoice_rate - quote_rate))
            elif quote is not None:
                fx_variance = Decimal()
                qp_variance = money(invoice_cny - quote_cny)
            else:
                fx_variance = Decimal()
                qp_variance = Decimal()
        else:
            cost_status = "estimated"
            if quote is not None:
                total_origin, total_cny = quote.amount, quote_cny
            else:
                total_origin, total_cny = theoretical_origin, theoretical_cny
            fx_variance = Decimal()
            qp_variance = Decimal()

        row = {
            "scope_id": scope_id, "leg_id": s["leg_id"], "basis": s["basis"],
            "currency": currency, "caused_order": s["caused_order"] or None,
            "rate": rate if s["rate"] not in (None, "") else None,
            "minimum": minimum if s["minimum"] not in (None, "") else None,
            "theoretical_origin": theoretical_origin,
            "theoretical_cny": theoretical_cny,
            "quote_origin": quote_origin, "quote_cny": quote_cny,
            "invoice_origin": invoice_origin, "invoice_cny": invoice_cny,
            "payment_origin": payment_origin, "payment_cny": payment_cny,
            "settlement_fx_gain_loss_cny": settlement_fx,
            "fx_variance_cny": fx_variance,
            "quantity_price_variance_cny": qp_variance,
            "total_cny": total_cny,
            "cost_status": cost_status,
            "fx_note": fx_note,
        }
        scope_cost_rows.append(row)
        con.execute(
            """INSERT INTO warehouse.fact_scope_cost
               (scope_id, leg_id, basis, currency, caused_order, rate, minimum,
                theoretical_origin, theoretical_cny, quote_origin, quote_cny,
                invoice_origin, invoice_cny, payment_origin, payment_cny,
                settlement_fx_gain_loss_cny, fx_variance_cny,
                quantity_price_variance_cny, total_cny, cost_status, fx_note)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [row[k] for k in (
                "scope_id", "leg_id", "basis", "currency", "caused_order", "rate",
                "minimum", "theoretical_origin", "theoretical_cny", "quote_origin",
                "quote_cny", "invoice_origin", "invoice_cny", "payment_origin",
                "payment_cny", "settlement_fx_gain_loss_cny", "fx_variance_cny",
                "quantity_price_variance_cny", "total_cny", "cost_status", "fx_note",
            )],
        )

    # ---- 分摊：先在范围层成立总价（最低收费只收一次），再按 basis 分配 ----
    pending: list[tuple[str, Decimal, str, str]] = []
    for row in scope_cost_rows:
        scope_id = row["scope_id"]
        total = row["total_cny"]
        if row["caused_order"]:
            # fixed 取消费：全部归因 caused_order，不按当前实载洗掉
            weights = [(row["caused_order"], Decimal("1"))]
            basis_label = "caused_order_full"
        elif row["basis"] == "fixed":
            # 其余 fixed 共享费：按范围内实际订单数均分。
            # 范围段无实载（空载调拨等）时，没有可归属订单，挂起到待核表，不强行摊入。
            member_orders = sorted(
                {e["order_id"] for e in scope_quantities.get(scope_id, [])}
            )
            if not member_orders:
                pending.append(
                    (scope_id, total, row["cost_status"], "范围段无实载订单，固定共享费无可归属对象")
                )
                continue
            weights = [(oid, Decimal("1")) for oid in member_orders]
            basis_label = "fixed_equal_order"
        else:
            entries = scope_quantities[scope_id]
            per_order: dict[str, Decimal] = {}
            for e in entries:
                per_order[e["order_id"]] = per_order.get(e["order_id"], Decimal()) + e["basis_value"]
            weights = sorted(per_order.items())
            basis_label = row["basis"]

        allocations = allocate(total, weights)
        base_total = sum((w for _, w in weights), Decimal())
        for oid, amount in allocations:
            w = dict(weights)[oid]
            con.execute(
                "INSERT INTO warehouse.fact_cost_allocation VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    scope_id, oid, basis_label, w,
                    money(w / base_total) if base_total > 0 else Decimal(),
                    amount, row["cost_status"],
                ],
            )

    # ---- 交付与收入确认 ----
    order_total_kg = {r["order_id"]: d(r["total_kg"]) for r in orders}
    for r in receipts:
        order = order_by_id[r["order_id"]]
        currency = order["currency"]
        ratio_val = d(r["weight_kg"]) / order_total_kg[r["order_id"]]
        rev_origin = money(d(order["revenue_amount"]) * ratio_val)
        if currency == "CNY":
            rev_rate = Decimal("1")
            rev_cny = rev_origin
        else:
            rev_rate = fx[(currency, str(r["delivered_at"])[:10])]
            rev_cny = money(rev_origin * rev_rate)
        con.execute(
            "INSERT INTO warehouse.fact_delivery_revenue VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                r["receipt_id"], r["order_id"], r["cargo_id"], d(r["weight_kg"]),
                r["delivered_at"], order_total_kg[r["order_id"]], ratio_val,
                rev_origin, currency, rev_rate, rev_cny,
            ],
        )

    for scope_id, total_cny, cost_status, reason in pending:
        con.execute(
            "INSERT INTO warehouse.pending_cost VALUES (?, ?, ?, ?)",
            [scope_id, total_cny, cost_status, reason],
        )

    con.execute("COMMIT")

    # ---- 路径与衔接等待（SQL 重建，依赖上面的物理表）----
    con.execute(
        """
        INSERT INTO warehouse.order_route
        SELECT
            f.order_id,
            f.leg_id,
            ROW_NUMBER() OVER (
                PARTITION BY f.order_id
                ORDER BY l.start_ts, f.leg_id
            ) AS seq,
            l.mode, l.origin, l.destination, l.start_ts, l.end_ts,
            STRING_AGG(f.cargo_id, ',' ORDER BY f.cargo_id) AS cargo_ids,
            SUM(f.weight_kg) AS weight_kg,
            SUM(f.tonne_km) AS tonne_km
        FROM warehouse.fact_actual_load f
        JOIN warehouse.dim_leg l USING (leg_id)
        WHERE f.order_id IS NOT NULL AND l.is_cancelled = FALSE
        GROUP BY f.order_id, f.leg_id, l.mode, l.origin, l.destination,
                 l.start_ts, l.end_ts
        """
    )
    con.execute(
        """
        INSERT INTO warehouse.connection_wait
        WITH ordered AS (
            SELECT
                f.cargo_id,
                f.leg_id,
                l.destination AS connection_point,
                l.end_ts AS arrive_ts,
                LEAD(l.start_ts) OVER (
                    PARTITION BY f.cargo_id ORDER BY l.start_ts, l.leg_id
                ) AS depart_ts,
                LEAD(f.leg_id) OVER (
                    PARTITION BY f.cargo_id ORDER BY l.start_ts, l.leg_id
                ) AS to_leg,
                LEAD(l.origin) OVER (
                    PARTITION BY f.cargo_id ORDER BY l.start_ts, l.leg_id
                ) AS next_origin
            FROM warehouse.fact_actual_load f
            JOIN warehouse.dim_leg l USING (leg_id)
            WHERE l.is_cancelled = FALSE
        )
        SELECT
            cargo_id,
            leg_id AS from_leg,
            to_leg,
            connection_point,
            arrive_ts,
            depart_ts,
            CAST(EXTRACT(EPOCH FROM (depart_ts - arrive_ts)) / 3600 AS DECIMAL(18,4))
                AS wait_hours
        FROM ordered
        WHERE to_leg IS NOT NULL AND next_origin = connection_point
        """
    )

    con.execute(MARTS_DDL)

    counts = {
        name: con.execute(f"SELECT COUNT(*) FROM warehouse.{name}").fetchone()[0]
        for name in (
            "dim_cargo", "dim_order", "dim_leg", "fact_actual_load",
            "fact_scope_quantity", "fact_scope_cost", "fact_cost_allocation",
            "fact_delivery_revenue", "order_route", "connection_wait",
            "pending_cost",
        )
    }
    return counts
