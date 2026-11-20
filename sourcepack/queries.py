"""集市查询：商务查一票货的实际服务与毛利，运营按真实方式查承运与衔接。

所有查询只读取 warehouse / marts 已装载仓表，不直接解释来源文件；
时间截点参数按业务事实日期过滤，历史截点不会看到晚于截点才可知的单据。
"""
from __future__ import annotations

from typing import Any

import duckdb


def _records(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    # 先探列名与类型（不实际执行），时间/日期列统一转 VARCHAR，避免对 pytz 的隐式依赖
    probe = con.execute(f"SELECT * FROM ({sql}) LIMIT 0", params or [])
    cols = [item[0] for item in probe.description]
    types = [item[1] for item in probe.description]
    cast_cols = [
        f'CAST("{name}" AS VARCHAR) AS "{name}"'
        if "TIMESTAMP" in str(typ).upper() or "DATE" in str(typ).upper()
        else f'"{name}"'
        for name, typ in zip(cols, types)
    ]
    wrapped = f"SELECT {', '.join(cast_cols)} FROM ({sql})"
    result = con.execute(wrapped, params or [])
    return [dict(zip(cols, row)) for row in result.fetchall()]


def order_margin(con: duckdb.DuckDBPyConnection, order_id: str | None = None) -> list[dict[str, Any]]:
    """订单毛利总览；不传订单则返回全部订单。"""
    sql = "SELECT * FROM marts.mart_order_margin"
    if order_id:
        sql += " WHERE order_id = ?"
    sql += " ORDER BY order_id"
    return _records(con, sql, [order_id] if order_id else [])


def order_detail(con: duckdb.DuckDBPyConnection, order_id: str) -> dict[str, Any]:
    """商务一票查询：约定、实际路径、未履约部分、收入与成本确认到哪里。"""
    margin = _records(con, "SELECT * FROM marts.mart_order_margin WHERE order_id = ?", [order_id])
    if not margin:
        raise KeyError(f"订单不存在：{order_id}")
    routes = _records(
        con,
        """
        SELECT seq, leg_id, mode, origin, destination, start_ts, end_ts,
               cargo_ids, weight_kg, tonne_km
        FROM warehouse.order_route
        WHERE order_id = ? ORDER BY seq
        """,
        [order_id],
    )
    costs = _records(
        con,
        """
        SELECT a.scope_id, s.leg_id, s.basis, a.allocation_basis,
               a.basis_value, a.share_ratio, a.allocated_cny, a.cost_status,
               s.theoretical_cny, s.quote_cny, s.invoice_cny,
               s.fx_variance_cny, s.quantity_price_variance_cny,
               s.settlement_fx_gain_loss_cny, s.fx_note
        FROM warehouse.fact_cost_allocation a
        JOIN warehouse.fact_scope_cost s USING (scope_id)
        WHERE a.order_id = ?
        ORDER BY a.scope_id
        """,
        [order_id],
    )
    revenues = _records(
        con,
        """
        SELECT receipt_id, cargo_id, delivered_kg, delivered_at,
               revenue_share_ratio, revenue_origin, currency,
               fx_cny_per_unit, revenue_cny
        FROM warehouse.fact_delivery_revenue
        WHERE order_id = ? ORDER BY delivered_at, receipt_id
        """,
        [order_id],
    )
    cargo_status = _records(
        con,
        """
        SELECT c.* FROM marts.mart_cargo_status c
        JOIN warehouse.bridge_order_cargo b USING (cargo_id)
        WHERE b.order_id = ? ORDER BY c.cargo_id
        """,
        [order_id],
    )
    cancellations = _records(
        con,
        """
        SELECT * FROM marts.mart_cancelled_legs
        WHERE caused_order = ? ORDER BY leg_id
        """,
        [order_id],
    )
    return {
        "order": margin[0],
        "actual_route": routes,
        "cargo_status": cargo_status,
        "revenue_recognition": revenues,
        "cost_recognition": costs,
        "cancellation_charges": cancellations,
    }


def leg_carriage(con: duckdb.DuckDBPyConnection, leg_id: str | None = None) -> list[dict[str, Any]]:
    """运营按实际运输段查承载的实物和订单。"""
    sql = "SELECT * FROM marts.mart_leg_carriage"
    if leg_id:
        sql += " WHERE leg_id = ?"
    sql += " ORDER BY start_ts, leg_id"
    return _records(con, sql, [leg_id] if leg_id else [])


def mode_volume(con: duckdb.DuckDBPyConnection, mode: str | None = None) -> list[dict[str, Any]]:
    """按真实运输方式统计承运量与吨公里（预订/取消段不计入）。"""
    sql = "SELECT * FROM marts.mart_mode_volume"
    if mode:
        sql += " WHERE mode = ?"
    sql += " ORDER BY mode"
    return _records(con, sql, [mode] if mode else [])


def connection_waits(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """中转衔接等待：同一实物流相邻实际段在同一中转点的停留小时数。"""
    return _records(
        con,
        """
        SELECT w.*, b.order_id
        FROM warehouse.connection_wait w
        LEFT JOIN warehouse.bridge_order_cargo b ON b.cargo_id = w.cargo_id
        ORDER BY w.cargo_id, w.arrive_ts
        """,
    )


def cargo_tracking(con: duckdb.DuckDBPyConnection, cargo_id: str | None = None) -> list[dict[str, Any]]:
    """货批全链路：状态、每一段实际承载、交付情况，支持改道后逐部分追踪。"""
    sql = """
        SELECT s.cargo_id, s.root_cargo_id, s.weight_kg, s.loaded_kg,
               s.delivered_kg, s.in_transit_or_pending_kg, s.status,
               f.leg_id, l.mode, l.origin, l.destination, l.start_ts, l.end_ts,
               f.tonne_km,
               dr.receipt_id, dr.delivered_at
        FROM marts.mart_cargo_status s
        LEFT JOIN warehouse.fact_actual_load f ON f.cargo_id = s.cargo_id
        LEFT JOIN warehouse.dim_leg l ON l.leg_id = f.leg_id
        LEFT JOIN warehouse.fact_delivery_revenue dr
            ON dr.cargo_id = s.cargo_id
    """
    params: list[Any] = []
    if cargo_id:
        sql += " WHERE s.cargo_id = ?"
        params.append(cargo_id)
    sql += " ORDER BY s.cargo_id, f.start_ts, f.leg_id"
    return _records(con, sql, params)


def bill_check(con: duckdb.DuckDBPyConnection, scope_id: str | None = None) -> list[dict[str, Any]]:
    """共享账单核对：理论/暂估/发票/付款、价差分解与分摊合计是否对平。"""
    sql = "SELECT * FROM marts.mart_bill_check"
    if scope_id:
        sql += " WHERE scope_id = ?"
    sql += " ORDER BY scope_id"
    rows = _records(con, sql, [scope_id] if scope_id else [])
    for row in rows:
        row["allocation_balanced"] = (
            row["allocated_total_cny"] is not None
            and row["allocated_total_cny"] == row["total_cny"]
        )
    return rows


def pending_costs(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """待核挂起费用（无订单可归属，未摊入任何一票毛利）。"""
    return _records(con, "SELECT * FROM warehouse.pending_cost ORDER BY scope_id")


def margin_as_of(con: duckdb.DuckDBPyConnection, as_of_date: str) -> list[dict[str, Any]]:
    """历史截点毛利：只使用单据日期 <= 截点的成本与收入事实。

    晚于截点到达的正式账单不计入，相关成本回到暂估口径；
    已发生的运输段（按段止时 <= 截点）照常保留。
    """
    sql = """
        WITH cost_cut AS (
            SELECT a.order_id,
                   SUM(CASE
                         WHEN s.cost_status = 'actual'
                              AND COALESCE(
                                (SELECT MIN(date) FROM staging.current_cost_documents d
                                 WHERE d.scope_id = s.scope_id AND d.kind = 'invoice'),
                                DATE '9999-12-31') <= CAST(? AS DATE)
                         THEN a.allocated_cny ELSE 0 END) AS confirmed_cut_cny,
                   SUM(CASE
                         WHEN COALESCE(
                                (SELECT MIN(date) FROM staging.current_cost_documents d
                                 WHERE d.scope_id = s.scope_id AND d.kind = 'invoice'),
                                DATE '9999-12-31') > CAST(? AS DATE)
                         THEN a.allocated_cny ELSE 0 END) AS not_yet_known_cny,
                   SUM(a.allocated_cny) AS total_cut_cny
            FROM warehouse.fact_cost_allocation a
            JOIN warehouse.fact_scope_cost s USING (scope_id)
            GROUP BY a.order_id
        ),
        rev_cut AS (
            SELECT order_id, SUM(revenue_cny) AS revenue_cut_cny
            FROM warehouse.fact_delivery_revenue
            WHERE CAST(delivered_at AS DATE) <= CAST(? AS DATE)
            GROUP BY order_id
        )
        SELECT o.order_id, o.customer,
               COALESCE(r.revenue_cut_cny, 0) AS recognized_revenue_cny,
               COALESCE(c.confirmed_cut_cny, 0) AS confirmed_cost_cny,
               COALESCE(c.not_yet_known_cny, 0) AS cost_not_yet_known_cny,
               COALESCE(c.total_cut_cny, 0) AS total_cost_cny,
               COALESCE(r.revenue_cut_cny, 0) - COALESCE(c.confirmed_cut_cny, 0)
                   AS confirmed_margin_cny
        FROM warehouse.dim_order o
        LEFT JOIN cost_cut c USING (order_id)
        LEFT JOIN rev_cut r USING (order_id)
        ORDER BY o.order_id
    """
    return _records(con, sql, [as_of_date, as_of_date, as_of_date])
