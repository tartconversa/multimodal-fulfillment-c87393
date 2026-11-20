"""资料接收与持续装载。

设计原则（对应 docs/资料口径.md）：

* 原始文件与接收历史保留：每次接收把文件副本归档到 ``archive/``，逐行写入
  ``raw_*`` 追加表，绝不 UPDATE/DELETE 原始事实；
* 同一记录键的新行表示整行修订：``current_*`` 视图只取每个键最新批次的行，
  本批次缺失的旧键标记删除（逻辑删除，原始行仍在 raw 表）；
* 业务时点与接收时点分开：原始业务日期保留在业务列，另记 ingested_at 与批次；
* 硬校验失败则整批回滚，仓内状态保持接收前一致；
* 相同文件摘要不产生新事实，重复接收直接返回幂等结果。
"""
from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

import duckdb

from .domain import CargoLineage, d

# 源表 -> 记录键（fx 为复合键）
TABLE_KEYS: dict[str, list[str]] = {
    "orders": ["order_id"],
    "cargo": ["cargo_id"],
    "orders_bridge": ["bridge_id"],
    "legs": ["leg_id"],
    "actual_loads": ["load_id"],
    "cost_scopes": ["scope_id"],
    "cost_documents": ["document_id"],
    "delivery_receipts": ["receipt_id"],
    "fx": ["currency", "date"],
}

# 源表 -> 引用关系（列, 目标表, 目标列）
REFERENCES: dict[str, list[tuple[str, str, str]]] = {
    "cargo": [("parent", "cargo", "cargo_id")],
    "orders_bridge": [
        ("order_id", "orders", "order_id"),
        ("cargo_id", "cargo", "cargo_id"),
    ],
    "actual_loads": [
        ("leg_id", "legs", "leg_id"),
        ("cargo_id", "cargo", "cargo_id"),
    ],
    "cost_scopes": [
        ("leg_id", "legs", "leg_id"),
        ("caused_order", "orders", "order_id"),
    ],
    "cost_documents": [("scope_id", "cost_scopes", "scope_id")],
    "delivery_receipts": [
        ("order_id", "orders", "order_id"),
        ("cargo_id", "cargo", "cargo_id"),
    ],
}

RAW_DDL_TEMPLATE = """
CREATE TABLE IF NOT EXISTS staging.raw_{table} (
    {typed_columns},
    receive_batch_id BIGINT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    row_hash VARCHAR NOT NULL
)
"""

# 业务列的 DuckDB 类型（其余审计列统一附加）
COLUMN_TYPES: dict[str, dict[str, str]] = {
    "orders": {
        "order_id": "VARCHAR", "customer": "VARCHAR",
        "total_kg": "DECIMAL(18,4)", "revenue_amount": "DECIMAL(18,2)",
        "currency": "VARCHAR",
    },
    "cargo": {
        "cargo_id": "VARCHAR", "parent": "VARCHAR",
        "weight_kg": "DECIMAL(18,4)", "volume_m3": "DECIMAL(18,6)",
        "split_at": "TIMESTAMPTZ",
    },
    "orders_bridge": {
        "bridge_id": "VARCHAR", "order_id": "VARCHAR", "cargo_id": "VARCHAR",
    },
    "legs": {
        "leg_id": "VARCHAR", "mode": "VARCHAR", "origin": "VARCHAR",
        "destination": "VARCHAR", "start_ts": "TIMESTAMPTZ", "end_ts": "TIMESTAMPTZ",
        "km": "DECIMAL(18,3)", "state": "VARCHAR",
    },
    "actual_loads": {
        "load_id": "VARCHAR", "leg_id": "VARCHAR", "cargo_id": "VARCHAR",
        "weight_kg": "DECIMAL(18,4)", "volume_m3": "DECIMAL(18,6)",
    },
    "cost_scopes": {
        "scope_id": "VARCHAR", "leg_id": "VARCHAR", "basis": "VARCHAR",
        "volumetric_kg_m3": "DECIMAL(18,4)", "rate": "DECIMAL(18,6)",
        "minimum": "DECIMAL(18,2)", "currency": "VARCHAR", "caused_order": "VARCHAR",
    },
    "cost_documents": {
        "document_id": "VARCHAR", "scope_id": "VARCHAR", "kind": "VARCHAR",
        "amount": "DECIMAL(18,2)", "date": "DATE",
    },
    "delivery_receipts": {
        "receipt_id": "VARCHAR", "order_id": "VARCHAR", "cargo_id": "VARCHAR",
        "weight_kg": "DECIMAL(18,4)", "delivered_at": "TIMESTAMPTZ",
    },
    "fx": {
        "currency": "VARCHAR", "date": "DATE",
        "cny_per_unit": "DECIMAL(18,8)",
    },
}

META_DDL = """
CREATE TABLE IF NOT EXISTS staging.receive_batches (
    batch_id BIGINT PRIMARY KEY,
    source_dir VARCHAR NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    note VARCHAR
);
CREATE TABLE IF NOT EXISTS staging.receive_files (
    batch_id BIGINT NOT NULL,
    table_name VARCHAR NOT NULL,
    file_name VARCHAR NOT NULL,
    sha256 VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    PRIMARY KEY (batch_id, table_name)
);
CREATE TABLE IF NOT EXISTS staging.receive_tombstones (
    batch_id BIGINT NOT NULL,
    table_name VARCHAR NOT NULL,
    key_value VARCHAR NOT NULL,
    PRIMARY KEY (batch_id, table_name, key_value)
);
"""


@dataclass
class ReceiveResult:
    """一次接收的回执。"""

    batch_id: int | None
    status: str  # loaded | duplicate | rolled_back
    files: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "status": self.status,
            "files": self.files,
            "errors": self.errors,
            "warnings": self.warnings,
        }


def connect(db_path: str | Path) -> duckdb.DuckDBPyConnection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.execute("CREATE SCHEMA IF NOT EXISTS marts")
    return con


def archive_root_for(db_path: str | Path) -> Path:
    """数据库文件旁的归档根目录，保存每批接收的原始文件副本。"""
    return Path(db_path).resolve().parent


def _row_hash(table: str, row: Mapping[str, str], columns: Sequence[str]) -> str:
    payload = json.dumps(
        [row.get(col, "") for col in columns], ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(f"{table}|{payload}".encode("utf-8")).hexdigest()


def _read_csv(path: Path, columns: Sequence[str]) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None:
            raise ValueError(f"字段不匹配：{path.name}")
        raw_fields = list(reader.fieldnames)
        # 保留字/来源字段名到仓内列名的映射（来源契约不变，仓内安全命名）
        aliases = {"start": "start_ts", "end": "end_ts", "at": "delivered_at"}
        expected = [aliases.get(name, name) for name in raw_fields]
        if expected != list(columns):
            raise ValueError(f"字段不匹配：{path.name}")
        rows = list(reader)
    normalized = []
    for row in rows:
        if any(value is None for value in row.values()):
            raise ValueError(f"列数不匹配：{path.name}")
        normalized.append({aliases.get(k, k): v for k, v in row.items()})
    return normalized


def _typed_value(column: str, value: str) -> Any:
    if value == "":
        return None
    return value


def ensure_raw_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(META_DDL)
    for table, columns in COLUMN_TYPES.items():
        ddl = RAW_DDL_TEMPLATE.format(
            table=table,
            typed_columns=", ".join(f"{name} {typ}" for name, typ in columns.items()),
        )
        con.execute(ddl)


def _current_view_ddl(table: str) -> str:
    key_cols = TABLE_KEYS[table]
    key_expr = " || '\x1f' || ".join(key_cols)
    return f"""
    CREATE OR REPLACE VIEW staging.current_{table} AS
    WITH ranked AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY {", ".join(key_cols)}
            ORDER BY receive_batch_id DESC, row_hash
        ) AS rn,
        MAX(receive_batch_id) OVER () AS latest_batch
        FROM staging.raw_{table}
    )
    SELECT * EXCLUDE (rn, latest_batch)
    FROM ranked
    WHERE rn = 1
      AND NOT EXISTS (
          SELECT 1 FROM staging.receive_tombstones t
          WHERE t.table_name = '{table}'
            AND t.batch_id = ranked.latest_batch
            AND t.key_value = {key_expr}
      )
    """


def refresh_current_views(con: duckdb.DuckDBPyConnection) -> None:
    for table in COLUMN_TYPES:
        con.execute(_current_view_ddl(table))


def _validate(rows_by_table: Mapping[str, list[dict[str, str]]]) -> list[str]:
    """跨表业务硬校验；返回错误列表，空列表表示通过。"""
    errors: list[str] = []

    def keys(table: str) -> set[str]:
        return {r[TABLE_KEYS[table][0]] for r in rows_by_table[table]}

    # 引用完整性（空串表示不适用，不参与引用检查）
    for table, refs in REFERENCES.items():
        for column, target_table, target_col in refs:
            allowed = {r[target_col] for r in rows_by_table[target_table]}
            for row in rows_by_table[table]:
                value = row[column]
                if value and value not in allowed:
                    errors.append(
                        f"资料引用不存在：{table}.{column}={value}"
                    )

    # 谱系：子流合计等于父流，且无环
    lineage = CargoLineage(rows_by_table["cargo"])
    errors.extend(lineage.validate_weights())
    for cargo_id in lineage.nodes:
        try:
            lineage.root_of(cargo_id)
        except ValueError as exc:
            errors.append(str(exc))

    cargo_ids = keys("cargo")
    leg_ids = keys("legs")
    order_ids = keys("orders")
    scope_ids = keys("cost_scopes")

    # 一个实物流只能归属一个客户订单（合单不合并订单，多订单需更细归属，资料未给）
    owner: dict[str, str] = {}
    for row in rows_by_table["orders_bridge"]:
        cid, oid = row["cargo_id"], row["order_id"]
        if cid in owner and owner[cid] != oid:
            errors.append(f"货批 {cid} 归属多个客户订单，无法唯一确定收入成本归属")
        owner[cid] = oid

    # 同货同段装载不重复，且累计装载不超过该流重量；取消段不得有装载
    leg_state = {r["leg_id"]: r["state"] for r in rows_by_table["legs"]}
    load_sum: dict[tuple[str, str], Decimal] = {}
    seen_pair: set[tuple[str, str]] = set()
    for row in rows_by_table["actual_loads"]:
        pair = (row["leg_id"], row["cargo_id"])
        if pair in seen_pair:
            errors.append(f"同货同段重复装载：leg={pair[0]} cargo={pair[1]}")
        seen_pair.add(pair)
        if leg_state.get(row["leg_id"]) == "cancelled":
            errors.append(f"取消段 {row['leg_id']} 不应存在实际装载 {row['load_id']}")
        load_sum[pair] = load_sum.get(pair, d("0")) + d(row["weight_kg"])
    for (leg_id, cargo_id), total_kg in load_sum.items():
        node = lineage.nodes.get(cargo_id)
        if node and total_kg > node.weight_kg:
            errors.append(
                f"装载超重：leg={leg_id} cargo={cargo_id} 累计{total_kg}kg > 流重{node.weight_kg}kg"
            )

    # 父子流不在同一段同时计存量（避免重复吨公里/分摊）
    leg_cargos: dict[str, set[str]] = {}
    for row in rows_by_table["actual_loads"]:
        leg_cargos.setdefault(row["leg_id"], set()).add(row["cargo_id"])
    for leg_id, cargos in leg_cargos.items():
        for cid in cargos:
            node = lineage.nodes.get(cid)
            if node and node.parent and node.parent in cargos:
                errors.append(f"段 {leg_id} 同时装载父流 {node.parent} 与子流 {cid}")

    # 回执：货批归属订单须与回执订单一致；累计交付不超过流重量
    cargo_order = {r["cargo_id"]: r["order_id"] for r in rows_by_table["orders_bridge"]}
    delivered: dict[str, Decimal] = {}
    for row in rows_by_table["delivery_receipts"]:
        cid, oid = row["cargo_id"], row["order_id"]
        if cargo_order.get(cid) and cargo_order[cid] != oid:
            errors.append(f"回执 {row['receipt_id']} 的订单与货批 {cid} 归属不一致")
        delivered[cid] = delivered.get(cid, d("0")) + d(row["weight_kg"])
    for cid, total_kg in delivered.items():
        node = lineage.nodes.get(cid)
        if node and total_kg > node.weight_kg:
            errors.append(f"交付超重：cargo={cid} 累计{total_kg}kg > {node.weight_kg}kg")

    # 单据：同一 scope 每类单据至多一张（修订走同键替换）；币种金额非负
    kind_seen: dict[tuple[str, str], str] = {}
    fx_pairs = {(r["currency"], r["date"]) for r in rows_by_table["fx"]}
    scope_currency = {r["scope_id"]: r["currency"] for r in rows_by_table["cost_scopes"]}
    for row in rows_by_table["cost_documents"]:
        sig = (row["scope_id"], row["kind"])
        if sig in kind_seen:
            errors.append(
                f"范围 {row['scope_id']} 存在多张 {row['kind']} 单据，整行修订应替换同键"
            )
        kind_seen[sig] = row["document_id"]
        if d(row["amount"]) < 0:
            errors.append(f"单据金额不能为负：{row['document_id']}")
        currency = scope_currency.get(row["scope_id"], "CNY")
        if currency != "CNY" and (currency, row["date"]) not in fx_pairs:
            errors.append(
                f"单据 {row['document_id']} 日期 {row['date']} 缺少 {currency} 汇率，禁止插值"
            )

    # 付款必须有对应正式账单；报价不能被支付
    docs_by_scope: dict[str, set[str]] = {}
    for row in rows_by_table["cost_documents"]:
        docs_by_scope.setdefault(row["scope_id"], set()).add(row["kind"])
    for scope_id, kinds in docs_by_scope.items():
        if "payment" in kinds and "invoice" not in kinds:
            errors.append(f"范围 {scope_id} 存在付款但无正式账单，付款不能凭空成立")

    # 取消费范围必须声明 caused_order，且 fixed 才允许直接归因
    for row in rows_by_table["cost_scopes"]:
        if row["caused_order"]:
            if row["basis"] != "fixed":
                errors.append(f"范围 {row['scope_id']} 取消费仅支持 fixed 直接归因")
            if row["caused_order"] not in order_ids:
                errors.append(f"范围 {row['scope_id']} 取消归因订单不存在")

    # 空跑调拨：无任何装载的实际段允许存在（空载），但不能产生按重/按体积共享费
    loaded_legs = {r["leg_id"] for r in rows_by_table["actual_loads"]}
    for row in rows_by_table["cost_scopes"]:
        if (
            row["leg_id"] in leg_state
            and leg_state[row["leg_id"]] == "actual"
            and row["leg_id"] not in loaded_legs
            and row["basis"] in {"charge_weight", "volume"}
            and not row["caused_order"]
        ):
            errors.append(
                f"范围 {row['scope_id']} 所在段 {row['leg_id']} 无实载，不能按重/体积成立共享费"
            )

    # 腿的时间与状态合法
    for row in rows_by_table["legs"]:
        if row["state"] not in {"actual", "cancelled"}:
            errors.append(f"段 {row['leg_id']} 状态非法：{row['state']}")
        if row["start_ts"] >= row["end_ts"]:
            errors.append(f"段 {row['leg_id']} 起时不早于止时")

    return errors


def receive(
    con: duckdb.DuckDBPyConnection,
    source_dir: str | Path,
    note: str | None = None,
    now: datetime | None = None,
    archive_root: str | Path | None = None,
) -> ReceiveResult:
    """从资料目录接收一批 CSV。幂等：全部文件摘要与上批相同则不新增事实。"""
    source_dir = Path(source_dir).resolve()
    catalog_path = source_dir / "data" / "catalog.json"
    if not catalog_path.is_file():
        # 也允许直接传入含 CSV 的目录
        catalog_path = source_dir / "catalog.json"
    data_dir = catalog_path.parent
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    catalog_columns = {
        t["name"]: [c["name"] for c in t["columns"]] for t in catalog["tables"]
    }
    if set(catalog_columns) != set(COLUMN_TYPES):
        raise ValueError("目录声明的表集合与仓内契约不一致")

    ensure_raw_tables(con)

    files_meta: list[dict[str, Any]] = []
    rows_by_table: dict[str, list[dict[str, str]]] = {}
    source_aliases = {"start": "start_ts", "end": "end_ts", "at": "delivered_at"}
    for table, columns in COLUMN_TYPES.items():
        catalog_expected = [source_aliases.get(name, name) for name in catalog_columns[table]]
        if catalog_expected != list(columns):
            raise ValueError(f"字段契约不一致：{table}")
        path = data_dir / f"{table}.csv"
        if not path.is_file():
            raise ValueError(f"缺少资料文件：{path}")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        rows = _read_csv(path, list(columns))
        rows_by_table[table] = rows
        files_meta.append(
            {
                "table_name": table,
                "file_name": path.name,
                "sha256": digest,
                "row_count": len(rows),
            }
        )

    # 幂等：与最近一批逐表摘要完全相同则不产生新事实
    last = con.execute(
        """
        SELECT b.batch_id, f.table_name, f.sha256
        FROM staging.receive_batches b JOIN staging.receive_files f USING (batch_id)
        WHERE b.batch_id = (SELECT MAX(batch_id) FROM staging.receive_batches)
        """
    ).fetchall()
    if last:
        last_batch_id = last[0][0]
        last_digests = {table_name: digest for _, table_name, digest in last}
        current_digests = {f["table_name"]: f["sha256"] for f in files_meta}
        if last_digests == current_digests:
            return ReceiveResult(
                batch_id=last_batch_id,
                status="duplicate",
                files=files_meta,
                warnings=["文件摘要与上一批完全相同，未新增事实"],
            )

    errors = _validate(rows_by_table)
    if errors:
        return ReceiveResult(batch_id=None, status="rolled_back", errors=errors)

    stamp = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    next_batch = con.execute(
        "SELECT COALESCE(MAX(batch_id), 0) + 1 FROM staging.receive_batches"
    ).fetchone()[0]

    try:
        con.execute("BEGIN")
        con.execute(
            "INSERT INTO staging.receive_batches VALUES (?, ?, ?, ?)",
            [next_batch, str(source_dir), stamp, note],
        )
        for meta in files_meta:
            con.execute(
                "INSERT INTO staging.receive_files VALUES (?, ?, ?, ?, ?)",
                [
                    next_batch,
                    meta["table_name"],
                    meta["file_name"],
                    meta["sha256"],
                    meta["row_count"],
                ],
            )
            # 归档原始文件副本（只读事实，永不覆盖）
            root = Path(archive_root) if archive_root else Path("runs").resolve()
            archive_dir = root / "archive" / str(next_batch)
            archive_dir.mkdir(parents=True, exist_ok=True)
            (archive_dir / meta["file_name"]).write_bytes(
                (data_dir / meta["file_name"]).read_bytes()
            )

        for table, columns in COLUMN_TYPES.items():
            placeholders = ", ".join(["?"] * (len(columns) + 3))
            col_list = ", ".join(list(columns) + ["receive_batch_id", "ingested_at", "row_hash"])
            insert_sql = f"INSERT INTO staging.raw_{table} ({col_list}) VALUES ({placeholders})"
            for row in rows_by_table[table]:
                values = [_typed_value(col, row[col]) for col in columns]
                values += [next_batch, stamp, _row_hash(table, row, list(columns))]
                con.execute(insert_sql, values)

            # 逻辑删除：新批次里消失的旧键
            key_cols = TABLE_KEYS[table]
            key_sql = " || '\x1f' || ".join(key_cols)
            new_keys = {
                "\x1f".join(row[k] for k in key_cols) for row in rows_by_table[table]
            }
            old_keys = {
                row[0]
                for row in con.execute(
                    f"SELECT DISTINCT {key_sql} "
                    f"FROM staging.raw_{table} WHERE receive_batch_id < ?",
                    [next_batch],
                ).fetchall()
            }
            for gone in sorted(old_keys - new_keys):
                con.execute(
                    "INSERT INTO staging.receive_tombstones VALUES (?, ?, ?)",
                    [next_batch, table, gone],
                )

        refresh_current_views(con)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    return ReceiveResult(batch_id=next_batch, status="loaded", files=files_meta)
