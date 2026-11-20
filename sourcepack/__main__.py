"""命令行入口：资料自检、接收、仓表重建、差异核实与集市查询。

用法示例（脚本目录与资料目录分离，全部路径显式传参，可在 python -I 下运行）：

    python -m sourcepack inspect --source /workspace
    python -m sourcepack receive --source /workspace --db /workspace/runs/warehouse.duckdb
    python -m sourcepack build   --db /workspace/runs/warehouse.duckdb
    python -m sourcepack reconcile --db /workspace/runs/warehouse.duckdb
    python -m sourcepack query order --order-id O1 --db ...
    python -m sourcepack query legs --db ...
"""
from __future__ import annotations

import argparse
import datetime
import decimal
import json
from pathlib import Path
from typing import Any

from . import queries
from . import ingest
from .catalog import inspect
from .ingest import connect, receive
from .warehouse import build_warehouse

DEFAULT_DB = "runs/warehouse.duckdb"


class _Encoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        return super().default(obj)


def _print(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, cls=_Encoder))


def _cmd_inspect(args: argparse.Namespace) -> None:
    _print(inspect(Path(args.source)))


def _cmd_receive(args: argparse.Namespace) -> None:
    con = connect(args.db)
    result = receive(
        con,
        args.source,
        note=args.note,
        archive_root=ingest.archive_root_for(args.db),
    )
    if result.status == "loaded":
        counts = build_warehouse(con)
        payload = {**result.to_dict(), "warehouse_rows": counts}
    else:
        payload = result.to_dict()
    _print(payload)
    if result.status == "rolled_back":
        raise SystemExit(2)


def _cmd_build(args: argparse.Namespace) -> None:
    con = connect(args.db)
    _print({"status": "rebuilt", "warehouse_rows": build_warehouse(con)})


def _cmd_reconcile(args: argparse.Namespace) -> None:
    con = connect(args.db)
    bills = queries.bill_check(con, args.scope_id)
    pending = queries.pending_costs(con)
    unbalanced = [b["scope_id"] for b in bills if not b["allocation_balanced"]]
    _print(
        {
            "status": "ok" if not unbalanced else "difference",
            "checked_scopes": len(bills),
            "unbalanced_scopes": unbalanced,
            "pending_costs": pending,
            "bills": bills,
        }
    )
    if unbalanced:
        raise SystemExit(3)


def _cmd_query(args: argparse.Namespace) -> None:
    con = connect(args.db)
    if args.what == "orders":
        payload = queries.order_margin(con, args.order_id)
    elif args.what == "order":
        payload = queries.order_detail(con, args.order_id)
    elif args.what == "legs":
        payload = queries.leg_carriage(con, args.leg_id)
    elif args.what == "modes":
        payload = queries.mode_volume(con, args.mode)
    elif args.what == "waits":
        payload = queries.connection_waits(con)
    elif args.what == "cargo":
        payload = queries.cargo_tracking(con, args.cargo_id)
    elif args.what == "bills":
        payload = queries.bill_check(con, args.scope_id)
    elif args.what == "asof":
        payload = queries.margin_as_of(con, args.as_of)
    else:  # pragma: no cover - argparse 已限定取值
        raise SystemExit(2)
    _print(payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="空铁公联运履约成本仓")
    parser.add_argument("--db", default=DEFAULT_DB, help="DuckDB 文件路径")
    sub = parser.add_subparsers(dest="command", required=True)

    p_inspect = sub.add_parser("inspect", help="资料文件自检（不解释业务）")
    p_inspect.add_argument("--source", default=".", help="资料根目录（含 data/）")
    p_inspect.set_defaults(func=_cmd_inspect)

    p_recv = sub.add_parser("receive", help="接收一批资料并持续装载、重建仓表")
    p_recv.add_argument("--source", default=".", help="资料根目录（含 data/）")
    p_recv.add_argument("--note", default=None)
    p_recv.set_defaults(func=_cmd_receive)

    p_build = sub.add_parser("build", help="从已装载 staging 重建 warehouse/marts")
    p_build.set_defaults(func=_cmd_build)

    p_rec = sub.add_parser("reconcile", help="共享账单核对与价差核实")
    p_rec.add_argument("--scope-id", default=None)
    p_rec.set_defaults(func=_cmd_reconcile)

    p_query = sub.add_parser("query", help="集市查询")
    p_query.add_argument(
        "what",
        choices=["orders", "order", "legs", "modes", "waits", "cargo", "bills", "asof"],
    )
    p_query.add_argument("--order-id", default=None)
    p_query.add_argument("--leg-id", default=None)
    p_query.add_argument("--cargo-id", default=None)
    p_query.add_argument("--scope-id", default=None)
    p_query.add_argument("--mode", default=None)
    p_query.add_argument("--as-of", default=None, help="历史截点日期 YYYY-MM-DD")
    p_query.set_defaults(func=_cmd_query)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    try:
        args.func(args)
    except (KeyError, ValueError, FileNotFoundError) as exc:
        # 业务/资料类错误给出明确信息，不向用户暴露调用栈
        message = exc.args[0] if exc.args else str(exc)
        print(json.dumps({"status": "error", "message": message}, ensure_ascii=False))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
