# 空铁公联运的履约成本仓

把「一票联运」还原成可核实的履约事实：主单/分单/集装器与实物货批、运输段的对应关系只来自明确装载记录；
预订、实载、取消和空载调拨分开保存；共享费用先在合同账单范围成立再分摊；预估成本与正式账单分列，
不把暂估伪装成已确认毛利。面向商务（一票货的实际服务与毛利）和运营（按真实方式的承运量与衔接等待）
提供基于持续装载仓表的查询。

## 环境与安装

Python 3.11，唯一运行依赖见 `requirements.txt`（duckdb 1.4.1），不需要 pytz 或网络。

    python3 -m pip install -r requirements.txt
    python3 -m pip install -e .        # 便于在 python -I 下以模块方式运行

所有命令都支持在隔离模式下从资料目录之外执行，资料目录以路径参数传入，脚本不依赖当前工作目录：

    python3 -I -m sourcepack --db runs/warehouse.duckdb <command> [options]

## 资料接收 → 核实差异 → 查询结果 的完整过程

### 1. 资料自检（只检查文件，不解释业务）

    python3 -I -m sourcepack inspect --source /workspace

核对文件摘要、字段类型、记录键、引用与行数，不做任何估计、批准或结算。

> **已知资料问题**：随附 `data/catalog.json` 记录的 9 张表 sha256 与随附 CSV 的实际字节
> 在初始交付时即全部不一致（字段与行数正确）。因此 `inspect` 当前会报「资料摘要不一致」，
> 这是资料侧问题而非本仓回归；入仓流程只依赖字段契约与行数，不依赖该摘要。资料提供方修正
> 文件后，`tests/test_sources.py` 中对应的已知问题断言会提醒更新。

### 2. 接收资料并持续装载

    python3 -I -m sourcepack --db runs/warehouse.duckdb receive --source /workspace --note "首批"

- 原始事实追加写入 `staging.raw_*`，**永不 UPDATE/DELETE**；同键新行按「整行修订」生效，
  `staging.current_*` 视图只取最新批次，消失的键进入逻辑删除清单；
- 每批原始文件副本归档到数据库旁的 `archive/<batch_id>/`，接收历史在 `staging.receive_batches`；
- 业务时点与接收时点分开保存（原始业务日期 + `ingested_at`/`receive_batch_id`）；
- 跨表硬校验（引用、谱系守恒、同货同段不重复且不超重、取消段无装载、外币单据当日必须有汇率、
  付款必须先有发票等）失败则**整批回滚**，仓内保持接收前状态；
- 相同文件摘要重复接收返回 `duplicate`，不产生新事实。

### 3. 重建仓表（幂等，可随时从已装载事实重算）

    python3 -I -m sourcepack --db runs/warehouse.duckdb build

### 4. 核实共享账单差异

    python3 -I -m sourcepack --db runs/warehouse.duckdb reconcile
    python3 -I -m sourcepack --db runs/warehouse.duckdb query bills --scope-id S2

正式账单为实际发生额，与理论合同费用的差额保留待核、不改写原始数量与单价。外币账单的价差拆为
**汇率差**（同外币额在发票日与报价日汇率下的人民币差）与**数量/计价差**（外币金额差按报价日汇率），
两项可加回总差。`payment` 只表示支付，不新增成本；发票与付款的人民币差单列为结算汇兑损益。
无法归属到订单的共享费（如空载调拨段的固定费）进入 `warehouse.pending_cost` 挂起，不摊入任何一票。

### 5. 集市查询

    # 商务：全部订单毛利（确认毛利与含预估的投影毛利分列）
    python3 -I -m sourcepack --db runs/warehouse.duckdb query orders
    # 商务：一票货的实际路径、货批状态、收入与成本各自确认到哪里、取消费
    python3 -I -m sourcepack --db runs/warehouse.duckdb query order --order-id O1
    # 运营：按实际段查承载的实物和订单
    python3 -I -m sourcepack --db runs/warehouse.duckdb query legs
    # 运营：按真实运输方式的承运量与吨公里（预订/取消段不计入）
    python3 -I -m sourcepack --db runs/warehouse.duckdb query modes
    # 运营：中转站衔接等待（同一实物流相邻实际段，仅同点衔接）
    python3 -I -m sourcepack --db runs/warehouse.duckdb query waits
    # 货批全链路追踪（支持拆分、改道后逐部分查在途/在库/交付）
    python3 -I -m sourcepack --db runs/warehouse.duckdb query cargo --cargo-id G1B
    # 历史截点毛利：晚于截点到达的账单不充当已知事实
    python3 -I -m sourcepack --db runs/warehouse.duckdb query asof --as-of 2026-10-19

## 模型分层与关键口径

| 层 | 内容 |
| --- | --- |
| `staging.raw_*` / `current_*` | 追加保留的原始事实与整行修订后的当前版本 |
| `warehouse.dim_*` | 货批谱系（递归 root/depth/叶子）、订单、段（含取消段、空载段） |
| `warehouse.fact_actual_load` | 明确装载事实；吨公里 = 实际kg/1000 × 真实段km，只在有货区段产生一次 |
| `warehouse.fact_scope_quantity` | 账单范围 × 货批的计费重/体积数量（计费重 = max(实重, 体积×折算系数)） |
| `warehouse.fact_scope_cost` | 理论合同额、暂估、发票、付款各自成立；价差分解与成本状态（actual/estimated） |
| `warehouse.fact_cost_allocation` | 范围总价向订单的分摊；最低收费在范围层只收一次 |
| `warehouse.fact_delivery_revenue` | 按已交付重量占订单总重量确认的收入 |
| `warehouse.order_route` / `connection_wait` | 订单实际路径与中转衔接等待 |
| `warehouse.pending_cost` | 无订单可归属的挂起费用 |
| `marts.*` | 订单毛利、段承运、方式承运量、货批状态、账单核对、取消留痕 |

关键口径（与 `docs/资料口径.md` 对齐）：

- **合单不合并订单、拆单不创造货物**：父流拆分后不与子流同时计存量；订单承运重量按根货批去重，
  同一实物跨多段不加成；吨公里按实际承载区段逐段累加，拼箱段各票按各自实载分别计、不重复。
- **预订不证明实运**：取消段保留在维度并单独留痕，取消费按 `caused_order` 直接归因，不被当前实载洗掉；
  空载调拨段保留维度但不进承运集市。
- **共享费用三段成立**：合同 rate/minimum 算理论额（最低收费先对整个范围生效一次），quote 为暂估，
  invoice 为实际额；分摊仍按 scope 约定 basis（charge_weight/volume/fixed），fixed 非取消费按范围内
  实际订单数均分，尾差给分摊基数最大项、并列按业务键升序。
- **预估≠确认**：无发票的范围以报价给预估成本并标 `estimated`，只进投影毛利；发票到达后转 `actual`，
  已确认毛利只含实际额。未履约订单的成本保留，不随收入确认一次性释放。
- **收入按服务完成范围确认**：部分交付按交付重量比例形成部分收入，余货继续占运输与共享成本，
  不按订单是否关闭一刀切。
- **改道/补账不覆盖历史**：后续批次只追加事实并重建当前层，已经发生的运输与付款始终可追溯，
  毛利解释随正式账单到达而变化，但不靠覆盖预订单或摊平差额取得表面一致。

## 测试

    python3 -m unittest discover -s tests -v
    python3 -m compileall -q sourcepack tests

- `tests/test_sources.py`：资料检查器逻辑 + 随附资料摘要不一致的已知问题断言；
- `tests/test_warehouse.py`：谱系、入仓幂等、整行修订、回滚、最低收费一次成立、取消费直接归因、
  汇率/计价差分解、付款不重复成成本、部分交付收入、预估与确认分列、改道路径、衔接等待、
  空载调拨挂起、历史截点等 30+ 个业务用例，全部在临时目录构造资料，不改动 `data/`。
