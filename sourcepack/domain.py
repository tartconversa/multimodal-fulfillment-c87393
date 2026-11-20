"""联运履约领域规则：谱系、归属、数量口径与分摊原语。

本模块只放与存储无关的纯领域逻辑，所有金额计算使用 :class:`decimal.Decimal`，
结算舍入到分；重量、体积保留来源精度，空值（未知/不适用）绝不按零参与运算。
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable, Mapping, Sequence

# 金额统一保留到人民币分 / 币种最小单位分
CENT = Decimal("0.01")


def d(value: object) -> Decimal:
    """把来源字符串安全转为 Decimal；空串视为未知，由调用方决定如何处理。"""
    if value is None or value == "":
        raise ValueError("空值不是零，不能参与数值运算")
    return Decimal(str(value))


def money(value: Decimal) -> Decimal:
    """结算金额四舍五入到分。"""
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def ratio(part: Decimal, whole: Decimal) -> Decimal:
    """分摊比例，whole 为 0/空时不允许静默归零，由调用方改走其他口径。"""
    if whole <= 0:
        raise ZeroDivisionError("分摊基数必须为正")
    return part / whole


def allocate(total: Decimal, weights: Sequence[tuple[str, Decimal]]) -> list[tuple[str, Decimal]]:
    """按正权重把 total 分摊到各项，尾差给权重最大项；并列按业务键升序。

    返回金额四舍五入到分且合计严格等于 total（total 本身应已到分）。
    权重全部为正时才允许调用；固定费用等均分场景由调用方先构造等权。
    """
    if not weights:
        raise ValueError("没有可分摊对象")
    if any(w < 0 for _, w in weights):
        raise ValueError("分摊权重不能为负")
    positives = [(key, w) for key, w in weights if w > 0]
    if not positives:
        raise ZeroDivisionError("分摊基数全部为零，无法分摊")
    base_total = sum((w for _, w in positives), Decimal())
    raw = [(key, total * (w / base_total)) for key, w in positives]
    rounded = [(key, money(amount)) for key, amount in raw]
    diff = total - sum((amount for _, amount in rounded), Decimal())
    if diff != 0:
        # 尾差给权重最大项；权重并列按业务键升序取首项，保证可复算。
        max_weight = max(w for _, w in positives)
        anchor_key = sorted(key for key, w in positives if w == max_weight)[0]
        rounded = [
            (key, amount + diff if key == anchor_key else amount)
            for key, amount in rounded
        ]
    return rounded


@dataclass(frozen=True)
class CargoNode:
    """一个可拆分实物流（货批支流）。"""

    cargo_id: str
    parent: str | None
    weight_kg: Decimal
    volume_m3: Decimal
    split_at: str | None


class CargoLineage:
    """货批拆分谱系。父流拆分后不再与子流同时计存量。"""

    def __init__(self, rows: Iterable[Mapping[str, str]]):
        self.nodes: dict[str, CargoNode] = {}
        for row in rows:
            parent = row["parent"] or None
            self.nodes[row["cargo_id"]] = CargoNode(
                cargo_id=row["cargo_id"],
                parent=parent,
                weight_kg=d(row["weight_kg"]),
                volume_m3=d(row["volume_m3"]),
                split_at=row["split_at"] or None,
            )
        self._children: dict[str, list[str]] = {}
        for node in self.nodes.values():
            if node.parent:
                self._children.setdefault(node.parent, []).append(node.cargo_id)
        for kids in self._children.values():
            kids.sort()

    def is_split_parent(self, cargo_id: str) -> bool:
        return bool(self._children.get(cargo_id))

    def root_of(self, cargo_id: str) -> str:
        current = cargo_id
        seen = set()
        while current not in seen:
            seen.add(current)
            node = self.nodes.get(current)
            if node is None or node.parent is None:
                return current
            current = node.parent
        raise ValueError(f"货批谱系存在环：{cargo_id}")

    def leaves_under(self, cargo_id: str) -> list[str]:
        """该流之下当前计存量的叶子流；未拆分流返回自身。"""
        kids = self._children.get(cargo_id)
        if not kids:
            return [cargo_id]
        result: list[str] = []
        for child in kids:
            result.extend(self.leaves_under(child))
        return sorted(result)

    def validate_weights(self) -> list[str]:
        """拆分子流重量/体积合计必须等于父流，否则返回差异描述（硬校验由入仓层执行）。"""
        problems: list[str] = []
        for parent_id, kids in self._children.items():
            parent = self.nodes[parent_id]
            child_weight = sum((self.nodes[k].weight_kg for k in kids), Decimal())
            child_volume = sum((self.nodes[k].volume_m3 for k in kids), Decimal())
            if child_weight != parent.weight_kg:
                problems.append(
                    f"货批 {parent_id} 子流重量合计 {child_weight} != 父流 {parent.weight_kg}"
                )
            if child_volume != parent.volume_m3:
                problems.append(
                    f"货批 {parent_id} 子流体积合计 {child_volume} != 父流 {parent.volume_m3}"
                )
        return problems


def chargeable_weight(
    actual_kg: Decimal, volume_m3: Decimal, volumetric_kg_m3: Decimal
) -> Decimal:
    """合同计费重 = max(实际kg, 体积m3 × 体积折算kg/m3)。"""
    return max(actual_kg, volume_m3 * volumetric_kg_m3)


def tonne_km(actual_kg: Decimal, km: Decimal) -> Decimal:
    """吨公里 = 实际kg/1000 × 真实段km；只对实际有货的装载行调用。"""
    if actual_kg <= 0 or km <= 0:
        raise ValueError("吨公里只计正重量、正距离的实际区段")
    return (actual_kg / Decimal("1000")) * km
