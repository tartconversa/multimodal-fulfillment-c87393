"""资料文件和通用检查器的已有测试。

随附 data/catalog.json 记录的 sha256 与随附 CSV 实际字节在初始交付时即不一致
（9 张表全部，但行数与字段契约正确）。因此本文件：
  * 用临时构造的合规/违规资料测试检查器逻辑（不依赖随附摘要）；
  * 对随附资料只验证字段、行数与引用，不把既有摘要不一致误报为本仓回归。
"""
from pathlib import Path
import hashlib
import json
import tempfile
import unittest

from sourcepack.catalog import inspect, read_table


class CheckerLogicTests(unittest.TestCase):
    def check_bad(self, data, message):
        table = {"columns": [{"name": "id", "type": "string"},
                             {"name": "amount", "type": "number"}], "key": ["id"]}
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "sample.csv"
            path.write_text(data, encoding="utf-8")
            with self.assertRaisesRegex(ValueError, message):
                read_table(path, table)

    def test_duplicate(self):
        self.check_bad("id,amount\na,1\na,2\n", "记录键")

    def test_type(self):
        self.check_bad("id,amount\na,unknown\n", "类型")

    def test_columns(self):
        self.check_bad("id,wrong\na,1\n", "字段")

    def test_nonfinite(self):
        self.check_bad("id,amount\na,nan\n", "类型")


class BundledDataTests(unittest.TestCase):
    """随附资料的结构契约测试；摘要不一致作为已知问题单独断言。"""

    def setUp(self):
        self.root = Path(__file__).resolve().parents[1]
        self.catalog = json.loads((self.root / "data/catalog.json").read_text(encoding="utf-8"))

    def test_row_counts_match(self):
        for table in self.catalog["tables"]:
            path = self.root / table["path"]
            with path.open(encoding="utf-8", newline="") as stream:
                import csv as _csv
                rows = list(_csv.DictReader(stream))
            self.assertEqual(len(rows), table["row_count"], table["name"])

    def test_known_sha_mismatch_is_documented(self):
        mismatches = []
        for table in self.catalog["tables"]:
            digest = hashlib.sha256((self.root / table["path"]).read_bytes()).hexdigest()
            if digest != table["sha256"]:
                mismatches.append(table["name"])
        # 初始交付即存在的摘要不一致；若资料提供方修复了文件，此列表应变空。
        self.assertEqual(
            sorted(mismatches),
            sorted(t["name"] for t in self.catalog["tables"]),
            "随附资料摘要状态变化，请更新 README 与本测试",
        )

    def test_inspect_reports_digest_mismatch(self):
        with self.assertRaisesRegex(ValueError, "资料摘要不一致"):
            inspect(self.root)


if __name__ == "__main__":
    unittest.main()
