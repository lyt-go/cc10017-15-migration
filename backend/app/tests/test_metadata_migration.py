"""Iceberg元数据迁移清单测试。

覆盖：
1. 生成清单：旧文本↔新规格双向对应、旧快照读取路径、回滚批次；
2. 中断恢复：只处理未完成项，重复恢复幂等；
3. 快照回放：兼容入口扫描结果与旧版本一致；
4. 冲突收口：两条路线形成不同数据版本，清单只有一个兼容读取入口；
5. 湖上分区校验新规格；
6. 跨模块共享读取结果一致；
7. 回滚批次恢复旧文本；
8. 空态与超时可恢复提示。
"""
from __future__ import annotations

import unittest

from app.routers.metadata_migration import router
from app.services.metadata_migration import (
    ITEM_TABLE,
    MANIFEST_TABLE,
    ROLLBACK_TABLE,
    MetadataMigrationService,
    MigrationError,
)
from app.store import store


class MigrationTestBase(unittest.TestCase):
    def setUp(self) -> None:
        # 每个用例清空迁移相关 artifact 表，避免用例间相互污染
        for table in (MANIFEST_TABLE, ITEM_TABLE, ROLLBACK_TABLE):
            store.rows(table).clear()
        self.svc = MetadataMigrationService()


class TestManifestGeneration(MigrationTestBase):
    def test_generate_manifest_splits_spec_and_keeps_legacy_path(self) -> None:
        manifest = self.svc.create_manifest("orders")
        self.assertEqual(manifest["status"], "已完成")
        self.assertGreaterEqual(manifest["snapshot_total"], 1)
        self.assertTrue(manifest["legacy_read_retained"])
        self.assertTrue(manifest["rollback_retained"])

        detail = self.svc.get_manifest_detail(manifest["id"])
        items = detail["items"]
        # 每个迁移项都保留旧快照读取路径
        self.assertTrue(all(item["legacy_snapshot_path"].endswith(".metadata.json") for item in items))
        # 旧文本与新规格双向对应
        first = items[0]
        self.assertIn("partition:", first["legacy_metadata_text"])
        self.assertIn("sort:", first["legacy_metadata_text"])
        self.assertEqual(first["new_partition_spec"]["partition_column"], "created_at")
        self.assertEqual(
            first["reverse_mapping"]["back_to_legacy_text"],
            first["legacy_metadata_text"],
        )
        self.assertEqual(first["reverse_mapping"]["back_to_snapshot"], first["snapshot_id"])
        # 回滚批次覆盖全部迁移项
        batch = detail["rollback_batch"]
        self.assertIsNotNone(batch)
        self.assertEqual(len(batch["entries"]), len(items))
        self.assertEqual(
            batch["entries"][0]["restore_to_legacy_text"],
            first["legacy_metadata_text"],
        )

    def test_legacy_text_partition_and_sort_are_split(self) -> None:
        parsed = self.svc.parse_legacy_text("partition:dt/month;sort:id,created_at")
        self.assertEqual(parsed["partition_column"], "dt")
        self.assertEqual(parsed["granularity"], "month")
        self.assertEqual(parsed["sort_fields"], "id,created_at")

    def test_manifest_for_unknown_table_is_recoverable(self) -> None:
        with self.assertRaises(MigrationError):
            self.svc.create_manifest("not_exist_table")

    def test_invalid_conflict_mode_rejected(self) -> None:
        with self.assertRaises(MigrationError):
            self.svc.create_manifest("orders", conflict_mode="wrong_route")


class TestResumeAfterInterrupt(MigrationTestBase):
    def test_resume_only_processes_unfinished_items(self) -> None:
        manifest = self.svc.create_manifest("orders", simulate_interrupt=True)
        self.assertEqual(manifest["status"], "已中断")
        items = self.svc._items_of(manifest["id"])
        unfinished = [item for item in items if item["status"] != "已完成"]
        self.assertTrue(unfinished)
        unfinished_ids = {item["id"] for item in unfinished}
        finished_before = {item["id"] for item in items if item["status"] == "已完成"}

        resumed = self.svc.resume_manifest(manifest["id"])
        self.assertEqual(resumed["status"], "已完成")
        items_after = self.svc._items_of(manifest["id"])
        # 之前已完成的项 updated_at 之外不被重建：迁移项总数不变
        self.assertEqual(len(items_after), len(items))
        self.assertTrue(all(item["status"] == "已完成" for item in items_after))
        # 被恢复的恰好是之前未完成的项
        self.assertEqual(
            {item["id"] for item in items_after if item["id"] in unfinished_ids},
            unfinished_ids,
        )
        self.assertTrue(finished_before.issubset({item["id"] for item in items_after}))
        self.assertEqual(resumed["resume_count"], 1)

    def test_resume_is_idempotent_when_nothing_pending(self) -> None:
        manifest = self.svc.create_manifest("orders")
        item_count_before = len(self.svc._items_of(manifest["id"]))
        self.svc.resume_manifest(manifest["id"])
        self.svc.resume_manifest(manifest["id"])
        self.assertEqual(len(self.svc._items_of(manifest["id"])), item_count_before)
        self.assertEqual(manifest["resume_count"], 0)

    def test_create_is_idempotent_for_interrupted_manifest(self) -> None:
        first = self.svc.create_manifest("orders", simulate_interrupt=True)
        again = self.svc.create_manifest("orders")
        self.assertEqual(first["id"], again["id"])
        self.assertEqual(len(store.rows(MANIFEST_TABLE)), 1)


class TestSnapshotReplay(MigrationTestBase):
    def test_legacy_first_replay_matches_old_version(self) -> None:
        manifest = self.svc.create_manifest("orders", conflict_mode="legacy_first")
        items = self.svc._items_of(manifest["id"])
        result = self.svc.replay_snapshot(manifest["id"], items[0]["snapshot_id"])
        self.assertTrue(result["consistent_with_legacy"])
        # 旧文本月粒度与新规格天粒度的扫描范围确实不同，冲突被显式暴露
        self.assertNotEqual(
            result["legacy_scan_range"]["partition_count"],
            result["spec_scan_range"]["partition_count"],
        )
        self.assertTrue(result["interpretation_diverges"])
        # 兼容入口唯一且指向旧文本路径
        self.assertTrue(result["compat_read_entry"].startswith("legacy://"))
        self.assertEqual(result["replay_scan_range"]["source"], "legacy_metadata_text")

    def test_spec_first_route_forms_new_data_version(self) -> None:
        manifest = self.svc.create_manifest("orders", conflict_mode="legacy_first")
        old_version = manifest["data_version"]
        rebased = self.svc.rebase_conflict(manifest["id"], "spec_first")
        self.assertNotEqual(rebased["data_version"], old_version)
        self.assertEqual(rebased["conflict_mode"], "spec_first")
        self.assertIn("新分区规格", rebased["conflict_mode_label"])
        self.assertTrue(rebased["compat_read_entry"].startswith("spec://"))
        items = self.svc._items_of(manifest["id"])
        result = self.svc.replay_snapshot(manifest["id"], items[0]["snapshot_id"])
        # 切换路线后回放历史快照，扫描结果仍与旧版本一致
        self.assertTrue(result["consistent_with_legacy"])
        # 兼容入口虽指向新规格清单，回放仍通过保留的旧快照路径读取
        self.assertEqual(
            result["replay_scan_range"]["read_through"], result["legacy_read_path"]
        )
        self.assertTrue(result["interpretation_diverges"])
        # 旧快照路径始终保留
        self.assertTrue(result["legacy_read_path"].endswith(".metadata.json"))

    def test_replay_unknown_snapshot_is_recoverable(self) -> None:
        manifest = self.svc.create_manifest("orders")
        with self.assertRaises(MigrationError):
            self.svc.replay_snapshot(manifest["id"], "snap-not-exist")


class TestPartitionVerificationAndSharing(MigrationTestBase):
    def test_lake_partition_verifies_new_spec(self) -> None:
        manifest = self.svc.create_manifest("orders")
        result = self.svc.verify_partition_spec(manifest["id"])
        self.assertTrue(result["verified"])
        self.assertTrue(all(check["passed"] for check in result["checks"]))
        self.assertTrue(manifest["partition_verified"])

    def test_shared_view_consistent_across_modules(self) -> None:
        manifest = self.svc.create_manifest("orders")
        iceberg_view = self.svc.shared_view(manifest["id"], reader_module="iceberg_table")
        partition_view = self.svc.shared_view(manifest["id"], reader_module="lake_partition")
        # 两个业务入口读到的数据版本与兼容入口完全一致
        self.assertEqual(
            iceberg_view["shared_consistency_key"],
            partition_view["shared_consistency_key"],
        )
        self.assertEqual(iceberg_view["data_version"], partition_view["data_version"])
        self.assertEqual(iceberg_view["compat_read_entry"], partition_view["compat_read_entry"])
        self.assertNotEqual(iceberg_view["reader_module"], partition_view["reader_module"])

    def test_rollback_batch_restores_legacy_text(self) -> None:
        manifest = self.svc.create_manifest("orders")
        result = self.svc.rollback_manifest(manifest["id"])
        self.assertEqual(manifest["status"], "已回滚")
        batch = result["rollback_batch"]
        self.assertEqual(batch["status"], "已执行")
        self.assertTrue(all(entry["status"] == "已回滚" for entry in batch["entries"]))
        # 回滚依据就是旧版元数据文本
        self.assertIn("partition:", batch["entries"][0]["restore_to_legacy_text"])


class TestEmptyStateAndTimeout(unittest.TestCase):
    def setUp(self) -> None:
        for table in (MANIFEST_TABLE, ITEM_TABLE, ROLLBACK_TABLE):
            store.rows(table).clear()
        self.svc = MetadataMigrationService()

    @staticmethod
    def _post_endpoint(path: str):
        for route in router.routes:
            if route.path == path and "POST" in route.methods:
                return route.endpoint
        raise AssertionError(f"POST {path} 路由未注册")

    def test_empty_state_when_no_manifest(self) -> None:
        self.assertEqual(self.svc.list_manifests(), [])
        self.assertEqual(self.svc.list_manifests("orders"), [])

    def test_router_timeout_returns_recoverable_hint(self) -> None:
        # 直接调路由函数，验证超时被转成 ok=False 的可恢复提示
        endpoint = self._post_endpoint("/api/metadata_migration/manifests")
        result = endpoint({
            "table_name": "orders",
            "simulate_timeout": True,
        })
        self.assertFalse(result.ok)
        self.assertIn("超时", result.message)
        self.assertIn("恢复", result.message)
        # 超时没有写入半成品清单
        self.assertEqual(store.rows(MANIFEST_TABLE), [])

    def test_router_missing_table_returns_hint(self) -> None:
        endpoint = self._post_endpoint("/api/metadata_migration/manifests")
        result = endpoint({"table_name": ""})
        self.assertFalse(result.ok)
        self.assertIn("表名", result.message)


if __name__ == "__main__":
    unittest.main()
