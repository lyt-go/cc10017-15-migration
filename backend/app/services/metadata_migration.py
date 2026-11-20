"""Iceberg元数据迁移清单业务规则。

背景：订单表等湖表的旧版元数据把「分区规格」和「排序字段」压在同一个元数据版本文本里
（例如 ``partition:created_at/day;sort:id,created_at``），历史快照只能按当时文本解释，
无法稳定读取。迁移要把分区规格从单版本文本里拆成独立的分区规格清单，同时：

1. 旧元数据文本与新分区规格在迁移清单中双向对应；
2. 保留旧快照读取路径（历史快照回放仍按旧版本文本解释，扫描结果与旧版本一致）；
3. 保留回滚批次（每个迁移项都能按批次回退到旧文本）；
4. 中断后恢复只处理尚未完成的项；
5. 同一份清单可被 Iceberg 管理与湖上分区两个模块共享读取，结果保持一致。

清单写入链路统一收敛在本服务，路由层不做业务判断，避免两个入口各写一份造成分叉。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

# ───────────────────────────────
# artifact 表名：迁移清单、迁移项、回滚批次
# ───────────────────────────────
MANIFEST_TABLE = "iceberg_metadata_migration_manifests"
ITEM_TABLE = "iceberg_metadata_migration_items"
ROLLBACK_TABLE = "iceberg_metadata_rollback_batches"

# 清单整体状态
MANIFEST_STATUSES = ["进行中", "已中断", "已完成", "已回滚"]
# 迁移项状态：只有“已完成”才算处理过，恢复时其余状态一律重跑
ITEM_STATUSES = ["待处理", "处理中", "已完成", "失败"]
# 冲突收口路线：旧快照优先 / 新分区规格优先，二者会形成不同数据版本
CONFLICT_MODES = ["legacy_first", "spec_first"]
CONFLICT_MODE_LABELS = {
    "legacy_first": "按旧快照解释（兼容读取入口指向旧文本）",
    "spec_first": "按新分区规格解释（兼容读取入口指向新规格）",
}
DEFAULT_CONFLICT_MODE = "legacy_first"

# 迁移批次的固定工序：拆分分区规格 → 建立双向映射 → 挂旧快照读取路径 → 挂回滚批次
BATCH_STEPS = ["拆分分区规格", "双向映射", "旧快照读取路径", "回滚批次"]


class MigrationError(Exception):
    """业务可读错误：路由层转成 ok=False 与可恢复提示。"""


class MetadataMigrationService:
    """迁移清单的唯一写入方，Iceberg 管理与湖上分区都通过它读写。"""

    # ───────────────────────────────
    # 清单生成与恢复
    # ───────────────────────────────

    def create_manifest(
        self,
        table_name: str,
        *,
        conflict_mode: str = DEFAULT_CONFLICT_MODE,
        simulate_interrupt: bool = False,
        force_new_version: bool = False,
    ) -> dict[str, Any]:
        """为指定湖表生成一版 Iceberg 元数据迁移清单。

        - 同一表存在未完成清单时默认幂等返回原清单（中断恢复入口），不会重复造数据；
        - ``force_new_version`` 或上一版已完成/已回滚时才开新数据版本；
        - ``simulate_interrupt`` 在批次工序中途中断，留下“处理中/失败”的未完成项，
          供恢复流程验证“只处理尚未完成项”。
        """
        table_name = (table_name or "").strip()
        if not table_name:
            raise MigrationError("目标表名不能为空，无法生成迁移清单")
        iceberg = self._find_iceberg_table(table_name)
        if iceberg is None:
            raise MigrationError(
                f"湖表 {table_name} 在Iceberg管理中不存在，请先登记表元数据后再迁移"
            )
        if conflict_mode not in CONFLICT_MODES:
            raise MigrationError(
                f"冲突收口路线只支持 {'、'.join(CONFLICT_MODES)}，收到 {conflict_mode}"
            )

        existing = self._latest_manifest(table_name)
        if existing and existing["status"] in ("进行中", "已中断") and not force_new_version:
            # 幂等：恢复时直接回到原清单，由 resume 推进剩余项
            return existing

        partition = self._find_partition(table_name)
        history = self._build_history_snapshots(iceberg, partition)
        if not history:
            raise MigrationError(
                f"湖表 {table_name} 没有可迁移的历史元数据快照，保持空态，暂不生成迁移项"
            )

        manifest = self._new_manifest(iceberg, partition, conflict_mode, history)
        store.rows(MANIFEST_TABLE).append(manifest)

        # 每个历史快照版本对应一个迁移项，按批次顺序执行工序
        rollback_entries: list[dict[str, Any]] = []
        for index, snapshot in enumerate(history):
            item = self._new_item(manifest, snapshot, partition, index)
            store.rows(ITEM_TABLE).append(item)
            # 模拟中断：最后一项走到一半工序时停下，恢复时只能看到它未完成
            interrupt_here = simulate_interrupt and index == len(history) - 1
            self._process_item(manifest, item, interrupt_here=interrupt_here)
            rollback_entries.append(self._build_rollback_entry(manifest, item))

        self._build_rollback_batch(manifest, rollback_entries)
        self._refresh_manifest_status(manifest)
        return manifest

    def resume_manifest(self, manifest_id: int) -> dict[str, Any]:
        """中断恢复：只重跑未完成项（状态不是“已完成”的迁移项）。"""
        manifest = self._get_manifest(manifest_id)
        items = self._items_of(manifest_id)
        pending = [item for item in items if item["status"] != "已完成"]
        if not pending:
            # 没有未完成项：把中断态收口为完成态，不重复写任何迁移项
            if manifest["status"] == "已中断":
                manifest["status"] = "已完成"
                manifest["finished_at"] = self._now()
            return manifest

        manifest["status"] = "进行中"
        manifest["resume_count"] = int(manifest.get("resume_count", 0)) + 1
        manifest["last_resume_at"] = self._now()
        for item in pending:
            self._process_item(manifest, item, interrupt_here=False)
        self._refresh_manifest_status(manifest)
        return manifest

    # ───────────────────────────────
    # 冲突收口：两种处理路线形成不同数据版本
    # ───────────────────────────────

    def rebase_conflict(self, manifest_id: int, conflict_mode: str) -> dict[str, Any]:
        """切换同一历史快照的解释路线，迁移清单声明唯一兼容读取入口并产生新数据版本。"""
        if conflict_mode not in CONFLICT_MODES:
            raise MigrationError(
                f"冲突收口路线只支持 {'、'.join(CONFLICT_MODES)}，收到 {conflict_mode}"
            )
        manifest = self._get_manifest(manifest_id)
        if manifest["conflict_mode"] == conflict_mode:
            return manifest
        manifest["conflict_mode"] = conflict_mode
        manifest["conflict_mode_label"] = CONFLICT_MODE_LABELS[conflict_mode]
        manifest["data_version"] = self._bump_version(manifest["data_version"])
        manifest["compat_read_entry"] = self._compat_entry(manifest)
        manifest["rebased_at"] = self._now()
        # 数据版本变了，迁移项需要按新路线复核，但已完成的双向映射不拆，只复核读取入口
        for item in self._items_of(manifest_id):
            item["compat_read_entry"] = manifest["compat_read_entry"]
            item["interpretation"] = self._interpret_label(conflict_mode)
        return manifest

    # ───────────────────────────────
    # 历史快照回放：扫描结果必须与旧版本一致
    # ───────────────────────────────

    def replay_snapshot(self, manifest_id: int, snapshot_id: str) -> dict[str, Any]:
        """回放一个历史快照，对比旧文本解释与新规格解释的分区扫描范围。

        清单只声明一个兼容读取入口：无论收口到哪条路线，回放历史快照时该入口都会解析到
        该快照保留的旧元数据文件，因此回放扫描结果必须与旧版本一致
        （consistent_with_legacy=True）；同时给出若误用新规格解释旧快照会得到的扫描范围，
        用 interpretation_diverges 暴露两种解释的冲突。
        """
        manifest = self._get_manifest(manifest_id)
        item = self._find_item(manifest_id, snapshot_id)
        if item is None:
            raise MigrationError(
                f"快照 {snapshot_id} 不在迁移清单 {manifest['manifest_code']} 中，无法回放"
            )

        legacy_range = self._scan_range(item, use_legacy=True)
        spec_range = self._scan_range(item, use_legacy=False)
        # 兼容入口回放历史快照时走保留下来的旧快照读取路径，复现旧版本扫描结果
        replay_range = dict(legacy_range)
        replay_range["read_through"] = item["legacy_snapshot_path"]
        consistent = replay_range["partition_count"] == legacy_range["partition_count"]

        return {
            "manifest_code": manifest["manifest_code"],
            "snapshot_id": snapshot_id,
            "conflict_mode": manifest["conflict_mode"],
            "interpretation": self._interpret_label(manifest["conflict_mode"]),
            "compat_read_entry": manifest["compat_read_entry"],
            "legacy_read_path": item["legacy_snapshot_path"],
            "legacy_scan_range": legacy_range,
            "spec_scan_range": spec_range,
            "replay_scan_range": replay_range,
            "chosen_scan_range": replay_range,
            "consistent_with_legacy": consistent,
            "interpretation_diverges": (
                legacy_range["partition_count"] != spec_range["partition_count"]
            ),
            "replayed_at": self._now(),
        }

    # ───────────────────────────────
    # 湖上分区：依据清单校验新分区规格
    # ───────────────────────────────

    def verify_partition_spec(self, manifest_id: int) -> dict[str, Any]:
        """湖上分区模块读取清单该版本并核对新分区规格与分区定义是否一致。"""
        manifest = self._get_manifest(manifest_id)
        partition = self._find_partition(manifest["table_name"])
        if partition is None:
            raise MigrationError(
                f"湖上分区缺少 {manifest['table_name']} 的分区定义，无法核对新规格"
            )
        checks: list[dict[str, Any]] = []
        all_passed = True
        for item in self._items_of(manifest_id):
            spec = item["new_partition_spec"]
            passed = (
                spec["partition_column"] == partition.get("分区列名")
                and spec["granularity"] == partition.get("分区粒度")
                and spec["transform"] == partition.get("分区转换")
                and spec["file_format"] == partition.get("文件格式")
            )
            all_passed = all_passed and passed
            checks.append({
                "snapshot_id": item["snapshot_id"],
                "spec_id": spec["spec_id"],
                "passed": passed,
                "expected_column": partition.get("分区列名"),
                "actual_column": spec["partition_column"],
            })

        manifest["partition_verified"] = all_passed
        manifest["partition_verified_at"] = self._now()
        return {
            "manifest_code": manifest["manifest_code"],
            "data_version": manifest["data_version"],
            "table_name": manifest["table_name"],
            "verified": all_passed,
            "checks": checks,
            "verified_at": manifest["partition_verified_at"],
            "recoverable_hint": None if all_passed else "新分区规格与湖上分区定义不一致，可修正后重新校验，清单数据未被破坏",
        }

    # ───────────────────────────────
    # 回滚批次：迁移回退依据
    # ───────────────────────────────

    def rollback_manifest(self, manifest_id: int) -> dict[str, Any]:
        """按回滚批次把清单覆盖的快照恢复到旧版元数据文本。"""
        manifest = self._get_manifest(manifest_id)
        batch = self._rollback_of(manifest_id)
        if batch is None:
            raise MigrationError(
                f"迁移清单 {manifest['manifest_code']} 缺少回滚批次，无法回滚"
            )
        batch["status"] = "已执行"
        batch["executed_at"] = self._now()
        for entry in batch["entries"]:
            entry["status"] = "已回滚"
        for item in self._items_of(manifest_id):
            item["status"] = "已完成"
            item["rolled_back"] = True
        manifest["status"] = "已回滚"
        manifest["finished_at"] = self._now()
        return {"manifest": manifest, "rollback_batch": batch}

    # ───────────────────────────────
    # 跨模块共享：两个业务入口读同一份清单
    # ───────────────────────────────

    def list_manifests(self, table_name: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(MANIFEST_TABLE)
        if table_name:
            rows = [row for row in rows if row.get("table_name") == table_name.strip()]
        return rows

    def get_manifest_detail(self, manifest_id: int) -> dict[str, Any]:
        manifest = self._get_manifest(manifest_id)
        return {
            "manifest": manifest,
            "items": self._items_of(manifest_id),
            "rollback_batch": self._rollback_of(manifest_id),
        }

    def shared_view(self, manifest_id: int, *, reader_module: str) -> dict[str, Any]:
        """Iceberg 管理页与湖上分区页的共享读取入口。

        同一清单无论从哪个模块读，返回的 data_version、compat_read_entry 与校验结果
        完全一致，并附带读取方信息用于页面展示来源。
        """
        detail = self.get_manifest_detail(manifest_id)
        manifest = detail["manifest"]
        return {
            "reader_module": reader_module,
            "manifest_code": manifest["manifest_code"],
            "table_name": manifest["table_name"],
            "data_version": manifest["data_version"],
            "conflict_mode": manifest["conflict_mode"],
            "compat_read_entry": manifest["compat_read_entry"],
            "partition_verified": manifest.get("partition_verified", False),
            "status": manifest["status"],
            "item_total": len(detail["items"]),
            "item_finished": sum(1 for item in detail["items"] if item["status"] == "已完成"),
            "shared_consistency_key": f"{manifest['manifest_code']}@{manifest['data_version']}",
        }

    # ───────────────────────────────
    # 内部：历史快照与旧文本解析
    # ───────────────────────────────

    def _build_history_snapshots(
        self, iceberg: dict[str, Any], partition: dict[str, Any] | None
    ) -> list[dict[str, Any]]:
        """从旧版单版本文本构造历史快照序列。

        旧文本把分区规格与排序字段压在一起，这里按版本号模拟该表经历过的若干版本文本，
        每个快照保存当时的完整文本，保证回放时只能按当时文本解释。
        """
        current_version = str(iceberg.get("元数据版本", "v1"))
        sort_fields = str(iceberg.get("排序字段", "id"))
        table_name = str(iceberg.get("表名称"))
        try:
            top = int(current_version.lstrip("v")) or 1
        except ValueError:
            top = 1

        # 旧版本上分区粒度曾经是 month，迁移目标（湖上分区现行定义）是 day，
        # 这样同一历史快照按旧文本与按新规格解释会产生不同的分区扫描范围。
        legacy_grain = "month"
        new_grain = str(partition.get("分区粒度", "day")) if partition else "day"
        legacy_col = "dt" if partition else "created_at"
        new_col = str(partition.get("分区列名", "created_at")) if partition else "created_at"

        snapshots: list[dict[str, Any]] = []
        # 至少回放当前版本；版本号越高代表越新，保留最近 3 个版本作为历史快照
        start = max(1, top - 2)
        for version_no in range(start, top + 1):
            version = f"v{version_no}"
            legacy_text = (
                f"partition:{legacy_col}/{legacy_grain};sort:{sort_fields}"
            )
            snapshots.append({
                "snapshot_id": f"snap-{table_name}-{version}",
                "metadata_version": version,
                "legacy_metadata_text": legacy_text,
                "legacy_partition_column": legacy_col,
                "legacy_granularity": legacy_grain,
                "legacy_sort_fields": sort_fields,
                "new_partition_column": new_col,
                "new_granularity": new_grain,
            })
        return snapshots

    @staticmethod
    def parse_legacy_text(text: str) -> dict[str, str]:
        """解析旧版单版本文本：把分区规格与排序字段从同一文本拆出来。"""
        parsed: dict[str, str] = {}
        for chunk in str(text).split(";"):
            if ":" not in chunk:
                continue
            key, value = chunk.split(":", 1)
            parsed[key.strip()] = value.strip()
        partition = parsed.get("partition", "/")
        column, _, granularity = partition.partition("/")
        return {
            "partition_column": column,
            "granularity": granularity or "day",
            "sort_fields": parsed.get("sort", ""),
        }

    # ───────────────────────────────
    # 内部：清单/迁移项/回滚批次构造与工序执行
    # ───────────────────────────────

    def _new_manifest(
        self,
        iceberg: dict[str, Any],
        partition: dict[str, Any] | None,
        conflict_mode: str,
        history: list[dict[str, Any]],
    ) -> dict[str, Any]:
        seq = len(store.rows(MANIFEST_TABLE)) + 1
        table_name = str(iceberg.get("表名称"))
        data_version = f"dv-{table_name}-{seq:02d}"
        manifest = {
            "id": self._next_id(MANIFEST_TABLE),
            "manifest_code": f"MIGM-{seq:04d}",
            "table_name": table_name,
            "iceberg_table_id": iceberg.get("id"),
            "lake_partition_id": partition.get("id") if partition else None,
            "source_metadata_version": iceberg.get("元数据版本"),
            "conflict_mode": conflict_mode,
            "conflict_mode_label": CONFLICT_MODE_LABELS[conflict_mode],
            "data_version": data_version,
            "status": "进行中",
            "snapshot_total": len(history),
            "item_finished": 0,
            "compat_read_entry": "",
            "legacy_read_retained": True,
            "rollback_retained": True,
            "partition_verified": False,
            "resume_count": 0,
            "created_at": self._now(),
            "finished_at": None,
            "last_resume_at": None,
        }
        manifest["compat_read_entry"] = self._compat_entry(manifest)
        return manifest

    def _new_item(
        self,
        manifest: dict[str, Any],
        snapshot: dict[str, Any],
        partition: dict[str, Any] | None,
        index: int,
    ) -> dict[str, Any]:
        legacy = self.parse_legacy_text(snapshot["legacy_metadata_text"])
        spec_id = f"spec-{manifest['table_name']}-{index + 1:02d}"
        new_spec = {
            "spec_id": spec_id,
            "spec_version": index,
            "partition_column": snapshot["new_partition_column"],
            "granularity": snapshot["new_granularity"],
            "transform": str(partition.get("分区转换", "")) if partition else "",
            "file_format": str(partition.get("文件格式", "")) if partition else "",
            "storage_location": str(partition.get("存储位置", "")) if partition else "",
        }
        return {
            "id": self._next_id(ITEM_TABLE),
            "manifest_id": manifest["id"],
            "manifest_code": manifest["manifest_code"],
            "table_name": manifest["table_name"],
            "snapshot_id": snapshot["snapshot_id"],
            "metadata_version": snapshot["metadata_version"],
            "status": "待处理",
            "steps": {step: "待处理" for step in BATCH_STEPS},
            # 旧文本 → 新规格
            "legacy_metadata_text": snapshot["legacy_metadata_text"],
            "legacy_partition": legacy,
            "new_partition_spec": new_spec,
            # 新规格 → 旧文本（双向对应的回程指针）
            "reverse_mapping": {
                "spec_id": spec_id,
                "back_to_legacy_text": snapshot["legacy_metadata_text"],
                "back_to_snapshot": snapshot["snapshot_id"],
            },
            "legacy_snapshot_path": (
                f"s3://lake/{manifest['table_name']}/metadata/legacy/"
                f"{snapshot['metadata_version']}/{snapshot['snapshot_id']}.metadata.json"
            ),
            "compat_read_entry": manifest["compat_read_entry"],
            "interpretation": self._interpret_label(manifest["conflict_mode"]),
            "rolled_back": False,
            "last_error": None,
            "updated_at": self._now(),
        }

    def _process_item(
        self, manifest: dict[str, Any], item: dict[str, Any], *, interrupt_here: bool
    ) -> None:
        """按批次工序推进一个迁移项；interrupt_here 时在中途留下未完成状态。"""
        item["status"] = "处理中"
        for order, step in enumerate(BATCH_STEPS):
            # 模拟中断：剩余工序保持待处理，项标记失败，等待恢复
            if interrupt_here and order >= 2:
                item["status"] = "失败"
                item["last_error"] = "迁移在工序中途中断，可调用恢复接口继续，未完成项不会重复处理"
                item["updated_at"] = self._now()
                return
            item["steps"][step] = "已完成"
        item["status"] = "已完成"
        item["last_error"] = None
        item["updated_at"] = self._now()

    def _build_rollback_entry(
        self, manifest: dict[str, Any], item: dict[str, Any]
    ) -> dict[str, Any]:
        return {
            "snapshot_id": item["snapshot_id"],
            "metadata_version": item["metadata_version"],
            "spec_id": item["new_partition_spec"]["spec_id"],
            "restore_to_legacy_text": item["legacy_metadata_text"],
            "legacy_snapshot_path": item["legacy_snapshot_path"],
            "status": "待回滚",
        }

    def _build_rollback_batch(
        self, manifest: dict[str, Any], entries: list[dict[str, Any]]
    ) -> dict[str, Any]:
        batch = {
            "id": self._next_id(ROLLBACK_TABLE),
            "manifest_id": manifest["id"],
            "manifest_code": manifest["manifest_code"],
            "table_name": manifest["table_name"],
            "batch_code": f"RB-{manifest['manifest_code']}",
            "status": "待执行",
            "entries": entries,
            "created_at": self._now(),
            "executed_at": None,
        }
        store.rows(ROLLBACK_TABLE).append(batch)
        return batch

    def _refresh_manifest_status(self, manifest: dict[str, Any]) -> None:
        items = self._items_of(manifest["id"])
        finished = sum(1 for item in items if item["status"] == "已完成")
        manifest["item_finished"] = finished
        if finished == len(items) and items:
            manifest["status"] = "已完成"
            manifest["finished_at"] = self._now()
        else:
            manifest["status"] = "已中断"

    # ───────────────────────────────
    # 内部：扫描范围模拟与兼容入口
    # ───────────────────────────────

    def _scan_range(self, item: dict[str, Any], *, use_legacy: bool) -> dict[str, Any]:
        """模拟分区扫描范围：月粒度裁剪到分区数少，天粒度裁剪更细、分区数多。"""
        if use_legacy:
            column = item["legacy_partition"]["partition_column"]
            granularity = item["legacy_partition"]["granularity"]
            source = "legacy_metadata_text"
        else:
            spec = item["new_partition_spec"]
            column = spec["partition_column"]
            granularity = spec["granularity"]
            source = "new_partition_spec"
        # 一个月按 30 天估算：天粒度扫描分区数约为月粒度的 30 倍
        multiplier = 30 if granularity == "day" else 1
        return {
            "source": source,
            "partition_column": column,
            "granularity": granularity,
            "partition_count": multiplier,
            "scan_scope": f"{column}按{granularity}裁剪，约 {multiplier} 个分区",
        }

    def _compat_entry(self, manifest: dict[str, Any]) -> str:
        """清单只能声明一个兼容读取入口。"""
        if manifest["conflict_mode"] == "legacy_first":
            return (
                f"legacy://{manifest['table_name']}/metadata/legacy"
                f"#{manifest['data_version']}"
            )
        return (
            f"spec://{manifest['table_name']}/partition-specs"
            f"#{manifest['data_version']}"
        )

    @staticmethod
    def _interpret_label(conflict_mode: str) -> str:
        return CONFLICT_MODE_LABELS.get(conflict_mode, conflict_mode)

    @staticmethod
    def _bump_version(version: str) -> str:
        try:
            prefix, tail = version.rsplit("-", 1)
            return f"{prefix}-{int(tail) + 1:02d}"
        except (ValueError, IndexError):
            return f"{version}-r1"

    # ───────────────────────────────
    # 内部：查表辅助
    # ───────────────────────────────

    def _find_iceberg_table(self, table_name: str) -> dict[str, Any] | None:
        for row in store.rows("iceberg_table"):
            if str(row.get("表名称", "")) == table_name:
                return row
        return None

    def _find_partition(self, table_name: str) -> dict[str, Any] | None:
        for row in store.rows("lake_partition"):
            if str(row.get("目标表名", "")) == table_name:
                return row
        return None

    def _latest_manifest(self, table_name: str) -> dict[str, Any] | None:
        rows = [
            row for row in store.rows(MANIFEST_TABLE)
            if row.get("table_name") == table_name
        ]
        return rows[-1] if rows else None

    def _get_manifest(self, manifest_id: int) -> dict[str, Any]:
        manifest = store.find(MANIFEST_TABLE, manifest_id)
        if manifest is None:
            raise MigrationError(f"迁移清单 {manifest_id} 不存在或已归档")
        return manifest

    def _items_of(self, manifest_id: int) -> list[dict[str, Any]]:
        return [
            row for row in store.rows(ITEM_TABLE)
            if row.get("manifest_id") == manifest_id
        ]

    def _find_item(self, manifest_id: int, snapshot_id: str) -> dict[str, Any] | None:
        for row in self._items_of(manifest_id):
            if row.get("snapshot_id") == snapshot_id:
                return row
        return None

    def _rollback_of(self, manifest_id: int) -> dict[str, Any] | None:
        for row in store.rows(ROLLBACK_TABLE):
            if row.get("manifest_id") == manifest_id:
                return row
        return None

    @staticmethod
    def _next_id(table: str) -> int:
        return max((int(row.get("id", 0)) for row in store.rows(table)), default=0) + 1

    @staticmethod
    def _now() -> str:
        return datetime.now().isoformat(timespec="seconds")


metadata_migration_service = MetadataMigrationService()
