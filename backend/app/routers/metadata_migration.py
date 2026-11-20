"""Iceberg元数据迁移清单接口。

写入链路统一收敛到 MetadataMigrationService：
- Iceberg 管理页通过这里生成/恢复/回滚/收口迁移清单；
- 湖上分区页通过这里读取同一版本并校验新分区规格；
- 另一个业务入口通过 shared 接口共享同一份清单结果。

异常与超时都转成 ok=False + 可恢复提示，不向前端抛裸堆栈。
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Query

from app.schemas import ActionResult, PageResult
from app.services.metadata_migration import (
    CONFLICT_MODES,
    DEFAULT_CONFLICT_MODE,
    MANIFEST_STATUSES,
    MetadataMigrationService,
    MigrationError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/metadata_migration", tags=["Iceberg元数据迁移"])

service = MetadataMigrationService()


def _fail(message: str) -> ActionResult:
    """统一的可恢复失败提示：告诉调用方下一步可以怎么做。"""
    return ActionResult(ok=False, message=message)


@router.get("/manifests", response_model=PageResult[dict])
def list_manifests(
    table_name: str | None = Query(default=None, description="按湖表名过滤迁移清单"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """列出迁移清单；没有数据时 items 为空数组，由前端保持空态。"""
    rows = service.list_manifests(table_name)
    total = len(rows)
    start = (page - 1) * size
    return PageResult(items=rows[start:start + size], total=total, page=page, size=size)


@router.post("/manifests", response_model=ActionResult)
def create_manifest(payload: dict) -> ActionResult:
    """生成 Iceberg 元数据迁移清单（拆分区规格、保留旧快照路径与回滚批次）。"""
    table_name = str(payload.get("table_name", "")).strip()
    conflict_mode = str(payload.get("conflict_mode") or DEFAULT_CONFLICT_MODE).strip()
    simulate_interrupt = bool(payload.get("simulate_interrupt", False))
    simulate_timeout = bool(payload.get("simulate_timeout", False))
    force_new_version = bool(payload.get("force_new_version", False))

    if not table_name:
        return _fail("目标表名不能为空，请填写湖表名后重新生成")
    if conflict_mode not in CONFLICT_MODES:
        return _fail(f"冲突收口路线只支持 {'、'.join(CONFLICT_MODES)}，请调整后重试")
    if simulate_timeout:
        # 模拟写清单超时：数据可能只写了一部分，恢复接口会只处理未完成项
        return _fail("迁移清单写入超时，未完成项已保留，请调用恢复接口继续，无需重新生成")
    try:
        manifest = service.create_manifest(
            table_name,
            conflict_mode=conflict_mode,
            simulate_interrupt=simulate_interrupt,
            force_new_version=force_new_version,
        )
    except MigrationError as exc:
        return _fail(str(exc))
    except Exception:  # pragma: no cover - 兜底，保证前端拿到可恢复提示
        logger.exception("生成迁移清单失败 table=%s", table_name)
        return _fail("迁移清单生成异常，请稍后重试；已写入的迁移项可通过恢复接口继续")

    interrupted = manifest["status"] == "已中断"
    message = (
        f"迁移清单 {manifest['manifest_code']} 已生成，存在未完成项，请执行恢复"
        if interrupted else
        f"迁移清单 {manifest['manifest_code']} 已生成，{manifest['snapshot_total']} 个快照全部迁移完成"
    )
    return ActionResult(ok=True, message=message, entry=manifest)


@router.get("/manifests/{manifest_id}", response_model=ActionResult)
def get_manifest(manifest_id: int) -> ActionResult:
    """读取清单明细（清单本体 + 迁移项 + 回滚批次）。"""
    try:
        detail = service.get_manifest_detail(manifest_id)
    except MigrationError as exc:
        return _fail(str(exc))
    return ActionResult(ok=True, message="迁移清单明细读取成功", entry=detail)


@router.post("/manifests/{manifest_id}/resume", response_model=ActionResult)
def resume_manifest(manifest_id: int) -> ActionResult:
    """中断恢复：只处理尚未完成的迁移项，已完成项不重复处理。"""
    try:
        manifest = service.resume_manifest(manifest_id)
    except MigrationError as exc:
        return _fail(str(exc))
    except Exception:  # pragma: no cover
        logger.exception("恢复迁移清单失败 manifest=%s", manifest_id)
        return _fail("恢复过程异常，请稍后再次调用恢复接口，清单状态不会丢失")
    return ActionResult(
        ok=True,
        message=f"恢复完成：{manifest['item_finished']}/{manifest['snapshot_total']} 个迁移项已完成",
        entry=manifest,
    )


@router.post("/manifests/{manifest_id}/rebase", response_model=ActionResult)
def rebase_conflict(manifest_id: int, payload: dict) -> ActionResult:
    """冲突收口：切换旧快照/新规格解释路线，形成新的数据版本。"""
    conflict_mode = str(payload.get("conflict_mode", "")).strip()
    if conflict_mode not in CONFLICT_MODES:
        return _fail(f"冲突收口路线只支持 {'、'.join(CONFLICT_MODES)}，请调整后重试")
    try:
        manifest = service.rebase_conflict(manifest_id, conflict_mode)
    except MigrationError as exc:
        return _fail(str(exc))
    return ActionResult(
        ok=True,
        message=f"冲突已按「{manifest['conflict_mode_label']}」收口，清单数据版本升级为 {manifest['data_version']}",
        entry=manifest,
    )


@router.post("/manifests/{manifest_id}/replay", response_model=ActionResult)
def replay_snapshot(manifest_id: int, payload: dict) -> ActionResult:
    """回放历史快照：确认兼容入口的扫描结果与旧版本一致。"""
    snapshot_id = str(payload.get("snapshot_id", "")).strip()
    if not snapshot_id:
        return _fail("缺少 snapshot_id，请选择要回放的历史快照")
    try:
        result = service.replay_snapshot(manifest_id, snapshot_id)
    except MigrationError as exc:
        return _fail(str(exc))
    except Exception:  # pragma: no cover
        logger.exception("快照回放失败 manifest=%s snapshot=%s", manifest_id, snapshot_id)
        return _fail("快照回放超时或异常，请稍后重试，旧快照读取路径保持不变")
    message = (
        "回放成功：兼容读取入口扫描结果与旧版本一致"
        if result["consistent_with_legacy"] else
        "回放完成：扫描结果与旧版本存在差异，请检查冲突收口路线"
    )
    return ActionResult(ok=result["consistent_with_legacy"], message=message, entry=result)


@router.post("/manifests/{manifest_id}/verify", response_model=ActionResult)
def verify_partition_spec(manifest_id: int) -> ActionResult:
    """湖上分区依据清单该版本校验新分区规格。"""
    try:
        result = service.verify_partition_spec(manifest_id)
    except MigrationError as exc:
        return _fail(str(exc))
    except Exception:  # pragma: no cover
        logger.exception("分区规格校验失败 manifest=%s", manifest_id)
        return _fail("校验超时，请稍后重试；迁移清单未被修改，可安全重新校验")
    message = "新分区规格校验通过" if result["verified"] else result["recoverable_hint"]
    return ActionResult(ok=result["verified"], message=message, entry=result)


@router.post("/manifests/{manifest_id}/rollback", response_model=ActionResult)
def rollback_manifest(manifest_id: int) -> ActionResult:
    """按回滚批次把快照恢复到旧版元数据文本。"""
    try:
        result = service.rollback_manifest(manifest_id)
    except MigrationError as exc:
        return _fail(str(exc))
    return ActionResult(
        ok=True,
        message=f"已按回滚批次 {result['rollback_batch']['batch_code']} 恢复到旧版元数据文本",
        entry=result,
    )


@router.get("/manifests/{manifest_id}/shared", response_model=ActionResult)
def shared_manifest(
    manifest_id: int,
    reader_module: str = Query(default="iceberg_table", description="读取方模块标识"),
) -> ActionResult:
    """跨模块共享读取：两个业务入口读同一清单，返回结果保持一致。"""
    try:
        view = service.shared_view(manifest_id, reader_module=reader_module)
    except MigrationError as exc:
        return _fail(str(exc))
    return ActionResult(ok=True, message="共享迁移清单读取成功", entry=view)


@router.get("/meta/options")
def migration_options() -> dict:
    """前端渲染按钮与状态标签用的枚举口径。"""
    return {
        "manifest_statuses": MANIFEST_STATUSES,
        "conflict_modes": [
            {"value": mode, "label": label}
            for mode, label in zip(CONFLICT_MODES, [
                "按旧快照解释", "按新分区规格解释",
            ])
        ],
    }
