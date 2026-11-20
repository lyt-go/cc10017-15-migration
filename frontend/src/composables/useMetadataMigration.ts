/**
 * Iceberg 元数据迁移清单的共享读写逻辑。
 *
 * Iceberg 管理页与湖上分区页都通过这里访问同一份清单，
 * 两个入口拿到的 data_version / compat_read_entry 保持一致。
 * 网络异常、超时统一转成可读的可恢复提示，不向上抛裸错误。
 */
import { computed, ref } from 'vue'

import { request } from '@/api/client'

const BASE = '/api/metadata_migration'

export interface MigrationItem {
  id: number
  manifest_id: number
  manifest_code: string
  snapshot_id: string
  metadata_version: string
  status: string
  steps: Record<string, string>
  legacy_metadata_text: string
  legacy_partition: { partition_column: string; granularity: string; sort_fields: string }
  new_partition_spec: {
    spec_id: string
    spec_version: number
    partition_column: string
    granularity: string
    transform: string
    file_format: string
    storage_location: string
  }
  reverse_mapping: { spec_id: string; back_to_legacy_text: string; back_to_snapshot: string }
  legacy_snapshot_path: string
  compat_read_entry: string
  interpretation: string
  rolled_back: boolean
  last_error: string | null
  updated_at: string
}

export interface RollbackEntry {
  snapshot_id: string
  metadata_version: string
  spec_id: string
  restore_to_legacy_text: string
  legacy_snapshot_path: string
  status: string
}

export interface RollbackBatch {
  id: number
  manifest_id: number
  manifest_code: string
  batch_code: string
  status: string
  entries: RollbackEntry[]
  created_at: string
  executed_at: string | null
}

export interface MigrationManifest {
  id: number
  manifest_code: string
  table_name: string
  source_metadata_version: string
  conflict_mode: string
  conflict_mode_label: string
  data_version: string
  status: string
  snapshot_total: number
  item_finished: number
  compat_read_entry: string
  legacy_read_retained: boolean
  rollback_retained: boolean
  partition_verified: boolean
  resume_count: number
  created_at: string
  finished_at: string | null
}

export interface ManifestDetail {
  manifest: MigrationManifest
  items: MigrationItem[]
  rollback_batch: RollbackBatch | null
}

export interface ReplayResult {
  manifest_code: string
  snapshot_id: string
  conflict_mode: string
  interpretation: string
  compat_read_entry: string
  legacy_read_path: string
  legacy_scan_range: { source: string; partition_column: string; granularity: string; partition_count: number; scan_scope: string }
  spec_scan_range: { source: string; partition_column: string; granularity: string; partition_count: number; scan_scope: string }
  replay_scan_range: { source: string; partition_column: string; granularity: string; partition_count: number; scan_scope: string; read_through: string }
  consistent_with_legacy: boolean
  interpretation_diverges: boolean
  replayed_at: string
}

export interface VerifyResult {
  manifest_code: string
  data_version: string
  table_name: string
  verified: boolean
  checks: Array<{ snapshot_id: string; spec_id: string; passed: boolean; expected_column: string; actual_column: string }>
  verified_at: string
  recoverable_hint: string | null
}

export interface SharedView {
  reader_module: string
  manifest_code: string
  table_name: string
  data_version: string
  conflict_mode: string
  compat_read_entry: string
  partition_verified: boolean
  status: string
  item_total: number
  item_finished: number
  shared_consistency_key: string
}

async function postAction(path: string, body?: Record<string, unknown>): Promise<{ ok: boolean; message: string; entry: any }> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body ?? {}) })
  return response.json()
}

/**
 * 迁移清单共享状态：单例 ref，两个页面挂载时各自调用 refresh，
 * 读到的始终是后端同一份数据。
 */
const manifests = ref<MigrationManifest[]>([])
const loading = ref(false)
const errorMessage = ref('')
const lastMessage = ref('')

export function useMetadataMigration() {
  const hasManifests = computed(() => manifests.value.length > 0)

  async function refresh(tableName?: string) {
    loading.value = true
    errorMessage.value = ''
    try {
      const query = tableName ? `?table_name=${encodeURIComponent(tableName)}` : ''
      const response = await request(`${BASE}/manifests${query}`)
      if (!response.ok) throw new Error('迁移清单读取失败，请稍后重试')
      const payload = await response.json()
      manifests.value = payload.items ?? []
    } catch (error) {
      errorMessage.value = error instanceof Error ? error.message : '迁移清单读取失败，请检查服务后重试'
    } finally {
      loading.value = false
    }
  }

  async function createManifest(tableName: string, options: { conflictMode: string; simulateInterrupt?: boolean; simulateTimeout?: boolean }) {
    errorMessage.value = ''
    lastMessage.value = ''
    try {
      const result = await postAction(`${BASE}/manifests`, {
        table_name: tableName,
        conflict_mode: options.conflictMode,
        simulate_interrupt: Boolean(options.simulateInterrupt),
        simulate_timeout: Boolean(options.simulateTimeout),
      })
      lastMessage.value = result.message
      if (!result.ok) {
        errorMessage.value = result.message
        return false
      }
      await refresh(tableName)
      return true
    } catch (error) {
      errorMessage.value = `${error instanceof Error ? error.message : '网络异常'}，请稍后重试，已生成的清单不会丢失`
      return false
    }
  }

  async function resumeManifest(manifestId: number, tableName?: string) {
    try {
      const result = await postAction(`${BASE}/manifests/${manifestId}/resume`)
      lastMessage.value = result.message
      if (!result.ok) errorMessage.value = result.message
      await refresh(tableName)
      return result.ok
    } catch (error) {
      errorMessage.value = '恢复请求未送达，请稍后重试；未完成项仍保留在清单中'
      return false
    }
  }

  async function rebaseConflict(manifestId: number, conflictMode: string, tableName?: string) {
    try {
      const result = await postAction(`${BASE}/manifests/${manifestId}/rebase`, { conflict_mode: conflictMode })
      lastMessage.value = result.message
      if (!result.ok) errorMessage.value = result.message
      await refresh(tableName)
      return result.ok
    } catch (error) {
      errorMessage.value = '冲突收口请求失败，请稍后重试，清单数据版本未被修改'
      return false
    }
  }

  async function rollbackManifest(manifestId: number, tableName?: string) {
    try {
      const result = await postAction(`${BASE}/manifests/${manifestId}/rollback`)
      lastMessage.value = result.message
      if (!result.ok) errorMessage.value = result.message
      await refresh(tableName)
      return result.ok
    } catch (error) {
      errorMessage.value = '回滚请求失败，请稍后重试；回滚批次仍保留，可再次执行'
      return false
    }
  }

  async function fetchDetail(manifestId: number): Promise<ManifestDetail | null> {
    try {
      const response = await request(`${BASE}/manifests/${manifestId}`)
      const payload = await response.json()
      if (!payload.ok) {
        errorMessage.value = payload.message
        return null
      }
      return payload.entry as ManifestDetail
    } catch (error) {
      errorMessage.value = '清单明细读取超时，请稍后重试'
      return null
    }
  }

  async function replaySnapshot(manifestId: number, snapshotId: string): Promise<ReplayResult | null> {
    try {
      const result = await postAction(`${BASE}/manifests/${manifestId}/replay`, { snapshot_id: snapshotId })
      lastMessage.value = result.message
      if (!result.ok) {
        errorMessage.value = result.message
        return null
      }
      return result.entry as ReplayResult
    } catch (error) {
      errorMessage.value = '快照回放超时，请稍后重试；旧快照读取路径保持不变'
      return null
    }
  }

  async function verifySpec(manifestId: number): Promise<VerifyResult | null> {
    try {
      const result = await postAction(`${BASE}/manifests/${manifestId}/verify`)
      lastMessage.value = result.message
      if (!result.ok) {
        errorMessage.value = result.message
        return null
      }
      await refresh()
      return result.entry as VerifyResult
    } catch (error) {
      errorMessage.value = '规格校验请求未送达，请稍后重试；迁移清单未被修改'
      return null
    }
  }

  async function fetchSharedView(manifestId: number, readerModule: string): Promise<SharedView | null> {
    try {
      const response = await request(`${BASE}/manifests/${manifestId}/shared?reader_module=${encodeURIComponent(readerModule)}`)
      const payload = await response.json()
      if (!payload.ok) {
        errorMessage.value = payload.message
        return null
      }
      return payload.entry as SharedView
    } catch (error) {
      errorMessage.value = '共享清单读取失败，请稍后重试'
      return null
    }
  }

  return {
    manifests,
    loading,
    errorMessage,
    lastMessage,
    hasManifests,
    refresh,
    createManifest,
    resumeManifest,
    rebaseConflict,
    rollbackManifest,
    fetchDetail,
    replaySnapshot,
    verifySpec,
    fetchSharedView,
  }
}
