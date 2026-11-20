<template>
  <section class="migration-panel" :data-testid="`migration-panel-${mode}`">
    <header class="migration-head">
      <div>
        <h3>Iceberg元数据迁移清单</h3>
        <p class="page-desc">
          {{ mode === 'iceberg'
            ? '把订单表旧版单版本文本中的分区规格拆分为分区规格清单，保留旧快照读取路径与回滚批次。'
            : '依据Iceberg管理写入的迁移清单版本核对新分区规格，两个业务入口共享同一份清单结果。' }}
        </p>
      </div>
      <span class="reader-badge" :data-testid="`reader-badge-${mode}`">
        读取入口：{{ mode === 'iceberg' ? 'Iceberg管理页' : '湖上分区页' }}
      </span>
    </header>

    <!-- 生成区：仅 Iceberg 管理页可写 -->
    <form v-if="mode === 'iceberg'" class="migration-form" @submit.prevent="onCreate">
      <label class="create-field">
        <span>目标湖表名</span>
        <input v-model="tableName" data-testid="migration-table-name" placeholder="例如 orders" required />
      </label>
      <label class="create-field">
        <span>冲突收口路线</span>
        <select v-model="conflictMode" data-testid="migration-conflict-mode">
          <option value="legacy_first">按旧快照解释（兼容入口指向旧文本）</option>
          <option value="spec_first">按新分区规格解释（兼容入口指向新规格）</option>
        </select>
      </label>
      <label class="check-field">
        <input v-model="simulateInterrupt" data-testid="migration-simulate-interrupt" type="checkbox" />
        <span>模拟工序中断（验证恢复）</span>
      </label>
      <label class="check-field">
        <input v-model="simulateTimeout" data-testid="migration-simulate-timeout" type="checkbox" />
        <span>模拟写入超时（验证可恢复提示）</span>
      </label>
      <button class="btn primary" data-testid="migration-create" type="submit" :disabled="loading">
        {{ loading ? '提交中…' : '生成迁移清单' }}
      </button>
      <button class="btn ghost" data-testid="migration-refresh" type="button" @click="refresh()">刷新清单</button>
    </form>

    <div v-else class="migration-form">
      <button class="btn ghost" data-testid="migration-refresh" type="button" @click="refresh()">刷新清单</button>
      <span class="hint-text">湖上分区为只读核对入口，清单由Iceberg管理跨模块写入。</span>
    </div>

    <div v-if="lastMessage" class="info-text" data-testid="migration-message">{{ lastMessage }}</div>
    <div v-if="errorMessage" class="error-text" data-testid="migration-error">
      {{ errorMessage }}
      <button class="link" type="button" @click="retryLast">重试</button>
    </div>

    <table class="data-table migration-table">
      <thead>
        <tr>
          <th>清单编号</th>
          <th>湖表</th>
          <th>数据版本</th>
          <th>状态</th>
          <th>迁移进度</th>
          <th>冲突收口</th>
          <th>兼容读取入口</th>
          <th>规格校验</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="manifest in manifests" :key="manifest.id" :data-testid="`migration-row-${manifest.id}`" :data-status="manifest.status">
          <td>{{ manifest.manifest_code }}</td>
          <td>{{ manifest.table_name }}</td>
          <td>{{ manifest.data_version }}</td>
          <td>{{ manifest.status }}</td>
          <td>{{ manifest.item_finished }}/{{ manifest.snapshot_total }}</td>
          <td>{{ manifest.conflict_mode_label }}</td>
          <td class="mono-cell">{{ manifest.compat_read_entry }}</td>
          <td>
            <span :data-testid="`verify-flag-${manifest.id}`">{{ manifest.partition_verified ? '已通过' : '未校验' }}</span>
          </td>
          <td class="row-actions">
            <button class="link" data-testid="migration-detail" type="button" @click="openDetail(manifest.id)">明细</button>
            <button
              v-if="mode === 'iceberg' && manifest.status === '已中断'"
              class="link"
              data-testid="migration-resume"
              type="button"
              @click="onResume(manifest)"
            >
              恢复
            </button>
            <button
              v-if="mode === 'iceberg'"
              class="link"
              data-testid="migration-rebase"
              type="button"
              @click="onRebase(manifest)"
            >
              {{ manifest.conflict_mode === 'legacy_first' ? '切换新规格路线' : '切换旧快照路线' }}
            </button>
            <button
              v-if="mode === 'iceberg' && manifest.status !== '已回滚'"
              class="link"
              data-testid="migration-rollback"
              type="button"
              @click="onRollback(manifest)"
            >
              回滚
            </button>
            <button
              v-if="mode === 'partition'"
              class="link"
              data-testid="migration-verify"
              type="button"
              @click="onVerify(manifest.id)"
            >
              校验新规格
            </button>
          </td>
        </tr>
        <tr v-if="!manifests.length">
          <td :colspan="9" class="empty-state" data-testid="migration-empty">
            暂无Iceberg元数据迁移清单{{ mode === 'iceberg' ? '，可先选择湖表生成' : '，请等待Iceberg管理写入后刷新' }}
          </td>
        </tr>
      </tbody>
    </table>

    <!-- 明细弹窗：迁移项双向映射 + 回滚批次 + 快照回放 + 共享视图 -->
    <div v-if="detail" class="modal-backdrop" data-testid="migration-detail-modal">
      <div class="modal migration-modal">
        <div class="modal-head">
          <h3>迁移清单 {{ detail.manifest.manifest_code }} 明细</h3>
          <button class="btn ghost" type="button" @click="detail = null">关闭</button>
        </div>

        <dl class="detail-grid">
          <dt>数据版本</dt><dd data-testid="detail-data-version">{{ detail.manifest.data_version }}</dd>
          <dt>冲突收口</dt><dd>{{ detail.manifest.conflict_mode_label }}</dd>
          <dt>兼容读取入口（唯一）</dt><dd class="mono-cell">{{ detail.manifest.compat_read_entry }}</dd>
          <dt>旧快照读取路径</dt><dd>{{ detail.manifest.legacy_read_retained ? '已保留' : '未保留' }}</dd>
          <dt>回滚批次</dt><dd>{{ detail.rollback_batch?.batch_code ?? '—' }}（{{ detail.rollback_batch?.status ?? '—' }}）</dd>
          <dt>清单状态</dt><dd>{{ detail.manifest.status }}</dd>
        </dl>

        <h4>迁移项：旧元数据文本 ↔ 新分区规格（双向对应）</h4>
        <table class="data-table">
          <thead>
            <tr>
              <th>历史快照</th>
              <th>旧版元数据文本（含排序字段）</th>
              <th>新分区规格</th>
              <th>回程映射（规格→旧文本）</th>
              <th>旧快照读取路径</th>
              <th>工序状态</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in detail.items" :key="item.id">
              <td>{{ item.snapshot_id }}<br /><span class="hint-text">{{ item.metadata_version }}</span></td>
              <td class="mono-cell">{{ item.legacy_metadata_text }}</td>
              <td class="mono-cell">
                {{ item.new_partition_spec.spec_id }}<br />
                {{ item.new_partition_spec.partition_column }} / {{ item.new_partition_spec.granularity }}
              </td>
              <td class="mono-cell">{{ item.reverse_mapping.back_to_legacy_text }}</td>
              <td class="mono-cell path-cell">{{ item.legacy_snapshot_path }}</td>
              <td>
                <span v-for="(stepState, stepName) in item.steps" :key="stepName" class="step-pill" :data-state="stepState">
                  {{ stepName }}:{{ stepState }}
                </span>
                <div v-if="item.last_error" class="error-text">{{ item.last_error }}</div>
              </td>
            </tr>
          </tbody>
        </table>

        <h4>历史快照回放：确认扫描结果与旧版本一致</h4>
        <div class="replay-bar">
          <select v-model="replaySnapshotId" data-testid="replay-snapshot-select">
            <option value="" disabled>选择要回放的快照</option>
            <option v-for="item in detail.items" :key="item.id" :value="item.snapshot_id">{{ item.snapshot_id }}</option>
          </select>
          <button class="btn primary" data-testid="replay-run" type="button" :disabled="!replaySnapshotId" @click="onReplay(detail.manifest.id)">
            回放快照
          </button>
        </div>
        <div v-if="replayResult" class="replay-result" data-testid="replay-result">
          <p :class="replayResult.consistent_with_legacy ? 'info-text' : 'error-text'">
            兼容入口回放扫描与旧版本{{ replayResult.consistent_with_legacy ? '一致' : '不一致' }}；
            两种解释的分区扫描范围{{ replayResult.interpretation_diverges ? '存在冲突' : '相同' }}
          </p>
          <table class="data-table">
            <thead>
              <tr><th>解释来源</th><th>分区列</th><th>粒度</th><th>扫描分区数</th><th>扫描范围说明</th></tr>
            </thead>
            <tbody>
              <tr>
                <td>旧版文本解释</td>
                <td>{{ replayResult.legacy_scan_range.partition_column }}</td>
                <td>{{ replayResult.legacy_scan_range.granularity }}</td>
                <td>{{ replayResult.legacy_scan_range.partition_count }}</td>
                <td>{{ replayResult.legacy_scan_range.scan_scope }}</td>
              </tr>
              <tr>
                <td>新分区规格解释</td>
                <td>{{ replayResult.spec_scan_range.partition_column }}</td>
                <td>{{ replayResult.spec_scan_range.granularity }}</td>
                <td>{{ replayResult.spec_scan_range.partition_count }}</td>
                <td>{{ replayResult.spec_scan_range.scan_scope }}</td>
              </tr>
              <tr>
                <td>兼容入口实际回放</td>
                <td>{{ replayResult.replay_scan_range.partition_column }}</td>
                <td>{{ replayResult.replay_scan_range.granularity }}</td>
                <td>{{ replayResult.replay_scan_range.partition_count }}</td>
                <td>{{ replayResult.replay_scan_range.scan_scope }}</td>
              </tr>
            </tbody>
          </table>
          <p class="hint-text">旧快照读取路径：{{ replayResult.legacy_read_path }}</p>
        </div>

        <h4>回滚批次：迁移回退依据</h4>
        <table class="data-table" v-if="detail.rollback_batch">
          <thead>
            <tr><th>快照</th><th>回滚目标旧文本</th><th>旧快照路径</th><th>状态</th></tr>
          </thead>
          <tbody>
            <tr v-for="entry in detail.rollback_batch.entries" :key="entry.snapshot_id">
              <td>{{ entry.snapshot_id }}</td>
              <td class="mono-cell">{{ entry.restore_to_legacy_text }}</td>
              <td class="mono-cell path-cell">{{ entry.legacy_snapshot_path }}</td>
              <td>{{ entry.status }}</td>
            </tr>
          </tbody>
        </table>

        <h4>跨模块共享核对</h4>
        <div class="replay-bar">
          <button class="btn" data-testid="shared-view-btn" type="button" @click="loadSharedView(detail.manifest.id)">
            读取另一业务入口的共享结果
          </button>
        </div>
        <div v-if="sharedView" class="shared-view" data-testid="shared-view-result">
          <p class="info-text">
            {{ sharedView.reader_module === 'iceberg_table' ? 'Iceberg管理' : '湖上分区' }}入口读取：
            一致性键 {{ sharedView.shared_consistency_key }}，进度 {{ sharedView.item_finished }}/{{ sharedView.item_total }}
          </p>
          <p class="hint-text">兼容读取入口：{{ sharedView.compat_read_entry }} · 规格校验：{{ sharedView.partition_verified ? '已通过' : '未校验' }}</p>
        </div>
      </div>
    </div>

    <!-- 湖上分区页的校验结果 -->
    <div v-if="verifyResult" class="verify-banner" data-testid="verify-result" :data-passed="verifyResult.verified">
      <strong>{{ verifyResult.verified ? '新分区规格校验通过' : '新分区规格校验未通过' }}</strong>
      <span class="hint-text">
        数据版本 {{ verifyResult.data_version }} ·
        通过 {{ verifyResult.checks.filter((c) => c.passed).length }}/{{ verifyResult.checks.length }} 项
      </span>
      <span v-if="verifyResult.recoverable_hint" class="error-text">{{ verifyResult.recoverable_hint }}</span>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  useMetadataMigration,
  type ManifestDetail,
  type ReplayResult,
  type SharedView,
  type VerifyResult,
  type MigrationManifest,
} from '@/composables/useMetadataMigration'

const props = defineProps<{ mode: 'iceberg' | 'partition' }>()

const {
  manifests,
  loading,
  errorMessage,
  lastMessage,
  refresh,
  createManifest,
  resumeManifest,
  rebaseConflict,
  rollbackManifest,
  fetchDetail,
  replaySnapshot,
  verifySpec,
  fetchSharedView,
} = useMetadataMigration()

const tableName = ref('orders')
const conflictMode = ref('legacy_first')
const simulateInterrupt = ref(false)
const simulateTimeout = ref(false)

const detail = ref<ManifestDetail | null>(null)
const replaySnapshotId = ref('')
const replayResult = ref<ReplayResult | null>(null)
const sharedView = ref<SharedView | null>(null)
const verifyResult = ref<VerifyResult | null>(null)

async function onCreate() {
  verifyResult.value = null
  const ok = await createManifest(tableName.value.trim(), {
    conflictMode: conflictMode.value,
    simulateInterrupt: simulateInterrupt.value,
    simulateTimeout: simulateTimeout.value,
  })
  if (ok) {
    simulateInterrupt.value = false
    simulateTimeout.value = false
  }
}

function retryLast() {
  errorMessage.value = ''
  void refresh()
}

async function onResume(manifest: MigrationManifest) {
  await resumeManifest(manifest.id, manifest.table_name)
  if (detail.value?.manifest.id === manifest.id) await openDetail(manifest.id)
}

async function onRebase(manifest: MigrationManifest) {
  const next = manifest.conflict_mode === 'legacy_first' ? 'spec_first' : 'legacy_first'
  await rebaseConflict(manifest.id, next, manifest.table_name)
  if (detail.value?.manifest.id === manifest.id) await openDetail(manifest.id)
}

async function onRollback(manifest: MigrationManifest) {
  await rollbackManifest(manifest.id, manifest.table_name)
  if (detail.value?.manifest.id === manifest.id) await openDetail(manifest.id)
}

async function openDetail(manifestId: number) {
  replayResult.value = null
  replaySnapshotId.value = ''
  sharedView.value = null
  const result = await fetchDetail(manifestId)
  if (result) detail.value = result
}

async function onReplay(manifestId: number) {
  if (!replaySnapshotId.value) return
  replayResult.value = await replaySnapshot(manifestId, replaySnapshotId.value)
}

async function onVerify(manifestId: number) {
  verifyResult.value = await verifySpec(manifestId)
  if (detail.value?.manifest.id === manifestId) await openDetail(manifestId)
}

async function loadSharedView(manifestId: number) {
  // 在当前页读取“另一个”业务入口视角，验证共享结果一致
  const otherReader = props.mode === 'iceberg' ? 'lake_partition' : 'iceberg_table'
  sharedView.value = await fetchSharedView(manifestId, otherReader)
}

onMounted(() => {
  void refresh()
})
</script>

<style scoped>
.migration-panel {
  margin-top: 20px;
  background: #f8fafc;
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 14px 16px;
}
.migration-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 12px;
}
.migration-head h3 {
  margin: 0 0 4px;
  font-size: 15px;
}
.reader-badge {
  font-size: 12px;
  color: var(--brand);
  background: #eff6ff;
  border: 1px solid #bfdbfe;
  border-radius: 999px;
  padding: 4px 10px;
  white-space: nowrap;
}
.migration-form {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  align-items: flex-end;
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px;
  margin: 10px 0;
}
.migration-form .create-field {
  margin-bottom: 0;
  min-width: 200px;
}
.migration-form select,
.migration-form input[type='text'],
.migration-form input:not([type]) {
  width: 100%;
  box-sizing: border-box;
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 8px;
}
.check-field {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: var(--muted);
}
.migration-table {
  margin-top: 8px;
}
.mono-cell {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 12px;
  word-break: break-all;
}
.path-cell {
  max-width: 240px;
}
.hint-text {
  color: var(--muted);
  font-size: 12px;
}
.info-text {
  color: #1d4ed8;
  font-size: 13px;
  margin: 6px 0;
}
.migration-modal {
  width: min(960px, 100%);
  max-height: 88vh;
  overflow: auto;
}
.modal-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.detail-grid {
  display: grid;
  grid-template-columns: 160px 1fr;
  gap: 6px 12px;
  background: #f8fafc;
  border-radius: 8px;
  padding: 10px 12px;
  margin: 10px 0;
}
.detail-grid dt {
  font-size: 12px;
  color: var(--muted);
}
.detail-grid dd {
  margin: 0;
  font-size: 13px;
}
.migration-modal h4 {
  margin: 16px 0 8px;
  font-size: 13px;
}
.step-pill {
  display: inline-block;
  font-size: 11px;
  border-radius: 999px;
  padding: 2px 8px;
  margin: 2px 4px 2px 0;
  background: #f1f5f9;
  color: #475569;
}
.step-pill[data-state='已完成'] {
  background: #dcfce7;
  color: #166534;
}
.replay-bar {
  display: flex;
  gap: 10px;
  align-items: center;
  margin-bottom: 8px;
}
.replay-bar select {
  padding: 6px 10px;
  border: 1px solid var(--border);
  border-radius: 6px;
  min-width: 220px;
}
.replay-result,
.shared-view {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 10px 12px;
  margin-bottom: 10px;
}
.verify-banner {
  margin-top: 10px;
  border-radius: 8px;
  padding: 10px 12px;
  display: flex;
  flex-direction: column;
  gap: 4px;
  background: #dcfce7;
  border: 1px solid #86efac;
  color: #166534;
}
.verify-banner[data-passed='false'] {
  background: #fef3c7;
  border-color: #fcd34d;
  color: #92400e;
}
</style>
