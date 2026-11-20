import { expect, test } from '@playwright/test'

/**
 * Iceberg 元数据迁移清单端到端验收：
 * 1. Iceberg 管理页生成清单（模拟中断）→ 恢复只处理未完成项；
 * 2. 明细里旧文本↔新规格双向对应、旧快照读取路径、回滚批次齐全；
 * 3. 回放历史快照，扫描结果与旧版本一致；
 * 4. 冲突收口切换路线形成新数据版本；
 * 5. 湖上分区页读取同一清单并校验新分区规格，两入口共享结果一致；
 * 6. 空态与超时都有可恢复提示。
 */

test.describe('Iceberg元数据迁移清单', () => {
  test.beforeEach(async ({ page }) => {
    const errors: string[] = []
    page.on('pageerror', (e) => errors.push(e.message))
    page.on('response', (r) => {
      if (r.url().includes('/api/metadata_migration') && r.status() >= 500) {
        errors.push(`${r.status()} ${r.url()}`)
      }
    })
    test.info().attach('page-errors', { body: errors.join('\n') || 'none' })
  })

  test('Iceberg管理页：生成、中断恢复、明细、回放、冲突收口全流程', async ({ page }) => {
    test.setTimeout(120_000)
    await page.goto('/iceberg_table')

    // 生成区与空态/列表至少渲染一个
    const panel = page.getByTestId('migration-panel-iceberg')
    await expect(panel).toBeVisible()
    await expect(page.getByTestId('migration-table-name')).toHaveValue('orders')

    // 干净后端下首次进入保持空态
    await expect(page.getByTestId('migration-empty')).toBeVisible()

    // 1. 模拟中断生成
    await page.getByTestId('migration-simulate-interrupt').check()
    await page.getByTestId('migration-create').click()
    await expect(page.getByTestId('migration-message')).toContainText('未完成项')

    const interruptedRow = page.locator('[data-testid^="migration-row-"]').first()
    await expect(interruptedRow).toHaveAttribute('data-status', '已中断')

    // 2. 恢复：只处理未完成项
    await interruptedRow.getByTestId('migration-resume').click()
    await expect(page.getByTestId('migration-message')).toContainText('3/3')
    await expect(interruptedRow).toHaveAttribute('data-status', '已完成')

    // 3. 打开明细，核对双向映射、旧快照路径与回滚批次
    await interruptedRow.getByTestId('migration-detail').click()
    const modal = page.getByTestId('migration-detail-modal')
    await expect(modal).toBeVisible()
    await expect(page.getByTestId('detail-data-version')).toContainText('dv-orders-')
    // 迁移项表格包含旧文本（分区+排序压在一起）与新规格
    await expect(modal.getByText('partition:dt/month;sort:id,created_at').first()).toBeVisible()
    await expect(modal.getByText('spec-orders-01').first()).toBeVisible()
    // 旧快照读取路径
    await expect(modal.getByText(/s3:\/\/lake\/orders\/metadata\/legacy\//).first()).toBeVisible()
    // 回滚批次
    await expect(modal.getByText('RB-MIGM-')).toBeVisible()

    // 4. 回放历史快照：与旧版本一致，且能看到两种解释的扫描范围冲突
    await page.getByTestId('replay-snapshot-select').selectOption({ index: 1 })
    await page.getByTestId('replay-run').click()
    const replay = page.getByTestId('replay-result')
    await expect(replay).toBeVisible()
    await expect(replay.locator('.info-text')).toContainText('一致')
    // 旧解释 month=1 分区，新规格 day=30 分区，冲突被显式展示
    await expect(replay).toContainText('month')
    await expect(replay).toContainText('day')

    // 5. 跨模块共享核对：以湖上分区入口读同一份清单
    await page.getByTestId('shared-view-btn').click()
    const shared = page.getByTestId('shared-view-result')
    await expect(shared).toBeVisible()
    await expect(shared).toContainText('湖上分区入口读取')
    await expect(shared).toContainText('MIGM-')

    // 记录当前数据版本，用于冲突收口后比对
    const versionBefore = await page.getByTestId('detail-data-version').innerText()
    await page.keyboard.press('Escape').catch(() => undefined)
    await modal.locator('button', { hasText: '关闭' }).click()

    // 6. 冲突收口切换到新规格路线 → 新数据版本
    await interruptedRow.getByTestId('migration-rebase').click()
    await expect(page.getByTestId('migration-message')).toContainText('数据版本升级')
    await interruptedRow.getByTestId('migration-detail').click()
    const versionAfter = await page.getByTestId('detail-data-version').innerText()
    expect(versionAfter).not.toBe(versionBefore)
    await expect(page.getByTestId('migration-detail-modal')).toContainText('spec://orders/partition-specs')
  })

  test('湖上分区页：只读核对同一清单并校验新分区规格', async ({ page }) => {
    test.setTimeout(120_000)
    // 先在 Iceberg 页准备一份已完成清单（若上个用例已生成则直接复用）
    await page.goto('/iceberg_table')
    const panel = page.getByTestId('migration-panel-iceberg')
    await expect(panel).toBeVisible()
    const rowCount = await page.locator('[data-testid^="migration-row-"]').count()
    if (rowCount === 0) {
      await page.getByTestId('migration-create').click()
      await expect(page.locator('[data-testid^="migration-row-"]').first()).toBeVisible()
    }

    // 湖上分区页读取同一份清单
    await page.goto('/lake_partition')
    const partitionPanel = page.getByTestId('migration-panel-partition')
    await expect(partitionPanel).toBeVisible()
    await expect(page.getByTestId('reader-badge-partition')).toContainText('湖上分区页')
    const firstRow = page.locator('[data-testid^="migration-row-"]').first()
    await expect(firstRow).toBeVisible()

    // 只读入口没有生成/恢复/回滚按钮
    await expect(page.getByTestId('migration-create')).toHaveCount(0)
    await expect(page.getByTestId('migration-resume')).toHaveCount(0)

    // 校验新分区规格
    await firstRow.getByTestId('migration-verify').click()
    const banner = page.getByTestId('verify-result')
    await expect(banner).toBeVisible()
    await expect(banner).toContainText('新分区规格校验通过')
    await expect(firstRow.getByTestId(/verify-flag-/)).toContainText('已通过')

    // 明细中以 Iceberg 入口视角共享读取，一致性键相同
    await firstRow.getByTestId('migration-detail').click()
    await page.getByTestId('shared-view-btn').click()
    await expect(page.getByTestId('shared-view-result')).toContainText('Iceberg管理入口读取')
  })

  test('空态与超时：给出可恢复提示', async ({ page }) => {
    test.setTimeout(120_000)
    await page.goto('/iceberg_table')
    const panel = page.getByTestId('migration-panel-iceberg')

    // 超时模拟：不写入清单，提示可恢复
    await page.getByTestId('migration-simulate-timeout').check()
    await page.getByTestId('migration-create').click()
    const errorBox = page.getByTestId('migration-error')
    await expect(errorBox).toBeVisible()
    await expect(errorBox).toContainText('超时')
    await expect(errorBox).toContainText('恢复')
  })
})
