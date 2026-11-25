import { forceCloseDatabase } from 'fake-indexeddb'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { installFakeBrowser, useFakeBrowser, waitFor } from './fakeBrowser.ts'

const MiB = 1024 * 1024

useFakeBrowser()

afterEach(() => {
  vi.useRealTimers()
})

describe('the quota snapshot', () => {
  /** A fresh quota module, with `estimate()` answered by hand. */
  async function quotaWithManualEstimate() {
    vi.useFakeTimers({ toFake: ['Date'] })
    vi.setSystemTime(0)
    const answers: ((value: StorageEstimate) => void)[] = []
    Object.defineProperty(navigator, 'storage', {
      configurable: true,
      value: { estimate: () => new Promise<StorageEstimate>((resolve) => answers.push(resolve)) },
    })
    vi.resetModules()
    const quota = await import('../../../src/spool/web/quota.ts')
    /** Resolves the oldest pending estimate. */
    const answer = async (estimate: StorageEstimate) => {
      await vi.waitFor(() => expect(answers.length).toBeGreaterThan(0))
      answers.shift()?.(estimate)
    }
    return { ...quota, answer }
  }

  it('reports no room without a floor right after a QuotaExceededError, until the estimate would expire', async () => {
    const quota = await quotaWithManualEstimate()
    expect(await quota.storageBudget(null)).toBe(Number.POSITIVE_INFINITY)
    quota.storageFull()
    expect(await quota.storageBudget(null)).toBe(0)
    vi.setSystemTime(999)
    expect(await quota.storageBudget(null)).toBe(0)
    vi.setSystemTime(1000)
    expect(await quota.storageBudget(null)).toBe(Number.POSITIVE_INFINITY)
  })

  it('restarts the snapshot clock when storage fills, so a nearly expired estimate does not end the pause at once', async () => {
    const quota = await quotaWithManualEstimate()
    const floor = { bytes: 0 } as const
    const first = quota.storageBudget(floor)
    await quota.answer({ quota: 10 * MiB, usage: 0 })
    expect(await first).toBe(10 * MiB)
    vi.setSystemTime(900)
    quota.storageFull()
    // Past the original estimate's 1 s, but within 1 s of the refusal: no new estimate.
    vi.setSystemTime(1500)
    expect(await quota.storageBudget(floor)).toBe(0)
  })

  it('keeps growth and refusals reported while an estimate is being read', async () => {
    const quota = await quotaWithManualEstimate()
    const floor = { bytes: 0 } as const
    const budget = quota.storageBudget(floor)
    quota.storageGrew(MiB)
    await quota.answer({ quota: 10 * MiB, usage: 0 })
    // The estimate may predate the growth: it still counts.
    expect(await budget).toBe(9 * MiB)

    vi.setSystemTime(2000)
    const next = quota.storageBudget(floor)
    quota.storageFull()
    await quota.answer({ quota: 10 * MiB, usage: 0 })
    expect(await next).toBe(0)
  })

  it('gives back freed growth (but never more than was grown since the estimate)', async () => {
    const quota = await quotaWithManualEstimate()
    const floor = { bytes: 0 } as const
    const first = quota.storageBudget(floor)
    await quota.answer({ quota: 10 * MiB, usage: 0 })
    await first
    quota.storageGrew(3 * MiB)
    expect(await quota.storageBudget(floor)).toBe(7 * MiB)
    quota.storageFreed(2 * MiB)
    expect(await quota.storageBudget(floor)).toBe(9 * MiB)
    quota.storageFreed(5 * MiB)
    expect(await quota.storageBudget(floor)).toBe(10 * MiB)
  })
})

describe('the spool database connection', () => {
  async function freshIdb() {
    vi.resetModules()
    return import('../../../src/spool/web/indexedDb.ts')
  }

  /** Whether `db` still accepts transactions. */
  function isOpen(db: IDBDatabase): boolean {
    try {
      db.transaction('chunks')
      return true
    } catch {
      return false
    }
  }

  it('opens a new connection after the browser closes one (onclose)', async () => {
    installFakeBrowser()
    const idb = await freshIdb()
    const first = await idb.openDb()
    forceCloseDatabase(first as never)
    const second = await idb.openDb()
    expect(second).not.toBe(first)
    expect(isOpen(second)).toBe(true)
    // Releasing the dead connection leaves the new one alone.
    idb.closeDb(first)
    expect(isOpen(second)).toBe(true)
    idb.closeDb(second)
    expect(isOpen(second)).toBe(false)
  })

  it('counts users per connection across a versionchange', async () => {
    installFakeBrowser()
    const idb = await freshIdb()
    const a = await idb.openDb()
    const b = await idb.openDb()
    expect(b).toBe(a)
    // Another tab wants a newer schema: this connection closes and is replaced.
    a.onversionchange?.(new Event('versionchange') as IDBVersionChangeEvent)
    const c = await idb.openDb()
    expect(c).not.toBe(a)
    // c's only user releases it: it closes, though a's two users haven't released theirs.
    idb.closeDb(c)
    expect(isOpen(c)).toBe(false)
    idb.closeDb(a)
    idb.closeDb(b)
    const d = await idb.openDb()
    expect(isOpen(d)).toBe(true)
    idb.closeDb(d)
  })

  it('closes a connection that opens after the open was given up on as blocked', async () => {
    const db = { close: vi.fn() }
    const req: Record<string, unknown> = {}
    vi.stubGlobal('indexedDB', { open: () => req })
    const idb = await freshIdb()
    const opening = idb.openDb()
    ;(req.onblocked as () => void)()
    await expect(opening).rejects.toThrow(/blocked by another tab/)
    req.result = db
    ;(req.onsuccess as () => void)()
    expect(db.close).toHaveBeenCalledOnce()
    vi.unstubAllGlobals()
  })
})

describe('OPFS truncation and the quota budget', () => {
  const floor = { bytes: 0 } as const
  const macrotask = () => new Promise((resolve) => setTimeout(resolve))

  /** A fresh page's quota module and an OPFS store holding 1 MiB, counted as growth. */
  async function storeWithOneMiB() {
    const browser = installFakeBrowser({ quota: 10 * MiB })
    vi.useFakeTimers({ toFake: ['Date'] })
    vi.setSystemTime(0)
    vi.resetModules()
    const quota = await import('../../../src/spool/web/quota.ts')
    const { OpfsStore } = await import('../../../src/spool/web/opfs.ts')
    expect(await quota.storageBudget(floor)).toBe(10 * MiB)
    const store = await OpfsStore.open('accounting', 64 * 1024, Number.POSITIVE_INFINITY, () => {})
    expect(store.room(MiB, MiB, false)).toBe(MiB)
    quota.storageGrew(await store.write(new Uint8Array(MiB)))
    expect(await quota.storageBudget(floor)).toBe(9 * MiB)
    return { browser, quota, store }
  }

  it('gives the space back only once the truncate is done', async () => {
    const { browser, quota, store } = await storeWithOneMiB()
    browser.holdReplies.add('truncate')
    while (!store.empty) await store.read()
    // The worker has truncated, but the page hasn't heard yet.
    await waitFor(() => browser.heldReplies === 1)
    expect(await quota.storageBudget(floor)).toBe(9 * MiB)
    browser.releaseReplies()
    await macrotask()
    expect(await quota.storageBudget(floor)).toBe(10 * MiB)
    await store.close()
  })

  it('does not give the space back when the truncate fails', async () => {
    const { browser, quota, store } = await storeWithOneMiB()
    browser.truncateError = 'InvalidStateError'
    while (!store.empty) await store.read()
    await waitFor(() => browser.truncates === 1)
    await macrotask()
    await macrotask()
    expect(await quota.storageBudget(floor)).toBe(9 * MiB)
    await store.close()
  })
})

describe('IndexedDB reads and the quota budget', () => {
  it('leaves deleted records counted until the next estimate (browsers reclaim them lazily)', async () => {
    installFakeBrowser({ quota: 10 * MiB })
    vi.useFakeTimers({ toFake: ['Date'] })
    vi.setSystemTime(0)
    vi.resetModules()
    const quota = await import('../../../src/spool/web/quota.ts')
    const { IdbStore } = await import('../../../src/spool/web/indexedDb.ts')
    const floor = { bytes: 0 } as const
    expect(await quota.storageBudget(floor)).toBe(10 * MiB)
    const store = await IdbStore.open('lazy', 64 * 1024, Number.POSITIVE_INFINITY, () => {})
    expect(store.room(MiB, MiB, false)).toBe(MiB)
    quota.storageGrew(await store.write(new Uint8Array(MiB)))
    expect(await quota.storageBudget(floor)).toBe(9 * MiB)
    while (!store.empty) await store.read()
    expect(await quota.storageBudget(floor)).toBe(9 * MiB)
    // A fresh estimate (which here never counted IndexedDB) replaces the guess.
    vi.setSystemTime(1000)
    expect(await quota.storageBudget(floor)).toBe(10 * MiB)
    await store.close()
  })
})
