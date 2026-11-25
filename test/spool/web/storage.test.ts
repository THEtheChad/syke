import { randomBytes } from 'node:crypto'
import { describe, expect, it, vi } from 'vitest'
import { chunk } from '../../harness.ts'
import { type FakeBrowserOptions, freshPage, installFakeBrowser, useFakeBrowser, waitFor } from './fakeBrowser.ts'

const KiB = 1024
const MiB = 1024 * KiB

useFakeBrowser()

async function readAll<T>(readable: ReadableStream<T>): Promise<T[]> {
  const out: T[] = []
  for await (const value of readable as unknown as AsyncIterable<T>) out.push(value)
  return out
}

/** Spills `data` through a spool with a stalled consumer, then drains it. */
async function spillAndDrain(spool: { readable: ReadableStream<Uint8Array>; writable: WritableStream<Uint8Array> }, data: Uint8Array) {
  const writer = spool.writable.getWriter()
  for (const piece of chunk(data, 64 * KiB)) await writer.write(piece)
  await writer.close()
  return Buffer.concat(await readAll(spool.readable))
}

describe('choosing storage', () => {
  it("'auto' uses OPFS when the page can run the worker", async () => {
    installFakeBrowser()
    const { Spool } = await freshPage()
    const spool = new Spool()
    expect(spool.storage).toBeNull()
    // More than one in-memory batch, so it must reach storage.
    const data = randomBytes(3 * MiB)
    expect((await spillAndDrain(spool, data)).equals(data)).toBe(true)
    expect(spool.storage).toBe('opfs')
  })

  it.each<[string, FakeBrowserOptions]>([
    ['a CSP blocks blob: workers', { workerBlocked: true }],
    ['the worker has no sync access handles', { noSyncHandles: true }],
    ['there is no Worker', { noWorker: true }],
  ])("'auto' falls back to IndexedDB when %s", async (_, options) => {
    const browser = installFakeBrowser(options)
    const { Spool } = await freshPage()
    const data = randomBytes(3 * MiB)
    const spool = new Spool()
    expect((await spillAndDrain(spool, data)).equals(data)).toBe(true)
    expect(spool.storage).toBe('indexeddb')
    await spool.closed
    expect(await browser.idbRecords()).toBe(0)

    // The failure is remembered for the page, so the next spool skips straight to IndexedDB.
    const next = new Spool()
    expect((await spillAndDrain(next, data)).equals(data)).toBe(true)
    expect(next.storage).toBe('indexeddb')
  })

  it("'opfs' fails with SpoolStorageError when OPFS is unavailable", async () => {
    installFakeBrowser({ workerBlocked: true })
    const { Spool, SpoolStorageError } = await freshPage()
    const spool = new Spool({ storage: 'opfs' })
    const error = await spillAndDrain(spool, randomBytes(3 * MiB)).catch((e: unknown) => e)
    expect(error).toBeInstanceOf(SpoolStorageError)
    expect(error).toMatchObject({ code: 'ERR_SYKE_SPOOL_STORAGE', message: expect.stringMatching(/^syke: OPFS/) })
  })

  it("'indexeddb' fails with SpoolStorageError when IndexedDB is unavailable", async () => {
    installFakeBrowser({ noIndexedDb: true })
    const { Spool, SpoolStorageError } = await freshPage()
    const error = await spillAndDrain(new Spool({ storage: 'indexeddb' }), randomBytes(3 * MiB)).catch((e: unknown) => e)
    expect(error).toBeInstanceOf(SpoolStorageError)
  })

  describe('with no storage at all', () => {
    const none: FakeBrowserOptions = { noWorker: true, noIndexedDb: true }

    it("applies backpressure with onLimit: 'backpressure', keeping memory bounded", async () => {
      installFakeBrowser(none)
      const { Spool } = await freshPage()
      const data = randomBytes(4 * MiB)
      const spool = new Spool()
      let accepted = 0
      const writing = (async () => {
        const writer = spool.writable.getWriter()
        for (const piece of chunk(data, 64 * KiB)) {
          await writer.write(piece)
          accepted += piece.byteLength
        }
        await writer.close()
      })()
      await waitFor(() => spool.storage !== null)
      await new Promise((resolve) => setTimeout(resolve, 20))
      expect(spool.storage).toBe('memory')
      // One batch (1 MiB) plus the chunk that passed through.
      expect(accepted).toBeLessThanOrEqual(MiB + 128 * KiB)

      expect(Buffer.concat(await readAll(spool.readable)).equals(data)).toBe(true)
      await writing
    })

    it("fails with SpoolLimitError with onLimit: 'error'", async () => {
      installFakeBrowser(none)
      const { Spool, SpoolLimitError } = await freshPage()
      const error = await spillAndDrain(new Spool({ onLimit: 'error' }), randomBytes(3 * MiB)).catch((e: unknown) => e)
      expect(error).toBeInstanceOf(SpoolLimitError)
      expect((error as Error).message).toMatch(/no browser storage is available.*OPFS.*IndexedDB/)
    })
  })

  it('shares one worker between spools and terminates it when the last one closes', async () => {
    const browser = installFakeBrowser()
    const { Spool } = await freshPage()
    const a = new Spool()
    const b = new Spool()
    const writers = [a.writable.getWriter(), b.writable.getWriter()]
    for (const writer of writers) for (const piece of chunk(randomBytes(MiB), 64 * KiB)) await writer.write(piece)
    await waitFor(() => browser.opfsFiles().length === 2)
    expect(browser.workers.size).toBe(1)

    await writers[0]?.close()
    await readAll(a.readable)
    await a.closed
    expect(browser.workers.size).toBe(1)
    expect(browser.opfsFiles()).toHaveLength(1)

    await b.readable.cancel()
    await b.closed
    expect(browser.workers.size).toBe(0)
    expect(browser.opfsFiles()).toEqual([])
  })
})

// The sweep needs Web Locks, which Node only has from v24 (syke/web skips the
// sweep where it's missing). `pnpm test:browser` covers it in real browsers.
const hasWebLocks = typeof navigator !== 'undefined' && navigator.locks !== undefined

describe.runIf(hasWebLocks)('the sweep of abandoned spools', () => {
  /** Leaves data as a spool in a closed tab would: an OPFS file and IndexedDB records, with its lock. */
  async function leaveOrphan(storageKinds: ('opfs' | 'indexeddb')[]) {
    const { Spool } = await freshPage()
    const spools = storageKinds.map((storage) => new Spool({ storage }))
    for (const spool of spools) {
      const writer = spool.writable.getWriter()
      for (const piece of chunk(randomBytes(MiB), 64 * KiB)) await writer.write(piece)
      writer.releaseLock()
    }
    await waitFor(() => spools.every((spool) => spool.storage !== null))
    return spools
  }

  /** Runs a spool in a "new page", which starts the sweep, and waits for it to finish. */
  async function newPageSpills() {
    const { Spool } = await freshPage()
    const spool = new Spool({ storage: 'indexeddb' })
    const data = randomBytes(256 * KiB)
    expect((await spillAndDrain(spool, data)).equals(data)).toBe(true)
    await spool.closed
    // The sweep runs in the background; give it a moment.
    await new Promise((resolve) => setTimeout(resolve, 50))
  }

  it("leaves a live spool's data alone and removes an abandoned one's", async () => {
    const browser = installFakeBrowser()
    // A "tab" whose lock we can drop, as the browser does when the tab closes.
    let closeTab!: () => void
    const tabClosed = new Promise<void>((resolve) => {
      closeTab = resolve
    })
    const id = 'abandoned-tab'
    await new Promise<void>((held) => {
      void navigator.locks.request(`syke-spool:${id}`, () => {
        held()
        return tabClosed
      })
    })
    await browser.root.getFileHandle(`syke-spool-${id}`, { create: true })
    const [live] = await leaveOrphan(['indexeddb'])
    await writeOrphanRecords(id)
    expect(browser.opfsFiles()).toEqual([`syke-spool-${id}`])
    const recordsBefore = await browser.idbRecords()

    await newPageSpills()
    expect(browser.opfsFiles()).toEqual([`syke-spool-${id}`])
    expect(await browser.idbRecords()).toBe(recordsBefore)

    closeTab()
    await newPageSpills()
    expect(browser.opfsFiles()).toEqual([])
    // Only the live spool's records remain.
    expect(await browser.idbRecords()).toBe(recordsBefore - 3)
    await live?.readable.cancel()
  })

  it('removes the data of a spool whose page went away mid-stream', async () => {
    const browser = installFakeBrowser()
    await leaveOrphan(['opfs', 'indexeddb'])
    expect(browser.opfsFiles()).toHaveLength(1)
    expect(await browser.idbRecords()).toBeGreaterThan(0)
    // The page closes: its worker stops and the browser drops its locks.
    for (const worker of browser.workers) worker.terminate()
    await releaseAllSpoolLocks()

    await newPageSpills()
    expect(browser.opfsFiles()).toEqual([])
    expect(await browser.idbRecords()).toBe(0)
  })
})

describe('the storage quota', () => {
  it('stops growing storage before free space drops below minFreeSpace', async () => {
    // 4 MiB quota, 1 MiB used elsewhere; '25%' leaves 1 MiB free, so about 2 MiB to spill into.
    const browser = installFakeBrowser({ quota: 4 * MiB, usage: MiB })
    const { Spool, SpoolLimitError } = await freshPage()
    const spool = new Spool({ minFreeSpace: '25%', onLimit: 'error' })
    const error = await spillAndDrain(spool, randomBytes(8 * MiB)).catch((e: unknown) => e)
    expect(error).toBeInstanceOf(SpoolLimitError)
    expect((error as Error).message).toMatch(/minFreeSpace/)
    expect(browser.root.peakFileSize).toBeLessThanOrEqual(2 * MiB)
    expect(browser.estimates).toBeGreaterThan(0)
  })

  it('pauses at the floor and resumes as the consumer frees space', async () => {
    const browser = installFakeBrowser({ quota: 4 * MiB, usage: MiB })
    const { Spool } = await freshPage()
    const data = randomBytes(8 * MiB)
    const spool = new Spool({ minFreeSpace: '25%' })
    expect((await spillAndDrainConcurrently(spool, data)).equals(data)).toBe(true)
    expect(browser.root.peakFileSize).toBeLessThanOrEqual(2 * MiB)
  })

  it('treats QuotaExceededError as a limit, without losing data', async () => {
    // No floor, so only the browser's refusal stops the spool.
    const browser = installFakeBrowser({ quota: 2 * MiB })
    const { Spool } = await freshPage()
    const data = randomBytes(6 * MiB)
    const spool = new Spool({ minFreeSpace: false, chunkSize: 32 * KiB })
    expect((await spillAndDrainConcurrently(spool, data)).equals(data)).toBe(true)
    expect(browser.root.peakFileSize).toBeLessThanOrEqual(2 * MiB)
  })

  it("fails with SpoolLimitError on QuotaExceededError with onLimit: 'error'", async () => {
    installFakeBrowser({ quota: 2 * MiB })
    const { Spool, SpoolLimitError } = await freshPage()
    const spool = new Spool({ minFreeSpace: false, onLimit: 'error' })
    const error = await spillAndDrain(spool, randomBytes(6 * MiB)).catch((e: unknown) => e)
    expect(error).toBeInstanceOf(SpoolLimitError)
    expect((error as Error).message).toMatch(/quota/)
  })
})

/** Whether `check` comes true within 100 event-loop turns, without timers. */
async function soon(check: () => boolean): Promise<boolean> {
  for (let i = 0; i < 100 && !check(); i++) await new Promise((resolve) => setImmediate(resolve))
  return check()
}

/** Writes `data` while a slow consumer reads it, so limits are hit and released. */
async function spillAndDrainConcurrently(
  spool: { readable: ReadableStream<Uint8Array>; writable: WritableStream<Uint8Array> },
  data: Uint8Array,
) {
  const writing = (async () => {
    const writer = spool.writable.getWriter()
    for (const piece of chunk(data, 64 * KiB)) await writer.write(piece)
    await writer.close()
  })()
  const out: Uint8Array[] = []
  for await (const piece of spool.readable as unknown as AsyncIterable<Uint8Array>) {
    out.push(piece)
    if (out.length % 8 === 0) await new Promise((resolve) => setTimeout(resolve, 1))
  }
  await writing
  return Buffer.concat(out)
}

async function writeOrphanRecords(id: string) {
  const db = await new Promise<IDBDatabase>((resolve, reject) => {
    const req = indexedDB.open('syke-spool', 1)
    req.onsuccess = () => resolve(req.result)
    req.onerror = () => reject(req.error)
  })
  await new Promise<void>((resolve, reject) => {
    const tx = db.transaction('chunks', 'readwrite')
    for (let seq = 0; seq < 3; seq++) tx.objectStore('chunks').put(new Uint8Array(1024), [id, seq])
    tx.oncomplete = () => resolve()
    tx.onerror = () => reject(tx.error)
  })
  db.close()
}

/**
 * Node has no tabs to close, so stand in for the browser dropping a closed
 * page's locks: steal each spool lock, which releases the holder's.
 */
async function releaseAllSpoolLocks() {
  const { held = [] } = await navigator.locks.query()
  for (const lock of held) {
    if (!lock.name?.startsWith('syke-spool:')) continue
    await navigator.locks.request(lock.name, { steal: true }, () => {})
  }
}

describe('storage failures', () => {
  it('keeps every byte when an IndexedDB write fails with QuotaExceededError (the transaction aborts)', async () => {
    const browser = installFakeBrowser()
    const { Spool } = await freshPage()
    const data = randomBytes(4 * MiB)
    const spool = new Spool({ storage: 'indexeddb', minFreeSpace: false })
    // The request's error event reaches the transaction while tx.error is still null.
    browser.idbPutFailures = 1
    expect((await spillAndDrainConcurrently(spool, data)).equals(data)).toBe(true)
    expect(browser.idbPutFailures).toBe(0)
    await spool.closed
    expect(await browser.idbRecords()).toBe(0)
  })

  it('hands a read that waited on a write refused for quota the batch at once, not at the next quota poll', async () => {
    const browser = installFakeBrowser()
    const { Spool } = await freshPage()
    const data = randomBytes(1 * MiB + 64 * KiB)
    const spool = new Spool({ storage: 'indexeddb', minFreeSpace: false })
    const reader = spool.readable.getReader()
    const writer = spool.writable.getWriter()
    // The quota poll never fires: only the spool itself can resume the read.
    // (fake-indexeddb runs on setImmediate.)
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
    try {
      const out: Uint8Array[] = []
      let waiting: Promise<void> | undefined
      browser.idbPutFailures = 1
      // Read while the write is in flight: the stream asks the spool for more,
      // and that pull waits for the write to land (it's older than the batch).
      browser.onIdbPut = () => {
        browser.onIdbPut = null
        waiting = reader.read().then(({ value }) => void out.push(value as Uint8Array))
      }
      // The first chunk goes straight to the stream's queue, the next 1 MiB fills the batch.
      for (const piece of chunk(data, 64 * KiB)) await writer.write(piece)
      expect(await soon(() => waiting !== undefined)).toBe(true)
      await waiting
      let settled = false
      const next = reader.read().then(({ value }) => {
        out.push(value as Uint8Array)
        settled = true
      })
      expect(await soon(() => settled)).toBe(true)
      await next
      vi.useRealTimers()
      const rest = (async () => {
        for (let r = await reader.read(); !r.done; r = await reader.read()) out.push(r.value)
      })()
      await writer.close()
      await rest
      expect(Buffer.concat(out).equals(data)).toBe(true)
    } finally {
      vi.useRealTimers()
    }
    await spool.closed
  })

  it("fails with SpoolLimitError on an IndexedDB QuotaExceededError with onLimit: 'error'", async () => {
    const browser = installFakeBrowser()
    const { Spool, SpoolLimitError } = await freshPage()
    const spool = new Spool({ storage: 'indexeddb', onLimit: 'error' })
    browser.idbPutFailures = 1
    const error = await spillAndDrain(spool, randomBytes(3 * MiB)).catch((e: unknown) => e)
    expect(error).toBeInstanceOf(SpoolLimitError)
    expect((error as Error).cause).toMatchObject({ name: 'QuotaExceededError' })
  })

  it('fails the stream with the real error (never null) when an IndexedDB write fails otherwise', async () => {
    const browser = installFakeBrowser()
    const { Spool } = await freshPage()
    const spool = new Spool({ storage: 'indexeddb' })
    browser.idbPutFailures = 1
    browser.idbPutError = 'UnknownError'
    const error = await spillAndDrain(spool, randomBytes(3 * MiB)).catch((e: unknown) => e)
    expect(error).toMatchObject({ name: 'UnknownError' })
    await spool.closed
    expect(await browser.idbRecords()).toBe(0)
  })

  it('reports a failed IndexedDB read as a stream error, with no unhandled rejection', async () => {
    const browser = installFakeBrowser()
    const { Spool } = await freshPage()
    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown) => unhandled.push(reason)
    process.on('unhandledRejection', onUnhandled)
    try {
      const spool = new Spool({ storage: 'indexeddb' })
      const writer = spool.writable.getWriter()
      for (const piece of chunk(randomBytes(2 * MiB), 64 * KiB)) await writer.write(piece)
      await writer.close()
      await waitFor(async () => (await browser.idbRecords()) >= 1)
      // Make the read's getAll() fail inside the transaction.
      const { IDBObjectStore } = await import('fake-indexeddb')
      const getAll = IDBObjectStore.prototype.getAll
      IDBObjectStore.prototype.getAll = function (this: IDBObjectStore & { _rawObjectStore: Record<string, unknown> }, ...args: unknown[]) {
        IDBObjectStore.prototype.getAll = getAll
        const raw = this._rawObjectStore
        const original = raw.getAllValues
        raw.getAllValues = () => {
          throw new DOMException('disk gone', 'UnknownError')
        }
        try {
          return (getAll as (...a: unknown[]) => IDBRequest).apply(this, args)
        } finally {
          raw.getAllValues = original
        }
      } as never
      const error = await readAll(spool.readable).catch((e: unknown) => e)
      IDBObjectStore.prototype.getAll = getAll
      expect(error).toMatchObject({ name: 'UnknownError' })
      await new Promise((resolve) => setTimeout(resolve, 20))
      expect(unhandled).toEqual([])
    } finally {
      process.off('unhandledRejection', onUnhandled)
    }
  })

  it('fails with "ended mid-record" instead of hanging when stored records come back short', async () => {
    const browser = installFakeBrowser()
    const { Spool } = await freshPage()
    const spool = new Spool<string>({ storage: 'indexeddb', objectMode: true })
    const writer = spool.writable.getWriter()
    await writer.write('passes straight through')
    // One record of exactly 3 MiB framed (4-byte header, 2 quotes): three 1 MiB IndexedDB records.
    void writer.write('x'.repeat(3 * MiB - 6))
    void writer.close()
    await waitFor(async () => (await browser.idbRecords()) === 3)
    await truncateLastRecord()
    const reader = spool.readable.getReader()
    expect((await reader.read()).value).toBe('passes straight through')
    const error = await reader.read().catch((e: unknown) => e)
    expect((error as Error).message).toBe('syke: spool ended mid-record')
  })

  it("'auto' still works where Web Locks throw SecurityError (an opaque origin)", async () => {
    installFakeBrowser({ locksSecurityError: true })
    const { Spool } = await freshPage()
    const data = randomBytes(3 * MiB)
    const spool = new Spool()
    expect((await spillAndDrain(spool, data)).equals(data)).toBe(true)
    await spool.closed
  })

  it("'auto' uses no storage, rather than crash, when the lock fails some other way", async () => {
    installFakeBrowser()
    Object.defineProperty(navigator, 'locks', {
      configurable: true,
      value: { request: () => Promise.reject(new DOMException('nope', 'InvalidStateError')) },
    })
    const { Spool, SpoolStorageError } = await freshPage()
    const data = randomBytes(256 * KiB)
    const spool = new Spool()
    expect((await spillAndDrain(spool, data)).equals(data)).toBe(true)
    expect(spool.storage).toBe('memory')
    const error = await spillAndDrain(new Spool({ storage: 'opfs' }), data).catch((e: unknown) => e)
    expect(error).toBeInstanceOf(SpoolStorageError)
  })

  it('remembers an OPFS SecurityError for the page: one worker, then straight to IndexedDB', async () => {
    const browser = installFakeBrowser({ opfsSecurityError: true })
    const { Spool } = await freshPage()
    const data = randomBytes(2 * MiB)
    for (let i = 0; i < 3; i++) {
      const spool = new Spool()
      expect((await spillAndDrain(spool, data)).equals(data)).toBe(true)
      expect(spool.storage).toBe('indexeddb')
      await spool.closed
    }
    expect(browser.workersStarted).toBe(1)
    expect(browser.workers.size).toBe(0)
  })

  it("does not lose a SpoolStorageError when a small stream drains before 'opfs' fails to open", async () => {
    installFakeBrowser({ workerBlocked: true })
    const { Spool, SpoolStorageError } = await freshPage()
    const spool = new Spool({ storage: 'opfs' })
    const writer = spool.writable.getWriter()
    // Fills the readable's queue; the next chunk goes to the batch and storage starts opening.
    await writer.write(new Uint8Array([1]))
    await writer.write(new Uint8Array([2]))
    await writer.close()
    const reader = spool.readable.getReader()
    expect((await reader.read()).value).toEqual(new Uint8Array([1]))
    expect((await reader.read()).value).toEqual(new Uint8Array([2]))
    const error = await reader.read().catch((e: unknown) => e)
    expect(error).toBeInstanceOf(SpoolStorageError)
  })
})

describe('OPFS file lifecycle', () => {
  it('truncates the file once everything is read, and keeps data intact after', async () => {
    const browser = installFakeBrowser()
    const { Spool } = await freshPage()
    const spool = new Spool({ storage: 'opfs', chunkSize: 64 * KiB })
    const writer = spool.writable.getWriter()
    const reader = spool.readable.getReader()
    const first = randomBytes(2 * MiB)
    const firstOut: Uint8Array[] = []
    for (const piece of chunk(first, 64 * KiB)) await writer.write(piece)
    await waitFor(() => (browser.root.files.values().next().value?.size ?? 0) >= MiB)
    while (Buffer.concat(firstOut).length < first.length) firstOut.push((await reader.read()).value as Uint8Array)
    expect(Buffer.concat(firstOut).equals(first)).toBe(true)
    await waitFor(() => browser.truncates > 0)
    expect([...browser.root.files.values()][0]?.size).toBe(0)

    // The file is reused from the start.
    const second = randomBytes(2 * MiB)
    const writing = (async () => {
      for (const piece of chunk(second, 64 * KiB)) await writer.write(piece)
      await writer.close()
    })()
    const secondOut: Uint8Array[] = []
    for (;;) {
      const { value, done } = await reader.read()
      if (done) break
      secondOut.push(value)
    }
    await writing
    expect(Buffer.concat(secondOut).equals(second)).toBe(true)
    await spool.closed
    expect(browser.opfsFiles()).toEqual([])
  })
})

describe('OPFS truncation', () => {
  it('does not truncate away a write in flight when the reader empties the file', async () => {
    const browser = installFakeBrowser()
    const { Spool } = await freshPage()
    const spool = new Spool({ storage: 'opfs', chunkSize: 64 * KiB })
    const writer = spool.writable.getWriter()
    const reader = spool.readable.getReader()
    const [a, b, c] = [randomBytes(64 * KiB), randomBytes(64 * KiB), randomBytes(64 * KiB)]
    const macrotask = () => new Promise((resolve) => setTimeout(resolve))
    browser.holdReplies.add('write')

    // `a` goes straight to the waiting consumer; `b` is stored.
    await writer.write(a)
    await writer.write(b)
    await waitFor(() => browser.heldReplies === 1)
    browser.releaseReplies()
    await macrotask()
    // `c` is written by the worker, but the page hasn't heard back yet.
    await writer.write(c)
    await waitFor(() => browser.heldReplies === 1)

    // Reading `b` empties the file as far as the page knows, with `c` in flight.
    expect(Buffer.from((await reader.read()).value as Uint8Array).equals(a)).toBe(true)
    expect(Buffer.from((await reader.read()).value as Uint8Array).equals(b)).toBe(true)
    await macrotask()
    expect(browser.truncates).toBe(0)

    browser.holdReplies.clear()
    browser.releaseReplies()
    await writer.close()
    const rest: Uint8Array[] = []
    for (;;) {
      const { value, done } = await reader.read()
      if (done) break
      rest.push(value)
    }
    expect(Buffer.concat(rest).equals(c)).toBe(true)
    await spool.closed
  })
})

describe('waiting at a limit', () => {
  it('does not poll at maxDiskBytes: only the consumer reading can make room', async () => {
    installFakeBrowser()
    const { Spool } = await freshPage()
    const data = randomBytes(4 * MiB)
    const spool = new Spool({ storage: 'opfs', maxDiskBytes: MiB })
    const writer = spool.writable.getWriter()
    let accepted = 0
    const writing = (async () => {
      for (const piece of chunk(data, 64 * KiB)) {
        await writer.write(piece)
        accepted += piece.byteLength
      }
      await writer.close()
    })()
    await waitFor(() => accepted >= MiB)
    await new Promise((resolve) => setTimeout(resolve, 50))
    const setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout')
    await new Promise((resolve) => setTimeout(resolve, 600))
    const polls = setTimeoutSpy.mock.calls.filter(([, ms]) => ms === 250).length
    setTimeoutSpy.mockRestore()
    expect(polls).toBe(0)
    expect(Buffer.concat(await readAll(spool.readable)).equals(data)).toBe(true)
    await writing
  })
})

async function truncateLastRecord() {
  const db = await new Promise<IDBDatabase>((resolve, reject) => {
    const req = indexedDB.open('syke-spool', 1)
    req.onsuccess = () => resolve(req.result)
    req.onerror = () => reject(req.error)
  })
  await new Promise<void>((resolve, reject) => {
    const tx = db.transaction('chunks', 'readwrite')
    const store = tx.objectStore('chunks')
    const req = store.openCursor(null, 'prev')
    req.onsuccess = () => {
      const cursor = req.result
      if (cursor) cursor.update((cursor.value as Uint8Array).subarray(0, 100).slice())
    }
    tx.oncomplete = () => resolve()
    tx.onabort = () => reject(tx.error)
  })
  db.close()
}
