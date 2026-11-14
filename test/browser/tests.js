// Browser tests for syke/web's Spool, run in real browsers by
// scripts/test-browser.mjs (`pnpm test:browser`). Also runnable by hand:
// `pnpm test:browser --serve`, then open the printed URL.
//
// Imports the built bundle (dist/web.mjs), as an app would.
import { Spool, SpoolLimitError } from '/dist/web.mjs'
import { frame, idbRows, opfsSpoolFiles, randomData, readAll, sleep, spillStalled, stallMonitor, waitFor } from './util.js'

const KiB = 1024
const MiB = 1024 * KiB
const results = { browser: new URLSearchParams(location.search).get('browser') ?? 'manual', env: {}, tests: [] }
const stalls = stallMonitor()

async function test(name, fn, timeoutMs = 120_000) {
  const started = performance.now()
  stalls.reset()
  let entry
  try {
    const detail = await Promise.race([
      fn(),
      sleep(timeoutMs).then(() => {
        throw new Error(`timed out after ${timeoutMs} ms`)
      }),
    ])
    entry = { name, ok: true, detail: detail ?? '' }
  } catch (error) {
    entry = { name, ok: false, detail: String(error?.stack ?? error) }
  }
  entry.ms = Math.round(performance.now() - started)
  entry.longestStallMs = Math.round(stalls.reset())
  results.tests.push(entry)
  render(entry)
}

function check(condition, message) {
  if (!condition) throw new Error(message)
}

function equalBytes(a, b) {
  if (a.byteLength !== b.byteLength) return false
  for (let i = 0; i < a.byteLength; i++) if (a[i] !== b[i]) return false
  return true
}

function concat(pieces) {
  const out = new Uint8Array(pieces.reduce((n, piece) => n + piece.byteLength, 0))
  let offset = 0
  for (const piece of pieces) {
    out.set(piece, offset)
    offset += piece.byteLength
  }
  return out
}

/** No spool file or record is left behind. */
async function expectClean(what) {
  const files = await opfsSpoolFiles()
  const rows = await idbRows()
  check(files.length === 0 && rows === 0, `${what}: ${files.length} OPFS file(s) and ${rows} IndexedDB record(s) left`)
}

const mbps = (bytes, ms) => `${((bytes / MiB) / (ms / 1000)).toFixed(0)} MB/s`

async function environment() {
  const estimate = await navigator.storage.estimate()
  results.env = {
    userAgent: navigator.userAgent,
    secureContext: isSecureContext,
    quotaGB: +(estimate.quota / 1024 ** 3).toFixed(1),
    usageMB: +(estimate.usage / MiB).toFixed(1),
    webLocks: !!navigator.locks,
  }
}

// --- Integrity ---------------------------------------------------------------

for (const storage of ['auto', 'opfs', 'indexeddb']) {
  await test(`bytes round-trip, consumer stalled (${storage})`, async () => {
    const data = randomData(32 * MiB)
    const spool = new Spool({ storage, chunkSize: 64 * KiB })
    const writeStart = performance.now()
    // Every write must resolve though nothing is reading: no backpressure.
    await spillStalled(spool.writable, data, [1, 4096, 65536, 100_003, 300_000])
    const writeMs = performance.now() - writeStart
    const expected = storage === 'auto' ? 'opfs' : storage
    check(spool.storage === expected, `storage is ${spool.storage}, expected ${expected}`)
    const readStart = performance.now()
    const out = await readAll(spool.readable)
    const readMs = performance.now() - readStart
    check(equalBytes(concat(out), data), 'data differs')
    check(Math.max(...out.slice(1).map((piece) => piece.byteLength)) <= 300_000, 'oversized chunk')
    await spool.closed
    await expectClean('after end')
    return `storage=${spool.storage}; 32 MiB; write ${mbps(data.byteLength, writeMs)}, drain ${mbps(data.byteLength, readMs)}`
  })
}

for (const storage of ['opfs', 'indexeddb']) {
  await test(`records round-trip in object mode (${storage})`, async () => {
    const records = Array.from({ length: 20_000 }, (_, i) => ({ i, s: 'x'.repeat(i % 200), a: [i, -i, i / 3], o: { even: i % 2 === 0 } }))
    const spool = new Spool({ storage, objectMode: true, chunkSize: 16 * KiB })
    const writer = spool.writable.getWriter()
    for (const record of records) await writer.write(record)
    await writer.close()
    check(spool.storage === storage, `storage is ${spool.storage}`)
    const out = await readAll(spool.readable)
    check(out.length === records.length, `${out.length} records out`)
    check(out[0] === records[0], 'the first record did not pass straight through')
    for (let i = 0; i < records.length; i++) {
      check(JSON.stringify(out[i]) === JSON.stringify(records[i]), `record ${i} differs`)
    }
    await spool.closed
    await expectClean('after end')
    return `storage=${spool.storage}; ${records.length} records`
  })

  await test(`reads interleaved with writes keep order (${storage})`, async () => {
    const data = randomData(16 * MiB)
    const spool = new Spool({ storage, chunkSize: 50_000 })
    const writer = spool.writable.getWriter()
    const reader = spool.readable.getReader()
    const out = []
    let offset = 0
    let step = 0
    while (offset < data.byteLength) {
      const size = 1 + ((step++ * 7919) % 200_000)
      await writer.write(data.subarray(offset, offset + size))
      offset += size
      if (step % 3 === 0) {
        const { value } = await reader.read()
        if (value) out.push(value)
      }
    }
    await writer.close()
    for (;;) {
      const { value, done } = await reader.read()
      if (done) break
      out.push(value)
    }
    check(equalBytes(concat(out), data), 'data differs')
    await spool.closed
    await expectClean('after end')
    return `storage=${spool.storage}; ${step} writes`
  })

  // --- Cleanup -----------------------------------------------------------------

  await test(`storage is deleted when the source errors (${storage})`, async () => {
    const spool = new Spool({ storage })
    const writer = spool.writable.getWriter()
    await spillStalled(writer, randomData(8 * MiB), [65536], { close: false })
    await waitFor(async () => (await opfsSpoolFiles()).length + (await idbRows()) > 0)
    const before = `${(await opfsSpoolFiles()).length} file(s), ${await idbRows()} record(s)`
    await writer.abort(new Error('socket reset'))
    const error = await spool.readable.getReader().read().catch((e) => e)
    check(error?.message === 'socket reset', `readable failed with ${error}`)
    await spool.closed
    await expectClean('after abort')
    return `before abort: ${before}; after: none`
  })

  await test(`storage is deleted when the consumer cancels (${storage})`, async () => {
    const spool = new Spool({ storage })
    const writer = spool.writable.getWriter()
    await spillStalled(writer, randomData(8 * MiB), [65536], { close: false })
    await waitFor(async () => (await opfsSpoolFiles()).length + (await idbRows()) > 0)
    const reason = new Error('gave up')
    await spool.readable.cancel(reason)
    const error = await writer.write(new Uint8Array(1)).catch((e) => e)
    check(error === reason, `writable failed with ${error}`)
    await spool.closed
    await expectClean('after cancel')
  })

  // --- Limits ------------------------------------------------------------------

  await test(`maxDiskBytes pauses the source, then resumes (${storage})`, async () => {
    const data = randomData(16 * MiB)
    const spool = new Spool({ storage, maxDiskBytes: 2 * MiB })
    let accepted = 0
    const writing = (async () => {
      const writer = spool.writable.getWriter()
      for (let offset = 0; offset < data.byteLength; offset += 64 * KiB) {
        await writer.write(data.subarray(offset, offset + 64 * KiB))
        accepted += 64 * KiB
      }
      await writer.close()
    })()
    await waitFor(() => accepted >= 2 * MiB)
    await sleep(300)
    const paused = accepted
    // The cap plus one in-memory batch (1 MiB) and the chunk that passed through.
    check(paused < 4 * MiB, `accepted ${paused} bytes past a 2 MiB cap`)
    const out = await readAll(spool.readable)
    await writing
    check(equalBytes(concat(out), data), 'data differs')
    await spool.closed
    await expectClean('after end')
    return `source paused after ${(paused / MiB).toFixed(1)} MiB of 16 MiB, then finished`
  })

  await test(`onLimit: 'error' fails with SpoolLimitError (${storage})`, async () => {
    const spool = new Spool({ storage, maxDiskBytes: MiB, onLimit: 'error' })
    const error = await spillStalled(spool.writable, randomData(8 * MiB), [65536]).catch((e) => e)
    check(error instanceof SpoolLimitError && error.code === 'ERR_SYKE_SPOOL_LIMIT', `failed with ${error}`)
    await spool.closed
    await expectClean('after the limit')
    return error.message
  })
}

// --- Main thread -------------------------------------------------------------

await test('no main-thread stalls while spilling and draining 64 MiB (auto)', async () => {
  const data = randomData(64 * MiB)
  const spool = new Spool()
  stalls.reset()
  // A source that delivers in tasks, like a network stream.
  const writer = spool.writable.getWriter()
  for (let offset = 0; offset < data.byteLength; offset += 64 * KiB) {
    await writer.write(data.subarray(offset, offset + 64 * KiB))
    if ((offset / (64 * KiB)) % 4 === 3) await sleep(0)
  }
  await writer.close()
  const reader = spool.readable.getReader()
  let received = 0
  for (let n = 0; ; n++) {
    const { value, done } = await reader.read()
    if (done) break
    received += value.byteLength
    if (n % 4 === 3) await sleep(0)
  }
  const longest = stalls.reset()
  check(received === data.byteLength, `received ${received} bytes`)
  check(longest < 100, `the main thread stalled for ${longest.toFixed(0)} ms`)
  await spool.closed
  return `storage=${spool.storage}; longest stall ${longest.toFixed(0)} ms`
})

// --- Fallback ----------------------------------------------------------------

await test("a CSP that blocks blob: workers makes 'auto' fall back to IndexedDB", async () => {
  const { message, remove } = frame('/csp.html?role=csp')
  const report = await message
  remove()
  check(report.ok, report.detail)
  return report.detail
})

// --- Sweep -------------------------------------------------------------------

await test('the sweep leaves live spools alone and removes abandoned ones', async () => {
  await expectClean('before')
  // A "tab" that spills to both backends and is then closed mid-stream.
  const orphan = frame('/frame.html?role=orphan')
  const ready = await orphan.message
  check(ready.ok, ready.detail)
  const files = (await opfsSpoolFiles()).length
  const rows = await idbRows()
  check(files === 1 && rows > 0, `orphan left ${files} file(s), ${rows} record(s)`)

  // A new page sweeps while the orphan's tab is still open: nothing is removed.
  const first = frame('/frame.html?role=fresh')
  check((await first.message).ok, 'fresh page failed')
  first.remove()
  check((await opfsSpoolFiles()).length === 1 && (await idbRows()) === rows, 'the sweep removed a live spool')

  // Close the tab, then a new page's sweep removes what it left.
  orphan.remove()
  await waitFor(async () => {
    const { held = [] } = await navigator.locks.query()
    return !held.some((lock) => lock.name.startsWith('syke-spool:'))
  }, 10_000)
  const second = frame('/frame.html?role=fresh')
  check((await second.message).ok, 'fresh page failed')
  await waitFor(async () => (await opfsSpoolFiles()).length === 0 && (await idbRows()) === 0, 10_000)
  second.remove()
  return `orphan left 1 file and ${rows} records; both removed by the next page's sweep`
})

// --- Bundling ----------------------------------------------------------------

await test('a minified bundle works (inline worker survives minification)', async () => {
  const min = await import('/min/web.mjs')
  const data = randomData(8 * MiB)
  const spool = new min.Spool({ storage: 'opfs' })
  await spillStalled(spool.writable, data, [65536])
  check(spool.storage === 'opfs', `storage is ${spool.storage}`)
  check(equalBytes(concat(await readAll(spool.readable)), data), 'data differs')
  await spool.closed
  await expectClean('after end')
})

await environment()
results.ok = results.tests.every((entry) => entry.ok)
results.longestStallMs = Math.max(...results.tests.map((entry) => entry.longestStallMs))
document.title = results.ok ? 'PASS' : 'FAIL'
if (new URLSearchParams(location.search).has('auto')) {
  await fetch('/results', { method: 'POST', body: JSON.stringify(results) })
}

function render(entry) {
  const row = document.createElement('tr')
  for (const text of [entry.ok ? 'pass' : 'FAIL', entry.name, `${entry.ms} ms`, `${entry.longestStallMs} ms`, entry.detail]) {
    const cell = document.createElement('td')
    cell.textContent = text
    row.append(cell)
  }
  row.className = entry.ok ? 'ok' : 'bad'
  document.querySelector('tbody').append(row)
}
