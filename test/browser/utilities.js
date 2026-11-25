// Smoke tests for every other syke/web utility in real browsers. The full
// behaviour is tested in Node (test/<utility>/); these check each one runs
// in a browser's own Web Streams, TextDecoder and timers, from the built bundle.
import * as syke from '/dist/web.mjs'

const enc = new TextEncoder()

/** A ReadableStream over `items`, pulled one at a time (no ReadableStream.from needed). */
function streamOf(items, onPull = () => {}) {
  const iterator = items[Symbol.iterator]()
  return new ReadableStream({
    pull(controller) {
      onPull()
      const { value, done } = iterator.next()
      if (done) controller.close()
      else controller.enqueue(value)
    },
  })
}

async function collect(readable) {
  const out = []
  const reader = readable.getReader()
  for (;;) {
    const { value, done } = await reader.read()
    if (done) return out
    out.push(value)
  }
}

const through = (items, transform) => collect(streamOf(items).pipeThrough(transform))
const rejects = (promise) => promise.then(() => null, (error) => error)

export async function utilityTests({ test, check, equalBytes, concat, randomData }) {
  await test('LengthCheck passes the expected length and fails a short stream', async () => {
    const data = randomData(10_000)
    check(equalBytes(concat(await through([data], new syke.LengthCheck({ expected: 10_000 }))), data), 'data differs')
    const error = await rejects(through([data], new syke.LengthCheck({ expected: 10_001 })))
    check(error?.code === 'ERR_SYKE_LENGTH', `got ${error}`)
  })

  await test('Digest computes CRC-32 and rejects a mismatch', async () => {
    const digest = new syke.Digest({ algorithm: 'crc32' })
    await through([enc.encode('1234'), enc.encode('56789')], digest)
    check((await digest.digest) === 'cbf43926', `got ${await digest.digest}`)
    const error = await rejects(through([enc.encode('x')], new syke.Digest({ algorithm: 'crc32', expected: '00000000' })))
    check(error?.code === 'ERR_SYKE_DIGEST', `got ${error}`)
  })

  await test('Cap truncates and cancels the source', async () => {
    let pulls = 0
    const chunks = Array.from({ length: 1000 }, () => new Uint8Array(100))
    const out = await collect(streamOf(chunks, () => pulls++).pipeThrough(new syke.Cap({ maxBytes: 250, onLimit: 'truncate' })))
    check(concat(out).byteLength === 250, `got ${concat(out).byteLength} bytes`)
    check(pulls < 10, `source pulled ${pulls} times after truncation`)
  })

  await test('Rechunk emits fixed-size chunks', async () => {
    const out = await through([new Uint8Array(7), new Uint8Array(10), new Uint8Array(3)], new syke.Rechunk({ size: 4 }))
    check(out.map((c) => c.byteLength).join() === '4,4,4,4,4', `got ${out.map((c) => c.byteLength)}`)
  })

  await test('Meter counts bytes and reports progress', async () => {
    const reports = []
    const meter = new syke.Meter({ interval: 0, onProgress: (stats) => reports.push(stats.bytes) })
    await through([new Uint8Array(10), new Uint8Array(20)], meter)
    check(meter.bytes === 30 && meter.chunks === 2, `bytes ${meter.bytes}, chunks ${meter.chunks}`)
    check(reports.at(-1) === 30, `reports ${reports}`)
  })

  await test('Watchdog fails a stalled source', async () => {
    const stalled = new ReadableStream({ pull: () => new Promise(() => {}) })
    const error = await rejects(collect(stalled.pipeThrough(new syke.Watchdog({ timeout: 50 }))))
    check(error?.code === 'ERR_SYKE_TIMEOUT', `got ${error}`)
  })

  await test('Throttle holds a stream to its rate', async () => {
    const started = performance.now()
    await through([1, 2, 3, 4, 5, 6], new syke.Throttle({ itemsPerSecond: 20, burst: 1 }))
    const ms = performance.now() - started
    check(ms >= 200, `6 items at 20/s with burst 1 took only ${ms.toFixed(0)} ms`)
  })

  await test('Lines splits text, including a character split across chunks', async () => {
    const bytes = enc.encode('héllo\r\nwörld\nlast')
    const split = bytes.indexOf(0xc3) + 1 // inside the 2-byte 'é'
    const out = await through([bytes.subarray(0, split), bytes.subarray(split)], new syke.Lines())
    check(JSON.stringify(out) === '["héllo","wörld","last"]', `got ${JSON.stringify(out)}`)
  })

  await test('Lines and NdjsonParse decode shared memory', async () => {
    // TextDecoder refuses shared memory in browsers. A SharedArrayBuffer needs a
    // cross-origin isolated page; shared WebAssembly memory may be had without.
    let shared
    let from = 'SharedArrayBuffer'
    if (globalThis.SharedArrayBuffer) shared = new SharedArrayBuffer(64)
    else {
      from = 'shared WebAssembly.Memory'
      try {
        shared = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }).buffer
      } catch (error) {
        return `skipped: no shared memory in this page (${error})`
      }
    }
    const bytes = enc.encode('héllo\n{"a":1}\n')
    new Uint8Array(shared).set(bytes)
    const split = bytes.indexOf(0xc3) + 1 // inside the 2-byte 'é'
    const views = [new Uint8Array(shared, 0, split), new DataView(shared, split, 6 - split)]
    const lines = await through(views, new syke.Lines())
    check(JSON.stringify(lines) === '["héllo"]', `Lines got ${JSON.stringify(lines)}`)
    const records = await through([new Uint8Array(shared, 6, bytes.length - 6)], new syke.NdjsonParse())
    check(JSON.stringify(records) === '[{"a":1}]', `NdjsonParse got ${JSON.stringify(records)}`)
    return `views of a ${from}`
  })

  await test('NdjsonStringify and NdjsonParse round-trip records', async () => {
    const records = [{ id: 1, name: 'a' }, [1, 2], 'text', 42]
    const out = await collect(
      streamOf(records).pipeThrough(new syke.NdjsonStringify()).pipeThrough(new syke.NdjsonParse()),
    )
    check(JSON.stringify(out) === JSON.stringify(records), `got ${JSON.stringify(out)}`)
  })

  await test('Batch groups records', async () => {
    const out = await through([1, 2, 3, 4, 5], new syke.Batch({ size: 2 }))
    check(JSON.stringify(out) === '[[1,2],[3,4],[5]]', `got ${JSON.stringify(out)}`)
  })

  await test('ParallelMap keeps order and bounds concurrency', async () => {
    let active = 0
    let peak = 0
    const out = await through(
      [5, 1, 4, 2, 3],
      new syke.ParallelMap(
        async (n) => {
          peak = Math.max(peak, ++active)
          await new Promise((resolve) => setTimeout(resolve, n * 5))
          active--
          return n * 10
        },
        { concurrency: 2 },
      ),
    )
    check(JSON.stringify(out) === '[50,10,40,20,30]', `got ${JSON.stringify(out)}`)
    check(peak === 2, `peak concurrency ${peak}`)
  })

  await test('fork, peek, concat and merge', async () => {
    const [a, b] = syke.fork(streamOf([1, 2, 3]))
    const [outA, outB] = await Promise.all([collect(a), collect(b)])
    check(JSON.stringify([outA, outB]) === '[[1,2,3],[1,2,3]]', `fork got ${JSON.stringify([outA, outB])}`)

    const { head, stream } = await syke.peek(streamOf([enc.encode('GIF8'), enc.encode('9a...')]), 3)
    check(new TextDecoder().decode(head) === 'GIF', 'peek head differs')
    check(new TextDecoder().decode(concat(await collect(stream))) === 'GIF89a...', 'peek replay differs')

    const joined = await collect(syke.concat([streamOf([1, 2]), () => streamOf([3])]))
    check(JSON.stringify(joined) === '[1,2,3]', `concat got ${JSON.stringify(joined)}`)

    const merged = await collect(syke.merge([streamOf([1, 2]), streamOf([3, 4])]))
    check(JSON.stringify(merged.toSorted()) === '[1,2,3,4]', `merge got ${JSON.stringify(merged)}`)
  })

  await test('fork with a Spool buffer lets a stalled branch fall behind', async () => {
    const data = Array.from({ length: 64 }, () => randomData(64 * 1024))
    const [fast, slow] = syke.fork(streamOf(data), { buffer: () => new syke.Spool() })
    const fastOut = await collect(fast) // completes even though nobody reads `slow` yet
    const slowOut = await collect(slow)
    check(equalBytes(concat(fastOut), concat(data)) && equalBytes(concat(slowOut), concat(data)), 'branch data differs')
  })
}
