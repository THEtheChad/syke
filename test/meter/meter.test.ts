import { randomBytes } from 'node:crypto'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Meter as NodeMeter, type MeterStats } from '../../src/node.ts'
import { Meter as WebMeter } from '../../src/web.ts'
import { chunk, pick, run, runBytes, runtimes } from '../harness.ts'

// Only the clocks: stream internals keep their real setImmediate/nextTick.
beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date', 'performance'] })
})
afterEach(() => {
  vi.useRealTimers()
})

/** Yields `chunks`, advancing the fake clock by `stepMs` before each one after the first. */
async function* timed<T>(chunks: T[], stepMs: number) {
  for (const [i, piece] of chunks.entries()) {
    if (i > 0) vi.advanceTimersByTime(stepMs)
    yield piece
  }
}

describe.each(runtimes)('Meter (%s)', (runtime) => {
  const Meter = pick(runtime, { node: NodeMeter, web: WebMeter })
  const data = randomBytes(10_000)

  it('passes data through unchanged and counts it', async () => {
    const meter = new Meter()
    expect((await runBytes(meter, timed(chunk(data, 1000), 100))).equals(data)).toBe(true)
    expect(meter.stats).toEqual({
      bytes: 10_000,
      chunks: 10,
      elapsedMs: 900,
      bytesPerSecond: 10_000 / 0.9,
      chunksPerSecond: 10 / 0.9,
    })
    // The clock stops when the stream ends.
    vi.advanceTimersByTime(5000)
    expect(meter.elapsedMs).toBe(900)
  })

  it('measures elapsed time from the first chunk', async () => {
    const meter = new Meter()
    vi.advanceTimersByTime(10_000)
    expect(meter.elapsedMs).toBe(0)
    expect(meter.bytesPerSecond).toBe(0)
    await run(meter, timed(chunk(data, 5000), 250))
    expect(meter.elapsedMs).toBe(250)
    expect(meter.bytesPerSecond).toBe(40_000)
  })

  it('reports progress at most every interval, and once at the end', async () => {
    const reports: MeterStats[] = []
    const meter = new Meter({ interval: 300, onProgress: (stats) => reports.push(stats) })
    await run(meter, timed(chunk(data, 1000), 100))
    // Chunks at 0..900 ms: reports at 0, 300, 600, 900, then the end.
    expect(reports.map((stats) => stats.elapsedMs)).toEqual([0, 300, 600, 900, 900])
    expect(reports.map((stats) => stats.chunks)).toEqual([1, 4, 7, 10, 10])
    expect(reports.at(-1)).toEqual(meter.stats)
  })

  it('reports every chunk with interval 0', async () => {
    const onProgress = vi.fn()
    await run(new Meter({ interval: 0, onProgress }), chunk(data, 1000))
    expect(onProgress).toHaveBeenCalledTimes(11)
  })

  it('reports once for an empty stream', async () => {
    const onProgress = vi.fn()
    const meter = new Meter({ onProgress })
    expect(await run(meter, [])).toEqual([])
    expect(onProgress).toHaveBeenCalledExactlyOnceWith({
      bytes: 0,
      chunks: 0,
      elapsedMs: 0,
      bytesPerSecond: 0,
      chunksPerSecond: 0,
    })
  })

  it('counts records, and only the bytes in byte chunks and strings', async () => {
    const meter = new Meter<unknown>({ objectMode: true })
    const records = [{ a: 1 }, 'héllo €𝄞', Uint8Array.of(1, 2, 3), 42, []]
    expect(await run(meter, records)).toEqual(records)
    expect(meter.chunks).toBe(5)
    expect(meter.bytes).toBe(Buffer.byteLength('héllo €𝄞') + 3)
  })

  it('fails the stream if onProgress throws', async () => {
    const meter = new Meter({
      onProgress: () => {
        throw new Error('boom')
      },
    })
    await expect(run(meter, [data])).rejects.toThrow('boom')
  })

  it('stops the clock without a final report when the source fails', async () => {
    const onProgress = vi.fn()
    const meter = new Meter({ interval: 10_000, onProgress })
    async function* failing() {
      yield data
      vi.advanceTimersByTime(100)
      throw new Error('boom')
    }
    await expect(run(meter, failing())).rejects.toThrow('boom')
    expect(onProgress).toHaveBeenCalledTimes(1)
    const elapsed = meter.elapsedMs
    vi.advanceTimersByTime(1000)
    expect(meter.elapsedMs).toBe(elapsed)
  })

  it('rejects invalid options', () => {
    expect(() => new Meter({ interval: -1 })).toThrow(RangeError)
    expect(() => new Meter({ interval: Number.NaN })).toThrow(RangeError)
    // @ts-expect-error not a function
    expect(() => new Meter({ onProgress: 'log' })).toThrow(TypeError)
  })
})

describe('Meter (node)', () => {
  it('counts strings as bytes in byte mode', async () => {
    const meter = new NodeMeter()
    await run(meter, ['héllo' as never])
    expect(meter.bytes).toBe(6)
  })
})
