import { Readable, type Transform } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { ReadableStream as NodeReadableStream } from 'node:stream/web'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Throttle as NodeThrottle } from '../../src/node.ts'
import { Throttle as WebThrottle } from '../../src/web.ts'
import { pick, runtimes, type Runtime } from '../harness.ts'

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] })
})
afterEach(() => {
  vi.useRealTimers()
})

const tick = () => new Promise((resolve) => setImmediate(resolve))

/** Runs `input` through `stream`, stepping the fake clock from timer to timer; returns when each chunk came out. */
async function timedRun(
  runtime: Runtime,
  stream: unknown,
  input: Iterable<unknown> | AsyncIterable<unknown>,
  output: { at: number; chunk: unknown }[] = [],
) {
  const collect = async (source: AsyncIterable<unknown>) => {
    for await (const chunk of source) output.push({ at: performance.now(), chunk })
  }
  const done =
    runtime === 'node'
      ? pipeline(Readable.from(input, { objectMode: true }), stream as Transform, collect)
      : collect(NodeReadableStream.from(input).pipeThrough(stream as never))
  let finished = false
  let error: unknown
  done.then(
    () => (finished = true),
    (e) => {
      finished = true
      error = e
    },
  )
  while (!finished) {
    await tick()
    await vi.advanceTimersToNextTimerAsync()
  }
  if (error) throw error
  return output
}

const bytes = (n: number) => new Uint8Array(n)

/** Total bytes out by each moment something came out (a Node byte stream may join chunks). */
function totals(output: { at: number; chunk: unknown }[]): [number, number][] {
  const byTime = new Map<number, number>()
  let total = 0
  for (const { at, chunk } of output) byTime.set(at, (total += (chunk as Uint8Array).byteLength))
  return [...byTime]
}

describe.each(runtimes)('Throttle (%s)', (runtime) => {
  const Throttle = pick(runtime, { node: NodeThrottle, web: WebThrottle })

  it('lets a burst through, then holds the average rate', async () => {
    const output = await timedRun(runtime, new Throttle({ bytesPerSecond: 1000 }), Array.from({ length: 30 }, () => bytes(100)))
    expect(totals(output)).toEqual([[0, 1000], ...Array.from({ length: 20 }, (_, i) => [(i + 1) * 100, 1100 + i * 100])])
  })

  it('passes a chunk bigger than the bucket whole, and makes later chunks wait off the debt', async () => {
    const output = await timedRun(runtime, new Throttle({ bytesPerSecond: 1000, burst: 100 }), [bytes(500), bytes(100), bytes(100)])
    expect(totals(output)).toEqual([
      [0, 500],
      [500, 600],
      [600, 700],
    ])
  })

  it('refills the bucket during a pause, up to burst', async () => {
    async function* paused() {
      yield bytes(100)
      yield bytes(100)
      // Wait long enough to refill far more than the bucket holds.
      await new Promise((resolve) => setTimeout(resolve, 10_000))
      yield bytes(100)
      yield bytes(100)
      yield bytes(100)
    }
    const output = await timedRun(runtime, new Throttle({ bytesPerSecond: 1000, burst: 200 }), paused())
    expect(totals(output)).toEqual([
      [0, 200],
      [10_000, 400],
      [10_100, 500],
    ])
  })

  it('throttles items', async () => {
    const records = Array.from({ length: 5 }, (_, n) => ({ n }))
    const output = await timedRun(runtime, new Throttle<{ n: number }>({ itemsPerSecond: 10, burst: 1 }), records)
    expect(output.map(({ chunk }) => chunk)).toEqual(records)
    expect(output.map(({ at }) => at)).toEqual([0, 100, 200, 300, 400])
  })

  it('holds back the source instead of buffering', async () => {
    const output: { at: number; chunk: unknown }[] = []
    let maxAhead = 0
    function* source() {
      for (let n = 0; n < 200; n++) {
        maxAhead = Math.max(maxAhead, n - output.length)
        yield { n }
      }
    }
    await timedRun(runtime, new Throttle({ itemsPerSecond: 100, burst: 1 }), source(), output)
    expect(output).toHaveLength(200)
    expect(output.at(-1)!.at).toBe(1990)
    // Only stream buffers (16 records by default in Node) sit between source and throttle.
    expect(maxAhead).toBeLessThan(40)
  })

  it('passes an empty stream without starting a timer', async () => {
    expect(await timedRun(runtime, new Throttle({ bytesPerSecond: 1 }), [])).toEqual([])
    expect(vi.getTimerCount()).toBe(0)
  })

  it('propagates a source error', async () => {
    async function* failing() {
      yield bytes(100)
      yield bytes(100)
      throw new Error('boom')
    }
    await expect(timedRun(runtime, new Throttle({ bytesPerSecond: 100, burst: 100 }), failing())).rejects.toThrow('boom')
    expect(vi.getTimerCount()).toBe(0)
  })

  it('waits out a delay longer than setTimeout can take', async () => {
    // 3000 bytes at 1 byte per 1000 s: a 3e9 ms wait, past setTimeout's 2^31 - 1 ms.
    const throttle = new Throttle({ bytesPerSecond: 0.001, burst: 3000 })
    const output = await timedRun(runtime, throttle, [bytes(3000), bytes(3000)])
    expect(totals(output)).toEqual([
      [0, 3000],
      [3_000_000_000, 6000],
    ])
  })

  it('rejects invalid options', () => {
    // @ts-expect-error neither given
    expect(() => new Throttle({})).toThrow(TypeError)
    // @ts-expect-error both given
    expect(() => new Throttle({ bytesPerSecond: 1, itemsPerSecond: 1 })).toThrow(TypeError)
    expect(() => new Throttle({ bytesPerSecond: 0 })).toThrow(RangeError)
    expect(() => new Throttle({ itemsPerSecond: Number.POSITIVE_INFINITY })).toThrow(RangeError)
    expect(() => new Throttle({ bytesPerSecond: 1, burst: -1 })).toThrow(RangeError)
  })
})

describe('Throttle (node)', () => {
  it('clears a pending delay when destroyed', async () => {
    const throttle = new NodeThrottle({ bytesPerSecond: 100, burst: 100 })
    throttle.on('data', () => {})
    throttle.write(bytes(100))
    throttle.write(bytes(100))
    await tick()
    expect(vi.getTimerCount()).toBe(1)
    throttle.destroy()
    await tick()
    expect(vi.getTimerCount()).toBe(0)
    expect(throttle.destroyed).toBe(true)
  })
})

describe('Throttle (web)', () => {
  it('fails a byte throttle with a TypeError on a non-byte chunk rather than letting it through free', async () => {
    await expect(timedRun('web', new WebThrottle({ bytesPerSecond: 100 }), [bytes(1), 'abc'])).rejects.toThrow(
      /^syke: Throttle with bytesPerSecond takes byte chunks \(an ArrayBuffer or a view of one\), got string$/,
    )
  })

  it('charges ArrayBuffer, DataView and other typed-array chunks by their bytes', async () => {
    const input = [new ArrayBuffer(100), new DataView(new ArrayBuffer(100)), new Uint16Array(50), new Float64Array(25)]
    const throttle = new WebThrottle<ArrayBufferView | ArrayBuffer>({ bytesPerSecond: 100, burst: 100 })
    const output = await timedRun('web', throttle, input)
    expect(output).toEqual(input.map((chunk, i) => ({ at: (i * 1000) as number, chunk })))
    output.forEach(({ chunk }, i) => expect(chunk).toBe(input[i]))
  })

  it('takes chunks of any type with itemsPerSecond', async () => {
    const output = await timedRun('web', new WebThrottle<unknown>({ itemsPerSecond: 1, burst: 1 }), ['a', { b: 1 }])
    expect(output).toEqual([
      { at: 0, chunk: 'a' },
      { at: 1000, chunk: { b: 1 } },
    ])
  })

  it('clears a pending delay and cancels the source when the consumer cancels', async () => {
    let cancelled = false
    const source = new NodeReadableStream<Uint8Array>({
      pull: (controller) => controller.enqueue(bytes(100)),
      cancel: () => {
        cancelled = true
      },
    })
    const reader = source.pipeThrough(new WebThrottle({ bytesPerSecond: 100, burst: 100 }) as never).getReader()
    await reader.read()
    // A pending read pulls the next chunk in, which has to wait.
    const pending = reader.read()
    await tick()
    expect(vi.getTimerCount()).toBe(1)
    await reader.cancel()
    expect(await pending).toEqual({ done: true, value: undefined })
    await tick()
    expect(vi.getTimerCount()).toBe(0)
    expect(cancelled).toBe(true)
  })
})
