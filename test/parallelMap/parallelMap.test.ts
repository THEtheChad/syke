import { describe, expect, it } from 'vitest'
import { ParallelMap as NodeParallelMap } from '../../src/node.ts'
import { ParallelMap as WebParallelMap } from '../../src/web.ts'
import { ParallelMapCore } from '../../src/parallelMap/core.ts'
import { pick, run, runSettled, runtimes } from '../harness.ts'
import { drive } from '../streamDriver.ts'

const range = (n: number) => Array.from({ length: n }, (_, i) => i)

/** Waits `turns` turns of the event loop: a deterministic stand-in for latency. */
async function turns(count: number) {
  for (let i = 0; i < count; i++) await new Promise((resolve) => setImmediate(resolve))
}

function deferred<T = void>() {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

/** Wraps `fn`, counting calls and the peak number in flight. */
function tracked<I, O>(fn: (item: I, context: { signal: AbortSignal }) => Promise<O>) {
  const stats = { calls: 0, inFlight: 0, peak: 0, signals: [] as AbortSignal[] }
  const wrapped = async (item: I, context: { signal: AbortSignal }) => {
    stats.calls++
    stats.signals.push(context.signal)
    stats.inFlight++
    stats.peak = Math.max(stats.peak, stats.inFlight)
    try {
      return await fn(item, context)
    } finally {
      stats.inFlight--
    }
  }
  return { fn: wrapped, stats }
}

/** A source that counts how many items have been pulled from it. */
function counted(total: number) {
  const source = {
    pulled: 0,
    async *[Symbol.asyncIterator]() {
      for (let i = 0; i < total; i++) {
        source.pulled++
        yield i
      }
    },
  }
  return source
}

// Pseudo-random but repeatable latencies, so results finish out of order.
const latency = (i: number) => (i * 7919) % 5

describe.each(runtimes)('ParallelMap (%s)', (runtime) => {
  const ParallelMap = pick(runtime, { node: NodeParallelMap, web: WebParallelMap })

  it('maps in input order by default, with at most concurrency calls in flight', async () => {
    const { fn, stats } = tracked(async (i: number) => {
      await turns(latency(i))
      return i * 2
    })
    expect(await run(new ParallelMap(fn, { concurrency: 4 }), range(50))).toEqual(range(50).map((i) => i * 2))
    expect(stats.peak).toBe(4)
    expect(stats.calls).toBe(50)
  })

  it('emits results as they finish with ordered: false', async () => {
    const delays = [6, 1, 3, 0]
    const { fn, stats } = tracked(async (i: number) => {
      await turns(delays[i] as number)
      return `item ${i}`
    })
    const output = await run(new ParallelMap(fn, { concurrency: 4, ordered: false }), range(4))
    expect(output).toEqual(['item 3', 'item 1', 'item 2', 'item 0'])
    expect(stats.peak).toBe(4)
  })

  it('holds concurrency in unordered mode too', async () => {
    const { fn, stats } = tracked(async (i: number) => {
      await turns(latency(i))
      return i
    })
    const output = await run<number>(new ParallelMap(fn, { concurrency: 3, ordered: false }), range(40))
    expect([...output].sort((a, b) => a - b)).toEqual(range(40))
    expect(stats.peak).toBe(3)
  })

  it('takes a synchronous fn', async () => {
    expect(await run(new ParallelMap((i: number) => i + 1, { concurrency: 2 }), range(5))).toEqual([1, 2, 3, 4, 5])
  })

  it('drops null and undefined results, so it can filter', async () => {
    const fn = async (i: number) => (i % 3 === 0 ? null : i % 3 === 1 ? undefined : i)
    expect(await run(new ParallelMap(fn, { concurrency: 3 }), range(9))).toEqual([2, 5, 8])
    expect(await run(new ParallelMap(fn, { concurrency: 3, ordered: false }), range(9))).toEqual(
      expect.arrayContaining([2, 5, 8]),
    )
  })

  it('emits nothing for empty input', async () => {
    expect(await run(new ParallelMap(async (i) => i, { concurrency: 2 }), [])).toEqual([])
  })

  it('waits for work in flight before ending', async () => {
    const gates = range(3).map(() => deferred())
    const map = new ParallelMap(async (i: number) => (await gates[i]?.promise, i), { concurrency: 4 })
    const stream = drive<number>(runtime, map)
    for (const i of range(3)) await stream.write(i)
    const ended = stream.end()
    let finished = false
    stream.done.then(() => {
      finished = true
    })
    await turns(5)
    expect(finished).toBe(false)
    gates[2]?.resolve()
    gates[0]?.resolve()
    await turns(5)
    expect(stream.output).toEqual([0])
    expect(finished).toBe(false)
    gates[1]?.resolve()
    await ended
    expect(await stream.done).toBeUndefined()
    expect(stream.output).toEqual([0, 1, 2])
  })

  it('stops pulling input while concurrency calls are in flight', async () => {
    const gate = deferred()
    const { fn, stats } = tracked(async (i: number) => (await gate.promise, i))
    const source = counted(100)
    const result = run(new ParallelMap(fn, { concurrency: 3, highWaterMark: 1 }), source)
    await turns(20)
    expect(stats.calls).toBe(3)
    expect(stats.inFlight).toBe(3)
    // The source is only read ahead by the pipes' own small buffers.
    expect(source.pulled).toBeLessThan(10)
    gate.resolve()
    expect(await result).toEqual(range(100))
    expect(stats.peak).toBe(3)
  })

  it('bounds results held back behind a slow head item when ordered', async () => {
    const head = deferred()
    const { fn, stats } = tracked(async (i: number) => {
      if (i === 0) await head.promise
      return i
    })
    const source = counted(100)
    const result = run(new ParallelMap(fn, { concurrency: 4, highWaterMark: 1 }), source)
    await turns(20)
    // Items 1-3 finished but wait for item 0, and keep their slots: nothing new starts.
    expect(stats.calls).toBe(4)
    expect(stats.inFlight).toBe(1)
    expect(source.pulled).toBeLessThan(10)
    head.resolve()
    expect(await result).toEqual(range(100))
  })

  it('stops calling fn while nobody reads the output', async () => {
    const { fn, stats } = tracked(async (i: number) => i)
    const stream = drive<number>(runtime, new ParallelMap(fn, { concurrency: 2, highWaterMark: 1 }), { reading: false })
    for (const i of range(50)) stream.write(i).catch(() => {})
    await turns(20)
    // A concurrency's worth plus the output buffer, not all 50.
    expect(stats.calls).toBeLessThanOrEqual(4)
    stream.read()
    stream.end().catch(() => {})
    await stream.done
    expect(stream.output).toEqual(range(50))
  })

  it('fails with the error from fn and aborts the calls in flight', async () => {
    const boom = new Error('boom')
    const aborted: unknown[] = []
    const { fn, stats } = tracked(async (i: number, { signal }) => {
      if (i === 2) {
        await turns(2)
        throw boom
      }
      // Never finishes on its own; rejects when aborted, as fetch would.
      await new Promise((_, reject) =>
        signal.addEventListener('abort', () => {
          aborted.push(signal.reason)
          reject(signal.reason)
        }),
      )
      return i
    })
    const { output, error } = await runSettled(new ParallelMap(fn, { concurrency: 4 }), range(20))
    expect(error).toBe(boom)
    expect(output).toEqual([])
    expect(stats.calls).toBe(4)
    expect(aborted).toEqual([boom, boom, boom])
    expect(stats.signals.every((signal) => signal.aborted)).toBe(true)
    await turns(5)
    expect(stats.calls).toBe(4)
  })

  it('fails when fn throws synchronously', async () => {
    const fn = (i: number) => {
      if (i === 3) throw new Error('sync boom')
      return i
    }
    const { output, error } = await runSettled(new ParallelMap(fn, { concurrency: 2 }), range(10))
    expect(error).toMatchObject({ message: 'sync boom' })
    expect(range(3)).toEqual(expect.arrayContaining(output))
  })

  it('fails while idle, waiting on the source', async () => {
    const fn = async () => {
      await turns(2)
      throw new Error('late boom')
    }
    const stream = drive(runtime, new ParallelMap(fn, { concurrency: 2 }))
    await stream.write(1)
    // No more input is coming yet; the failure must still reach the consumer.
    expect(await stream.done).toMatchObject({ message: 'late boom' })
  })

  it('aborts calls in flight when the stream is destroyed or cancelled', async () => {
    const { fn, stats } = tracked(
      (_: number, { signal }) =>
        new Promise((_resolve, reject) => signal.addEventListener('abort', () => reject(signal.reason))),
    )
    const stream = drive(runtime, new ParallelMap(fn, { concurrency: 3 }))
    stream.write(1).catch(() => {})
    stream.write(2).catch(() => {})
    await turns(5)
    expect(stats.inFlight).toBe(2)
    stream.abort(new Error('stop'))
    await stream.done
    await turns(2)
    expect(stats.signals.map((signal) => signal.aborted)).toEqual([true, true])
    expect(stats.inFlight).toBe(0)
  })

  it('aborts calls in flight when the consumer stops early', async () => {
    const { fn, stats } = tracked(async (i: number, { signal }) => {
      if (i < 2) return i
      await new Promise((_resolve, reject) => signal.addEventListener('abort', () => reject(signal.reason)))
      return i
    })
    const map = new ParallelMap(fn, { concurrency: 3 })
    const readable = runtime === 'node' ? map : (map as unknown as WebParallelMap).readable
    const input = runtime === 'node' ? (map as NodeParallelMap<number, number>) : (map as unknown as WebParallelMap).writable.getWriter()
    for (const i of range(4)) Promise.resolve(input.write(i)).catch(() => {})
    for await (const value of readable as AsyncIterable<number>) {
      if (value === 1) break
    }
    await turns(5)
    expect(stats.signals.slice(2).every((signal) => signal.aborted)).toBe(true)
    expect(stats.inFlight).toBe(0)
  })

  describe.each([
    ['undefined', undefined],
    ['null', null],
    ["''", ''],
    ['0', 0],
    ['false', false],
    ['NaN', Number.NaN],
  ])('when fn fails with %s', (shown, thrown) => {
    const expectFailure = (error: unknown) => {
      // Node reads a falsy error as success, so it's wrapped, keeping the original as `cause`.
      expect(error).toBeInstanceOf(Error)
      expect((error as Error).message).toBe(`syke: stream failed with ${shown}`)
      expect((error as Error).cause).toBe(thrown)
    }

    it.each([1, 3])('fails the stream when it throws synchronously (concurrency %i)', async (concurrency) => {
      const fn = (i: number) => {
        if (i === 2) throw thrown
        return i
      }
      const { error } = await runSettled(new ParallelMap(fn, { concurrency }), range(10))
      expectFailure(error)
    })

    it.each([1, 3])('fails the stream when it rejects (concurrency %i)', async (concurrency) => {
      const fn = async (i: number) => {
        await turns(1)
        if (i === 2) throw thrown
        return i
      }
      const { error } = await runSettled(new ParallelMap(fn, { concurrency }), range(10))
      expectFailure(error)
    })

    it('fails the stream when it rejects while idle', async () => {
      const fn = async () => {
        await turns(2)
        throw thrown
      }
      const stream = drive(runtime, new ParallelMap(fn, { concurrency: 2 }))
      await stream.write(1)
      expectFailure(await stream.done)
    })
  })

  it('keeps order with a high concurrency and results finishing in reverse', async () => {
    const total = 5000
    const gates = range(total).map(() => deferred<number>())
    const map = new ParallelMap((i: number) => gates[i]?.promise, { concurrency: total })
    const stream = drive<number>(runtime, map)
    for (const i of range(total)) stream.write(i).catch(() => {})
    await turns(2)
    for (let i = total - 1; i >= 0; i--) gates[i]?.resolve(i)
    await turns(2)
    await stream.end()
    expect(await stream.done).toBeUndefined()
    expect(stream.output).toEqual(range(total))
  })

  it('keeps order as the queue drains and refills', async () => {
    // Enough items to compact the queue's dead prefix many times over.
    const fn = async (i: number) => {
      await turns(latency(i))
      return i
    }
    expect(await run(new ParallelMap(fn, { concurrency: 2000 }), range(20_000))).toEqual(range(20_000))
  })

  it('rejects invalid options', () => {
    const fn = (i: unknown) => i
    // @ts-expect-error concurrency is required
    expect(() => new ParallelMap(fn, {})).toThrow(TypeError)
    // @ts-expect-error options are required
    expect(() => new ParallelMap(fn)).toThrow(TypeError)
    // @ts-expect-error fn is required
    expect(() => new ParallelMap(undefined, { concurrency: 1 })).toThrow(TypeError)
    expect(() => new ParallelMap(fn, { concurrency: 0 })).toThrow(RangeError)
    expect(() => new ParallelMap(fn, { concurrency: 1.5 })).toThrow(RangeError)
    expect(() => new ParallelMap(fn, { concurrency: 2 ** 53 })).toThrow(RangeError)
    // @ts-expect-error not a boolean
    expect(() => new ParallelMap(fn, { concurrency: 1, ordered: 'no' })).toThrow(TypeError)
  })
})

describe('ParallelMap (node)', () => {
  it('forwards only highWaterMark and signal to the Node stream', () => {
    const map = new NodeParallelMap((i: unknown) => i, { concurrency: 1, highWaterMark: 3, autoDestroy: false } as never)
    expect(map.readableHighWaterMark).toBe(3)
    expect(map.readableObjectMode).toBe(true)
    expect((map as unknown as { _readableState: { autoDestroy: boolean } })._readableState.autoDestroy).toBe(true)
  })
})

describe('ParallelMapCore', () => {
  it('drops finished results held back in order when closed', async () => {
    const pushed: number[] = []
    const sink = { push: (value: number) => pushed.push(value), end() {}, error() {}, full: false }
    const head = deferred<number>()
    const core = new ParallelMapCore<number, number>((i) => (i === 0 ? head.promise : i), { concurrency: 4 })
    core.start(sink)
    core.transform(0)
    core.transform(1)
    core.transform(2)
    await turns(1)
    // 1 and 2 are done but wait behind 0; closing must not leave them (or 0) to be emitted.
    core.close()
    head.resolve(0)
    await turns(1)
    expect(pushed).toEqual([])
  })
})
