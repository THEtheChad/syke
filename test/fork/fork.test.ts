import { randomBytes } from 'node:crypto'
import { PassThrough, Writable } from 'node:stream'
import { describe, expect, it, vi } from 'vitest'
import type { Outlet, Puller } from '../../src/shared/combinator/core.ts'
import { ForkCore } from '../../src/fork/core.ts'
import { fork as nodeFork, Spool } from '../../src/node.ts'
import { fork as webFork } from '../../src/web.ts'
import { pick, runtimes } from '../harness.ts'
import { useTempDir } from '../nodeHelpers.ts'
import {
  collect,
  collectSettled,
  endedSource,
  endedSourceKinds,
  generator,
  probe,
  reader,
  readUpTo,
  settle,
  staysOpenAtEnd,
} from '../streamHarness.ts'

describe.each(runtimes)('fork (%s)', (runtime) => {
  const fork = pick(runtime, { node: nodeFork, web: webFork }) as (source: unknown, options?: any) => any[]
  /** A buffer that holds up to `size` chunks without applying backpressure. */
  const roomy = (size: number) =>
    pick(runtime, {
      node: () => new PassThrough({ objectMode: true, highWaterMark: size }),
      web: () => new TransformStream({}, { highWaterMark: size }),
    })

  it('gives every branch every chunk, sharing chunk objects', async () => {
    const chunks = [{ a: 1 }, 'two', 3, [4]]
    const [a, b, c] = fork(chunks, { count: 3 })
    const [outA, outB, outC] = await Promise.all([collect(a), collect(b), collect(c)])
    expect(outA).toEqual(chunks)
    expect(outB).toEqual(chunks)
    expect(outC).toEqual(chunks)
    expect(outB[0]).toBe(outA[0])
  })

  it('defaults to two branches', () => {
    expect(fork([1])).toHaveLength(2)
  })

  it('reads streams as well as iterables', async () => {
    const source = probe(runtime, { length: 5 })
    const [a, b] = fork(source.stream)
    expect(await Promise.all([collect(a), collect(b)])).toEqual([
      [0, 1, 2, 3, 4],
      [0, 1, 2, 3, 4],
    ])
  })

  it('ends every branch for an empty source', async () => {
    const [a, b] = fork(probe(runtime, { length: 0 }).stream)
    expect(await Promise.all([collect(a), collect(b)])).toEqual([[], []])
  })

  it('stops pulling the source while one branch is stalled', async () => {
    const source = probe(runtime)
    const [fast, slow] = fork(source.stream)
    const fastReader = reader(fast)
    const slowReader = reader(slow)
    // Both branches start; then the slow one stops reading.
    await Promise.all([readUpTo(fastReader, 2), readUpTo(slowReader, 2)])
    const read = await readUpTo(fastReader, 1000)
    const pulls = source.pulls
    await settle()
    // The source stopped: it isn't buffering for the stalled branch without limit.
    expect(source.pulls).toBe(pulls)
    expect(pulls).toBeGreaterThan(2)
    expect(pulls).toBeLessThan(40)
    expect(read.length).toBeLessThan(40)

    // Reading the slow branch lets both move on again.
    await readUpTo(slowReader, 100)
    expect((await readUpTo(fastReader, 1000)).length).toBeGreaterThan(0)
    expect(source.pulls).toBeGreaterThan(pulls)
    await fastReader.cancel()
    await slowReader.cancel()
  })

  it('lets a buffered branch fall behind without holding back the others', async () => {
    const source = probe(runtime, { length: 200 })
    const [fast, slow] = fork(source.stream, { buffer: roomy(1000) })
    expect(await collect(fast)).toHaveLength(200)
    expect(await collect(slow)).toHaveLength(200)
  })

  it('errors every branch when the source fails', async () => {
    const error = new Error('source failed')
    const [a, b] = fork(probe(runtime, { length: 10, failAt: 3, error }).stream)
    const [resultA, resultB] = await Promise.all([collectSettled(a), collectSettled(b)])
    expect(resultA.error).toBe(error)
    expect(resultB.error).toBe(error)
    expect(resultA.output.length).toBeLessThanOrEqual(3)
  })

  it('errors buffered branches when the source fails', async () => {
    const error = new Error('source failed')
    const [a, b] = fork(probe(runtime, { length: 10, failAt: 3, error }).stream, { buffer: roomy(100) })
    const [resultA, resultB] = await Promise.all([collectSettled(a), collectSettled(b)])
    expect(resultA.error).toBe(error)
    expect(resultB.error).toBe(error)
  })

  it('detaches a cancelled branch, and cancels the source once every branch is gone', async () => {
    const source = probe(runtime)
    const [a, b] = fork(source.stream)
    const readerA = reader(a)
    const readerB = reader(b)
    await readUpTo(readerA, 3)
    await readerB.cancel()
    // With b gone, a is no longer held back.
    expect(await readUpTo(readerA, 100)).toHaveLength(100)
    expect(source.cancelled).toBe(false)
    await readerA.cancel()
    await settle()
    expect(source.cancelled).toBe(true)
  })

  it('detaches a cancelled buffered branch', async () => {
    const source = probe(runtime, { length: 50 })
    const [a, b] = fork(source.stream, { buffer: roomy(1) })
    await reader(b).cancel()
    expect(await collect(a)).toHaveLength(50)
  })

  it('rejects invalid options', () => {
    expect(() => fork([1], { count: 0 })).toThrow(RangeError)
    expect(() => fork([1], { count: 1.5 })).toThrow(/syke: count must be a positive integer/)
    expect(() => fork([1], { buffer: 'big' })).toThrow(TypeError)
    expect(() => fork([1], { buffer: () => ({}) })).toThrow(/syke: buffer\(\) must return/)
    expect(() => fork('text')).toThrow(/syke: source must be a stream or an iterable of chunks/)
    expect(() => fork(42)).toThrow(TypeError)
  })

  it('checks the source before making any buffer', () => {
    const buffer = vi.fn(roomy(1))
    expect(() => fork(42, { buffer })).toThrow(/syke: source must be a stream/)
    expect(() => fork('text', { buffer })).toThrow(/wrap it in an array/)
    expect(buffer).not.toHaveBeenCalled()
  })

  /** A buffer that records whether it was released. */
  const tracked = () => {
    const state = { released: false }
    const make = pick(runtime, {
      node: () => {
        const through = new PassThrough({ objectMode: true })
        through.on('close', () => {
          state.released = true
        })
        return through
      },
      web: () => ({
        writable: new WritableStream({
          abort() {
            state.released = true
          },
        }),
        readable: new ReadableStream({
          cancel() {
            state.released = true
          },
        }),
      }),
    })
    return { state, make }
  }

  it('releases the buffers already made when a later buffer() is invalid', async () => {
    const first = tracked()
    let calls = 0
    const buffer = () => (calls++ === 0 ? first.make() : {})
    expect(() => fork([1], { count: 3, buffer })).toThrow(/syke: buffer\(\) must return/)
    expect(calls).toBe(2)
    await settle()
    expect(first.state.released).toBe(true)
  })

  it('releases the buffers already made when a later buffer() throws', async () => {
    const first = tracked()
    const error = new Error('no room')
    let calls = 0
    const buffer = () => {
      if (calls++ === 0) return first.make()
      throw error
    }
    expect(() => fork([1], { buffer })).toThrow(error)
    await settle()
    expect(first.state.released).toBe(true)
  })

  it('rejects a locked ReadableStream before making any buffer', () => {
    const locked = new ReadableStream()
    locked.getReader()
    const buffer = vi.fn(roomy(1))
    expect(() => fork(locked, { buffer })).toThrow(/syke: source is a locked ReadableStream/)
    expect(buffer).not.toHaveBeenCalled()
  })

  it('releases the buffers when the source cannot be opened', async () => {
    const error = new Error('no iterator')
    const broken = {
      [Symbol.iterator]() {
        throw error
      },
    }
    const made = [tracked(), tracked()]
    let i = 0
    expect(() => fork(broken, { buffer: () => made[i++]!.make() })).toThrow(error)
    await settle()
    expect(made.map((m) => m.state.released)).toEqual([true, true])
  })

  it.each([
    ['', undefined],
    [', through a buffer', roomy(4)],
  ])('forwards the last branch\'s cancel reason to the source%s', async (_, buffer) => {
    const reasons: unknown[] = []
    let i = 0
    const source = new ReadableStream({
      pull(controller) {
        controller.enqueue(i++)
      },
      cancel(reason) {
        reasons.push(reason)
      },
    })
    const [a, b] = fork(source, { buffer })
    const readerA = reader(a)
    const readerB = reader(b)
    await Promise.all([readUpTo(readerA, 1), readUpTo(readerB, 1)])
    await readerA.cancel(new Error('first'))
    expect(reasons).toEqual([])
    const last = new Error('last')
    await readerB.cancel(last)
    await settle()
    expect(reasons).toEqual([last])
  })

  it('cancels the source once every buffered branch is gone', async () => {
    const source = probe(runtime)
    const [a, b] = fork(source.stream, { buffer: roomy(4) })
    const readerA = reader(a)
    const readerB = reader(b)
    await Promise.all([readUpTo(readerA, 3), readUpTo(readerB, 3)])
    await readerA.cancel()
    await settle()
    expect(source.cancelled).toBe(false)
    await readerB.cancel()
    await settle()
    expect(source.cancelled).toBe(true)
  })

  it('reads a source whose Symbol.asyncIterator is present but undefined', async () => {
    const source = { [Symbol.asyncIterator]: undefined, *[Symbol.iterator]() { yield* [1, 2] } }
    const [a, b] = fork(source)
    expect(await Promise.all([collect(a), collect(b)])).toEqual([
      [1, 2],
      [1, 2],
    ])
  })
})

describe('ForkCore', () => {
  const outlet = () => ({ push: vi.fn(), end: vi.fn(), error: vi.fn() }) satisfies Outlet<unknown>
  /** A source that never produces anything. */
  const idle = (): Puller<unknown> => ({ next: () => new Promise(() => {}), cancel: () => {}, release: () => {} })

  // Not reachable through fork(), which starts every branch at once; this
  // checks the core keeps its side of the Producer contract anyway.
  it('errors a branch that starts after the source failed', () => {
    const error = new Error('early')
    const core = new ForkCore((onError) => {
      onError(error)
      return idle()
    }, 2)
    const first = outlet()
    const second = outlet()
    core.producers[0]!.start(first)
    core.producers[1]!.start(second)
    expect(first.error).toHaveBeenCalledWith(error)
    expect(second.error).toHaveBeenCalledWith(error)
  })

  it('does not error a branch that was cancelled before it started', () => {
    const fail: { onError?: (error: unknown) => void } = {}
    const core = new ForkCore((onError) => {
      fail.onError = onError
      return idle()
    }, 2)
    core.producers[0]!.cancel(undefined)
    fail.onError?.(new Error('late'))
    const first = outlet()
    core.producers[0]!.start(first)
    expect(first.error).not.toHaveBeenCalled()
  })
})

/** Resolves with the next uncaught exception, which would otherwise fail the test run. */
function nextUncaught(): { error: Promise<unknown>; restore: () => void } {
  const saved = process.listeners('uncaughtException')
  process.removeAllListeners('uncaughtException')
  const restore = () => {
    process.removeAllListeners('uncaughtException')
    for (const listener of saved) process.on('uncaughtException', listener)
  }
  const error = new Promise<unknown>((resolve) => {
    process.once('uncaughtException', (e) => {
      restore()
      resolve(e)
    })
  })
  return { error, restore }
}

// Outside the block below: within its async temp-dir hooks, vitest doesn't deliver
// uncaught exceptions to process listeners, so this test can't see one there.
describe('fork (node) error surfacing', () => {
  it('surfaces a buffered branch\'s error through .pipe() instead of swallowing it', async () => {
    const error = new Error('source failed')
    const [a] = nodeFork(probe('node', { length: 10, failAt: 2, error }).stream, {
      count: 1,
      buffer: () => new PassThrough({ objectMode: true }),
    })
    const uncaught = nextUncaught()
    try {
      // No 'error' listener of ours: like any stream, the error must not vanish.
      a!.pipe(new Writable({ objectMode: true, write: (_chunk, _encoding, done) => done() }))
      const swallowed = new Promise((resolve) => setTimeout(resolve, 1000, 'swallowed'))
      const caught = await Promise.race([uncaught.error, swallowed])
      expect(caught).toBe(error)
    } finally {
      uncaught.restore()
    }
  })

})

describe('fork (node)', () => {
  const tmp = useTempDir()

  it('emits a buffered branch\'s error to its own listeners, once', async () => {
    const error = new Error('source failed')
    const [a] = nodeFork(probe('node', { length: 10, failAt: 2, error }).stream, {
      count: 1,
      buffer: () => new PassThrough({ objectMode: true }),
    })
    const errors: unknown[] = []
    a!.on('error', (e) => errors.push(e))
    a!.resume()
    await settle()
    expect(errors).toEqual([error])
  })

  it('detaches a buffered branch whose buffer fails, leaving the others running', async () => {
    const source = probe('node', { length: 50 })
    const [a, b] = nodeFork(source.stream, { buffer: () => new PassThrough({ objectMode: true, highWaterMark: 1 }) })
    const error = new Error('buffer failed')
    const errors: unknown[] = []
    b!.on('error', (e) => errors.push(e))
    b!.destroy(error)
    expect(await collect(a)).toHaveLength(50)
    expect(errors).toEqual([error])
  })

  it('cancels the source when every buffered branch is destroyed', async () => {
    const source = probe('node')
    const branches = nodeFork(source.stream, { buffer: () => new PassThrough({ objectMode: true }) })
    for (const branch of branches) branch.destroy()
    await settle()
    expect(source.cancelled).toBe(true)
  })

  it('errors every branch, and stops the source, when the source yields null', async () => {
    const source = generator((i) => (i === 2 ? null : i))
    const [a, b] = nodeFork(source)
    const [resultA, resultB] = await Promise.all([collectSettled(a), collectSettled(b)])
    for (const result of [resultA, resultB]) {
      expect(result.error).toBeInstanceOf(TypeError)
      expect(result.error.message).toMatch(/syke: a source produced null/)
      expect(result.error.code).toBe('ERR_STREAM_NULL_VALUES')
      expect(result.output).toEqual([0, 1])
    }
    await settle()
    expect(source.returned).toBe(true)
  })

  it('reads nothing past a null chunk', async () => {
    let reads = 0
    const source = {
      [Symbol.iterator]: () => ({
        next: () => {
          reads++
          return { done: false, value: reads === 3 ? null : reads }
        },
      }),
    }
    const [a, b, c] = nodeFork(source, { count: 3 })
    const results = await Promise.all([collectSettled(a), collectSettled(b), collectSettled(c)])
    for (const result of results) expect(result.error?.code).toBe('ERR_STREAM_NULL_VALUES')
    await settle()
    expect(reads).toBe(3)
  })

  it('errors the branches with an Error when the source fails with a falsy value', async () => {
    async function* failing() {
      yield 1
      throw undefined
    }
    const [a] = nodeFork(failing(), { count: 1 })
    const { error } = await collectSettled(a)
    expect(error).toBeInstanceOf(Error)
    expect(error.message).toMatch(/syke: a source failed with undefined/)
  })

  it('keeps a byte stream in byte mode', async () => {
    const data = randomBytes(1000)
    const [a, b] = nodeFork(probe('node', { length: 4, objectMode: false, chunk: () => data }).stream)
    expect(a?.readableObjectMode).toBe(false)
    const [outA, outB] = await Promise.all([collect<Buffer>(a), collect<Buffer>(b)])
    expect(Buffer.concat(outA)).toHaveLength(4000)
    expect(Buffer.concat(outB)).toHaveLength(4000)
  })

  it('spills a stalled branch to disk through a Spool', async () => {
    const data = randomBytes(64 * 1024)
    const source = probe('node', { length: 64, objectMode: false, chunk: () => data })
    const [fast, slow] = nodeFork(source.stream, { buffer: () => new Spool({ dir: tmp.path, minFreeSpace: false }) })
    expect(Buffer.concat(await collect<Buffer>(fast))).toHaveLength(64 * data.length)
    expect(Buffer.concat(await collect<Buffer>(slow))).toHaveLength(64 * data.length)
  })

  // Stopped from the source's own 'end' listener, which runs before fork
  // hears of the end.
  describe.each(endedSourceKinds)('with a %s that has ended', (kind) => {
    // Some Node releases destroy a half-open Duplex themselves at its end.
    const test = it.skipIf(!staysOpenAtEnd[kind])

    test('leaves it undestroyed when every branch is destroyed', async () => {
      const source = endedSource(kind)
      const branches = nodeFork(source.stream)
      source.stream.once('end', () => {
        for (const branch of branches) branch.destroy()
      })
      await Promise.all(branches.map((branch) => collectSettled(branch)))
      await settle()
      expect(source.alive()).toBe(true)
      source.stream.destroy()
    })

    test('leaves it undestroyed when every buffered branch is destroyed', async () => {
      const source = endedSource(kind)
      const branches = nodeFork(source.stream, { buffer: () => new PassThrough() })
      source.stream.once('end', () => {
        for (const branch of branches) branch.destroy()
      })
      await Promise.all(branches.map((branch) => collectSettled(branch)))
      await settle()
      expect(source.alive()).toBe(true)
      source.stream.destroy()
    })
  })
})
