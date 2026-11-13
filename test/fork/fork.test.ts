import { randomBytes } from 'node:crypto'
import { PassThrough } from 'node:stream'
import { describe, expect, it } from 'vitest'
import { fork as nodeFork, Spool } from '../../src/node.ts'
import { fork as webFork } from '../../src/web.ts'
import { pick, runtimes } from '../harness.ts'
import { useTempDir } from '../nodeHelpers.ts'
import { collect, collectSettled, probe, reader, readUpTo, settle } from '../streamHarness.ts'

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
})

describe('fork (node)', () => {
  const tmp = useTempDir()

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
})
