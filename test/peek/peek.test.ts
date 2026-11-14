import { describe, expect, it } from 'vitest'
import { peek as nodePeek } from '../../src/node.ts'
import { peek as webPeek } from '../../src/web.ts'
import { chunk, pick, runtimes } from '../harness.ts'
import { collect, collectSettled, probe, reader, readUpTo, settle } from '../streamHarness.ts'

const data = Uint8Array.from({ length: 100 }, (_, i) => i)
const join = (chunks: Uint8Array[]) => Buffer.concat(chunks)

describe.each(runtimes)('peek (%s)', (runtime) => {
  const peek = pick(runtime, { node: nodePeek, web: webPeek }) as (
    source: unknown,
    bytes: number,
  ) => Promise<{ head: Uint8Array; stream: any }>
  const bytes = (options: { length?: number; failAt?: number; error?: Error } = {}) =>
    probe(runtime, { objectMode: false, chunk: (i) => data.subarray((i * 7) % 93, ((i * 7) % 93) + 7), ...options })

  it('returns the head across chunk boundaries and replays it with the rest', async () => {
    const { head, stream } = await peek(chunk(data, 3), 5)
    expect([...head]).toEqual([0, 1, 2, 3, 4])
    expect(join(await collect(stream))).toEqual(Buffer.from(data))
  })

  it('returns a copy, not a view of the source chunk', async () => {
    const { head } = await peek([data], 4)
    expect(head.buffer).not.toBe(data.buffer)
    expect(head.byteLength).toBe(4)
  })

  it('returns a shorter head when the source ends sooner', async () => {
    const { head, stream } = await peek(chunk(data.subarray(0, 10), 4), 64)
    expect(head.byteLength).toBe(10)
    expect(join(await collect(stream))).toEqual(Buffer.from(data.subarray(0, 10)))
  })

  it('handles an empty source and a zero-byte peek', async () => {
    const empty = await peek(probe(runtime, { length: 0, objectMode: false }).stream, 4)
    expect(empty.head.byteLength).toBe(0)
    expect(await collect(empty.stream)).toEqual([])

    const none = await peek(chunk(data, 10), 0)
    expect(none.head.byteLength).toBe(0)
    expect(join(await collect(none.stream))).toEqual(Buffer.from(data))
  })

  it('reads only what it needs, then streams the rest on demand', async () => {
    const source = bytes()
    const { head, stream } = await peek(source.stream, 10)
    expect(head.byteLength).toBe(10)
    await settle()
    expect(source.emitted).toBeLessThan(5)

    const r = reader(stream)
    expect((await readUpTo(r, 50)).length).toBe(50)
    const pulls = source.pulls
    await settle()
    expect(source.pulls - pulls).toBeLessThan(40)
    await r.cancel()
    await settle()
    expect(source.cancelled).toBe(true)
  })

  it('rejects when the source fails while peeking', async () => {
    const error = new Error('early')
    await expect(peek(bytes({ failAt: 1, error }).stream, 10)).rejects.toBe(error)
  })

  it('errors the stream when the source fails after peeking', async () => {
    const error = new Error('late')
    const { stream } = await peek(bytes({ failAt: 5, error }).stream, 3)
    const { output, error: caught } = await collectSettled<Uint8Array>(stream)
    expect(caught).toBe(error)
    expect(output.length).toBeLessThanOrEqual(5)
  })

  it('rejects a source that yields something other than bytes', async () => {
    await expect(peek([{ not: 'bytes' }], 2)).rejects.toThrow(/syke: peek\(\) reads bytes, but the source produced Object/)
  })

  it('passes later chunks through unchecked', async () => {
    const { stream } = await peek([data.subarray(0, 2), 'later'], 2)
    expect(await collect(stream)).toEqual([data.subarray(0, 2), 'later'])
  })

  it('rejects invalid arguments', () => {
    expect(() => peek([data], -1)).toThrow(RangeError)
    expect(() => peek([data], 1.5)).toThrow(/syke: bytes must be a non-negative integer/)
    expect(() => peek(data, 2)).toThrow(/wrap it in an array/)
    expect(() => peek(null, 2)).toThrow(TypeError)
  })
})
