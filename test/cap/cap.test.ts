import { randomBytes } from 'node:crypto'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { ReadableStream as NodeReadableStream } from 'node:stream/web'
import { describe, expect, it } from 'vitest'
import { Cap as NodeCap, CapError } from '../../src/node.ts'
import { Cap as WebCap, CapError as WebCapError } from '../../src/web.ts'
import { chunk, pick, run, runBytes, runSettled, runtimes } from '../harness.ts'

describe.each(runtimes)('Cap (%s)', (runtime) => {
  const Cap = pick(runtime, { node: NodeCap, web: WebCap })
  const data = randomBytes(10_000)

  it('passes a stream within the cap through unchanged', async () => {
    const cap = new Cap({ maxBytes: data.length })
    expect((await runBytes(cap, chunk(data, 999))).equals(data)).toBe(true)
    expect(cap).toMatchObject({ bytes: data.length, items: 11, truncated: false })
  })

  it('fails before passing on the chunk that overruns', async () => {
    const { output, error } = await runSettled<Uint8Array>(new Cap({ maxBytes: 2500 }), chunk(data, 1000))
    expect(error).toBeInstanceOf(CapError)
    expect(error).toMatchObject({ code: 'ERR_SYKE_CAP', limit: 2500, unit: 'bytes' })
    expect(error.message).toBe('syke: stream exceeds the cap of 2500 bytes')
    expect(Buffer.concat(output).length).toBeLessThanOrEqual(2000)
  })

  it('truncates mid-chunk, passing on the part that fits', async () => {
    const cap = new Cap({ maxBytes: 2500, onLimit: 'truncate' })
    expect((await runBytes(cap, chunk(data, 1000))).equals(data.subarray(0, 2500))).toBe(true)
    expect(cap).toMatchObject({ bytes: 2500, items: 3, truncated: true })
  })

  it('truncates on a chunk boundary', async () => {
    const cap = new Cap({ maxBytes: 2000, onLimit: 'truncate' })
    expect((await runBytes(cap, chunk(data, 1000))).equals(data.subarray(0, 2000))).toBe(true)
    expect(cap.truncated).toBe(true)
  })

  it('is not truncated when the stream ends exactly at the cap', async () => {
    const cap = new Cap({ maxBytes: data.length, onLimit: 'truncate' })
    expect((await runBytes(cap, chunk(data, 1000))).length).toBe(data.length)
    expect(cap.truncated).toBe(false)
    await run(new Cap({ maxBytes: data.length }), chunk(data, 1000))
  })

  it('handles empty streams and empty chunks', async () => {
    expect(await run(new Cap({ maxBytes: 0 }), [])).toEqual([])
    expect((await runBytes(new Cap({ maxBytes: 0 }), [new Uint8Array(0)])).length).toBe(0)
    await expect(run(new Cap({ maxBytes: 0 }), [data.subarray(0, 1)])).rejects.toThrow(CapError)
    expect((await runBytes(new Cap({ maxBytes: 0, onLimit: 'truncate' }), [data])).length).toBe(0)
  })

  it('caps items', async () => {
    const records = Array.from({ length: 10 }, (_, i) => ({ i }))
    expect(await run(new Cap({ maxItems: 10 }), records)).toEqual(records)
    const truncated = new Cap<{ i: number }>({ maxItems: 3, onLimit: 'truncate' })
    expect(await run(truncated, records)).toEqual(records.slice(0, 3))
    expect(truncated).toMatchObject({ items: 3, truncated: true })
    const { output, error } = await runSettled(new Cap({ maxItems: 3 }), records)
    expect(error).toMatchObject({ code: 'ERR_SYKE_CAP', limit: 3, unit: 'items' })
    expect(output.length).toBeLessThanOrEqual(3)
  })

  it('propagates a source error', async () => {
    async function* failing() {
      yield data.subarray(0, 10)
      throw new Error('boom')
    }
    await expect(run(new Cap({ maxBytes: 100 }), failing())).rejects.toThrow('boom')
  })

  it('rejects invalid options', () => {
    // @ts-expect-error neither given
    expect(() => new Cap({})).toThrow(TypeError)
    // @ts-expect-error both given
    expect(() => new Cap({ maxBytes: 1, maxItems: 1 })).toThrow(TypeError)
    expect(() => new Cap({ maxBytes: -1 })).toThrow(RangeError)
    expect(() => new Cap({ maxItems: 1.5 })).toThrow(RangeError)
    // @ts-expect-error not an action
    expect(() => new Cap({ maxBytes: 1, onLimit: 'drop' })).toThrow(/onLimit must be 'error' or 'truncate'/)
  })
})

describe('Cap (web)', () => {
  it('fails a byte cap with a TypeError on a non-byte chunk rather than miscounting', async () => {
    const cap = new WebCap({ maxBytes: 10 })
    const { output, error } = await runSettled(cap, [new Uint8Array(2), 'abc'])
    expect(error).toBeInstanceOf(TypeError)
    expect(error.message).toBe('syke: Cap with maxBytes takes Uint8Array chunks, got string')
    expect(cap.bytes).toBe(2)
    expect(output.length).toBeLessThanOrEqual(1)
  })

  it('counts chunks of any type with maxItems', async () => {
    expect(await run(new WebCap<unknown>({ maxItems: 3 }), ['a', 1, { b: 2 }])).toEqual(['a', 1, { b: 2 }])
  })

  it('cancels a piped source after truncating', async () => {
    let cancelled = false
    let pulls = 0
    const source = new NodeReadableStream<Uint8Array>({
      pull(controller) {
        pulls++
        controller.enqueue(new Uint8Array(100))
      },
      cancel() {
        cancelled = true
      },
    })
    const cap = new WebCap({ maxBytes: 250, onLimit: 'truncate' })
    let total = 0
    for await (const piece of source.pipeThrough(cap as never) as AsyncIterable<Uint8Array>) total += piece.byteLength
    expect(total).toBe(250)
    // Let the pipe notice the closed writable side.
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(cancelled).toBe(true)
    expect(pulls).toBeLessThan(10)
  })
})

describe('Cap (node)', () => {
  it('discards the rest of a finite source after truncating, so pipeline() resolves', async () => {
    const data = randomBytes(10_000)
    const output: Buffer[] = []
    const cap = new NodeCap({ maxBytes: 2500, onLimit: 'truncate' })
    await pipeline(Readable.from(chunk(data, 1000)), cap, async (source: AsyncIterable<Buffer>) => {
      for await (const piece of source) output.push(piece)
    })
    expect(Buffer.concat(output).equals(data.subarray(0, 2500))).toBe(true)
  })

  it('forwards only highWaterMark and signal to the Node stream', () => {
    const cap = new NodeCap({ maxBytes: 3, highWaterMark: 11, objectMode: true } as never)
    expect(cap.writableObjectMode).toBe(false)
    expect(cap.writableHighWaterMark).toBe(11)
    const items = new NodeCap({ maxItems: 3, objectMode: false } as never)
    expect(items.writableObjectMode).toBe(true)
  })

  it('accepts strings in byte mode', async () => {
    expect((await runBytes(new NodeCap({ maxBytes: 3, onLimit: 'truncate' }), ['hello' as never])).toString()).toBe('hel')
  })
})

it('shares one CapError class between syke/node and syke/web', () => {
  expect(WebCapError).toBe(CapError)
})
