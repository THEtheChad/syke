import { randomBytes } from 'node:crypto'
import { describe, expect, it } from 'vitest'
import { Rechunk as NodeRechunk } from '../../src/node.ts'
import { Rechunk as WebRechunk } from '../../src/web.ts'
import { chunk, pick, run, runtimes } from '../harness.ts'

describe.each(runtimes)('Rechunk (%s)', (runtime) => {
  const Rechunk = pick(runtime, { node: NodeRechunk, web: WebRechunk })
  const data = randomBytes(10_007)

  it.each([1, 7, 100, 1000, 1024, 4096, 20_000])('re-cuts input chunks of %i bytes into fixed-size chunks', async (inputSize) => {
    for (const size of [1, 3, 1000, 1024, 10_007, 20_000]) {
      const output = await run<Uint8Array>(new Rechunk({ size }), chunk(data, inputSize))
      expect(Buffer.concat(output).equals(data)).toBe(true)
      for (const piece of output.slice(0, -1)) expect(piece.byteLength).toBe(size)
      expect(output.at(-1)!.byteLength).toBe(data.length % size || Math.min(size, data.length))
    }
  })

  it('passes whole chunks as views of the input, without copying', async () => {
    const input = randomBytes(4096)
    const output = await run<Uint8Array>(new Rechunk({ size: 1024 }), [input])
    expect(output).toHaveLength(4)
    for (const piece of output) expect(piece.buffer).toBe(input.buffer)
  })

  it('never writes into a buffer it has handed out', async () => {
    const output = await run<Uint8Array>(new Rechunk({ size: 4 }), [
      Uint8Array.of(1, 2, 3),
      Uint8Array.of(4, 5, 6),
      Uint8Array.of(7, 8, 9),
      Uint8Array.of(10),
    ])
    // Snapshot after the stream has finished: earlier outputs must be intact.
    expect(output.map((piece) => [...piece])).toEqual([
      [1, 2, 3, 4],
      [5, 6, 7, 8],
      [9, 10],
    ])
  })

  it('copies a remainder, so a producer reusing its buffer does not corrupt output', async () => {
    const reused = new Uint8Array(3)
    async function* reusing() {
      for (const byte of [1, 2, 3]) {
        reused.fill(byte)
        yield reused
      }
    }
    const output = await run<Uint8Array>(new Rechunk({ size: 4 }), reusing())
    expect(Buffer.concat(output)).toEqual(Buffer.from([1, 1, 1, 2, 2, 2, 3, 3, 3]))
  })

  it('handles empty streams and empty chunks', async () => {
    expect(await run(new Rechunk({ size: 4 }), [])).toEqual([])
    expect(await run(new Rechunk({ size: 4 }), [new Uint8Array(0)])).toEqual([])
    const output = await run<Uint8Array>(new Rechunk({ size: 2 }), [new Uint8Array(0), Uint8Array.of(1), new Uint8Array(0), Uint8Array.of(2)])
    expect(output.map((piece) => [...piece])).toEqual([[1, 2]])
  })

  it('propagates a source error without emitting the partial chunk', async () => {
    async function* failing() {
      yield Uint8Array.of(1, 2, 3)
      throw new Error('boom')
    }
    await expect(run(new Rechunk({ size: 4 }), failing())).rejects.toThrow('boom')
  })

  it('rejects invalid options', () => {
    expect(() => new Rechunk({ size: 0 })).toThrow(RangeError)
    expect(() => new Rechunk({ size: 1.5 })).toThrow(RangeError)
    // @ts-expect-error size is required
    expect(() => new Rechunk({})).toThrow(RangeError)
  })
})

describe('Rechunk (node)', () => {
  it('emits Buffers, keeps chunk sizes under for-await, and sizes its readable buffer in bytes', async () => {
    const data = randomBytes(10_000)
    const rechunk = new NodeRechunk({ size: 1000, highWaterMark: 4000 })
    expect(rechunk.readableHighWaterMark).toBe(4)
    expect(rechunk.writableHighWaterMark).toBe(4000)
    expect(new NodeRechunk({ size: 1 << 20 }).readableHighWaterMark).toBe(1)
    const output = await run<Uint8Array>(rechunk, chunk(data, 333))
    expect(output.map((piece) => piece.byteLength)).toEqual(Array(10).fill(1000))
    for (const piece of output) expect(Buffer.isBuffer(piece)).toBe(true)
  })
})
