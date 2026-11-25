import { runInNewContext } from 'node:vm'
import { describe, expect, it } from 'vitest'
import {
  type Codec,
  FrameDecoder,
  SpoolCodecError,
  encodeFrame,
  encodeRecord,
  jsonCodec,
  writeFrame,
} from '../../../src/spool/core/records.ts'

const records = [{ id: 1, name: 'a' }, [1, 2, 3], 'text', 42, null, { nested: { deep: true } }]

describe('frames', () => {
  const stream = (() => {
    const frames = records.map((r) => encodeFrame(jsonCodec.encode(r)))
    const out = new Uint8Array(frames.reduce((n, f) => n + f.length, 0))
    let offset = 0
    for (const frame of frames) {
      out.set(frame, offset)
      offset += frame.length
    }
    return out
  })()

  it.each([1, 2, 3, 4, 5, 7, 16, 1000])('round-trips records read back %i bytes at a time', (size) => {
    const decoder = new FrameDecoder()
    const decoded: unknown[] = []
    for (let i = 0; i < stream.length; i += size) {
      for (const payload of decoder.push(stream.subarray(i, i + size))) decoded.push(jsonCodec.decode(payload))
    }
    expect(decoded).toEqual(records)
    expect(decoder.pendingBytes).toBe(0)
  })

  it('holds an incomplete frame until the rest arrives', () => {
    const decoder = new FrameDecoder()
    const frame = encodeFrame(new Uint8Array([9, 8, 7]))
    expect(decoder.push(frame.subarray(0, 5))).toEqual([])
    expect(decoder.pendingBytes).toBe(5)
    expect(decoder.push(frame.subarray(5))).toEqual([new Uint8Array([9, 8, 7])])
  })

  it('handles empty payloads', () => {
    const decoder = new FrameDecoder()
    expect(decoder.push(encodeFrame(new Uint8Array()))).toEqual([new Uint8Array()])
  })
})

describe('jsonCodec', () => {
  it.each([undefined, () => 1, Symbol('s')])('refuses to encode %s, which has no JSON form', (value) => {
    expect(() => jsonCodec.encode(value)).toThrow(TypeError)
    expect(() => jsonCodec.encode(value)).toThrow(/^syke: jsonCodec cannot encode/)
  })

  it('fails encodeRecord() with a SpoolCodecError on write, not later on read', () => {
    let error: unknown
    try {
      encodeRecord(jsonCodec, undefined)
    } catch (caught) {
      error = caught
    }
    expect(error).toBeInstanceOf(SpoolCodecError)
    expect(error).toMatchObject({ code: 'ERR_SYKE_SPOOL_CODEC', message: /encode/, cause: expect.any(TypeError) })
  })

  it('still encodes undefined nested in objects and arrays', () => {
    expect(jsonCodec.decode(jsonCodec.encode({ a: undefined, b: [undefined] }))).toEqual({ b: [null] })
  })
})

describe('encodeRecord()', () => {
  const returning = (value: unknown): Codec => ({ encode: () => value as Uint8Array, decode: () => null })

  it('accepts a Uint8Array from another realm', () => {
    const foreign = runInNewContext('new Uint8Array([1, 2, 3])') as Uint8Array
    expect(foreign instanceof Uint8Array).toBe(false)
    expect(encodeRecord(returning(foreign), 'x')).toBe(foreign)
  })

  it('accepts a Node Buffer', () => {
    const buffer = Buffer.from('abc')
    expect(encodeRecord(returning(buffer), 'x')).toBe(buffer)
  })

  it.each([
    ['a DataView', new DataView(new ArrayBuffer(4))],
    ['a Uint16Array', new Uint16Array(2)],
    ['an ArrayBuffer', new ArrayBuffer(4)],
    ['a string', 'bytes'],
    ['a look-alike', { [Symbol.toStringTag]: 'Uint8Array', byteLength: 0 }],
  ])('rejects %s', (_name, value) => {
    expect(() => encodeRecord(returning(value), 'x')).toThrow(SpoolCodecError)
  })
})

describe('frame headers', () => {
  it('writes the length big-endian', () => {
    const frame = encodeFrame(new Uint8Array(0x010203))
    expect([...frame.subarray(0, 4)]).toEqual([0x00, 0x01, 0x02, 0x03])
  })

  it('frames several records into one buffer with writeFrame()', () => {
    const payloads = [new Uint8Array([1]), new Uint8Array(), new Uint8Array([2, 3])]
    const out = new Uint8Array(payloads.reduce((n, p) => n + 4 + p.length, 0))
    let offset = 0
    for (const payload of payloads) offset = writeFrame(out, offset, payload)
    expect(offset).toBe(out.length)
    expect(new FrameDecoder().push(out)).toEqual(payloads)
  })

  it('reads a length of 2 GiB or more as positive (waiting for the rest)', () => {
    const decoder = new FrameDecoder()
    expect(decoder.push(new Uint8Array([0x80, 0, 0, 0, 1, 2]))).toEqual([])
    expect(decoder.pendingBytes).toBe(6)
  })

  it('reads a header split across pieces', () => {
    const decoder = new FrameDecoder()
    const frame = encodeFrame(new Uint8Array([7, 8, 9]))
    for (const byte of frame.subarray(0, 3)) expect(decoder.push(new Uint8Array([byte]))).toEqual([])
    expect(decoder.push(frame.subarray(3))).toEqual([new Uint8Array([7, 8, 9])])
    expect(decoder.pendingBytes).toBe(0)
  })
})

describe('FrameDecoder queueing', () => {
  it('handles many frames per push and many pushes per frame', () => {
    const payloads = Array.from({ length: 3000 }, (_, i) => new Uint8Array(i % 50).fill(i % 256))
    const total = payloads.reduce((n, p) => n + 4 + p.length, 0)
    const stream = new Uint8Array(total)
    let offset = 0
    for (const payload of payloads) offset = writeFrame(stream, offset, payload)

    for (const size of [1, 3, 64, 4096, total]) {
      const decoder = new FrameDecoder()
      const decoded: Uint8Array[] = []
      for (let i = 0; i < total; i += size) decoded.push(...decoder.push(stream.subarray(i, i + size)))
      expect(decoded).toEqual(payloads)
      expect(decoder.pendingBytes).toBe(0)
    }
  })

  it('copies a small remainder out of the large read buffer it came from, so it is not pinned', () => {
    const decoder = new FrameDecoder()
    const slab = new Uint8Array(64 * 1024)
    slab.set(encodeFrame(new Uint8Array(10).fill(1)), 0)
    // The start of the next frame: header for 3 bytes, then 1 of them.
    slab.set([0, 0, 0, 3, 42], 14)
    expect(decoder.push(slab.subarray(0, 19))).toHaveLength(1)
    // A reader that recycles its buffer must not corrupt the pending bytes.
    slab.fill(0)
    expect(decoder.push(new Uint8Array([43, 44]))).toEqual([new Uint8Array([42, 43, 44])])
  })
})
