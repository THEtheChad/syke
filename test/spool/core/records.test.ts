import { describe, expect, it } from 'vitest'
import { FrameDecoder, encodeFrame, jsonCodec } from '../../../src/spool/core/records.ts'

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
