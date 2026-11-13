/**
 * Turns records into bytes and back, so an object-mode spool can write them
 * to storage. Must round-trip every value the stream carries.
 */
export interface Codec<T = unknown> {
  encode(value: T): Uint8Array
  decode(bytes: Uint8Array): T
}

const encoder = new TextEncoder()
const decoder = new TextDecoder()

/** JSON over UTF-8. Runtime-neutral; loses what JSON can't express (Dates become strings, etc.). */
export const jsonCodec: Codec = {
  encode: (value) => encoder.encode(JSON.stringify(value)),
  decode: (bytes) => JSON.parse(decoder.decode(bytes)),
}

/** Bytes of the length prefix on every frame: a big-endian uint32. */
export const FRAME_HEADER_BYTES = 4
const MAX_FRAME = 0xffff_ffff

/** Prefixes `payload` with its length, so frames can be split back apart. */
export function encodeFrame(payload: Uint8Array): Uint8Array {
  if (payload.byteLength > MAX_FRAME) {
    throw new RangeError(`syke: record is ${payload.byteLength} bytes encoded; the limit is ${MAX_FRAME}`)
  }
  const frame = new Uint8Array(FRAME_HEADER_BYTES + payload.byteLength)
  new DataView(frame.buffer).setUint32(0, payload.byteLength)
  frame.set(payload, FRAME_HEADER_BYTES)
  return frame
}

/**
 * Reassembles frames from bytes read back in arbitrary pieces (a frame may
 * span several reads, and a read may hold several frames).
 */
export class FrameDecoder {
  #pending: Uint8Array[] = []
  #pendingBytes = 0

  /** Bytes held toward an incomplete frame. */
  get pendingBytes(): number {
    return this.#pendingBytes
  }

  /** Adds `bytes` and returns the payloads of every frame now complete, in order. */
  push(bytes: Uint8Array): Uint8Array[] {
    if (bytes.byteLength > 0) {
      this.#pending.push(bytes)
      this.#pendingBytes += bytes.byteLength
    }
    const frames: Uint8Array[] = []
    while (this.#pendingBytes >= FRAME_HEADER_BYTES) {
      const header = this.#peek(FRAME_HEADER_BYTES)
      const length = new DataView(header.buffer, header.byteOffset, FRAME_HEADER_BYTES).getUint32(0)
      if (this.#pendingBytes < FRAME_HEADER_BYTES + length) break
      this.#take(FRAME_HEADER_BYTES)
      frames.push(this.#take(length))
    }
    return frames
  }

  /** The first `n` pending bytes, without consuming them. */
  #peek(n: number): Uint8Array {
    const first = this.#pending[0] as Uint8Array
    if (first.byteLength >= n) return first.subarray(0, n)
    return this.#copy(n, false)
  }

  /** Removes and returns the first `n` pending bytes. */
  #take(n: number): Uint8Array {
    const first = this.#pending[0]
    if (first && first.byteLength >= n) {
      // Fast path: no copy when the bytes sit in one piece.
      const out = first.subarray(0, n)
      if (first.byteLength === n) this.#pending.shift()
      else this.#pending[0] = first.subarray(n)
      this.#pendingBytes -= n
      return out
    }
    return this.#copy(n, true)
  }

  #copy(n: number, consume: boolean): Uint8Array {
    const out = new Uint8Array(n)
    let filled = 0
    let index = 0
    while (filled < n) {
      const piece = this.#pending[index] as Uint8Array
      const used = Math.min(piece.byteLength, n - filled)
      out.set(piece.subarray(0, used), filled)
      filled += used
      if (consume) {
        if (used === piece.byteLength) this.#pending.shift()
        else this.#pending[0] = piece.subarray(used)
      } else {
        index++
      }
    }
    if (consume) this.#pendingBytes -= n
    return out
  }
}
