import { SykeError } from '../../shared/errors.ts'

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
  encode: (value) => {
    const json = JSON.stringify(value)
    // `undefined`, functions and symbols have no JSON form: fail now, on write,
    // rather than writing zero bytes that can't be parsed back.
    if (json === undefined) throw new TypeError(`syke: jsonCodec cannot encode ${typeof value}`)
    return encoder.encode(json)
  },
  decode: (bytes) => JSON.parse(decoder.decode(bytes)),
}

/** Bytes of the length prefix on every frame: a big-endian uint32. */
export const FRAME_HEADER_BYTES = 4
const MAX_FRAME = 0xffff_ffff

function checkFrameSize(byteLength: number): void {
  if (byteLength > MAX_FRAME) {
    throw new RangeError(`syke: record is ${byteLength} bytes encoded; the limit is ${MAX_FRAME}`)
  }
}

/**
 * Writes `payload`, prefixed with its length, into `target` at `offset` and
 * returns the offset just past it. `target` needs `FRAME_HEADER_BYTES +
 * payload.byteLength` bytes free there. Lets callers frame many records into
 * one buffer.
 */
export function writeFrame(target: Uint8Array, offset: number, payload: Uint8Array): number {
  const length = payload.byteLength
  checkFrameSize(length)
  target[offset] = length >>> 24
  target[offset + 1] = (length >>> 16) & 0xff
  target[offset + 2] = (length >>> 8) & 0xff
  target[offset + 3] = length & 0xff
  target.set(payload, offset + FRAME_HEADER_BYTES)
  return offset + FRAME_HEADER_BYTES + length
}

/** Prefixes `payload` with its length, so frames can be split back apart. */
export function encodeFrame(payload: Uint8Array): Uint8Array {
  checkFrameSize(payload.byteLength)
  const frame = new Uint8Array(FRAME_HEADER_BYTES + payload.byteLength)
  writeFrame(frame, 0, payload)
  return frame
}

/**
 * A pending piece smaller than this share of the buffer it views is copied
 * out, so a small remainder doesn't keep a large read buffer alive.
 */
const COMPACT_RATIO = 4

/**
 * Reassembles frames from bytes read back in arbitrary pieces (a frame may
 * span several reads, and a read may hold several frames).
 */
export class FrameDecoder {
  // Pieces from `#head` on are pending, oldest first.
  #pending: Uint8Array[] = []
  #head = 0
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
      const length = this.#peekUint32()
      if (this.#pendingBytes < FRAME_HEADER_BYTES + length) break
      this.#skip(FRAME_HEADER_BYTES)
      frames.push(this.#take(length))
    }
    this.#compact()
    return frames
  }

  /** The big-endian uint32 at the start of the pending bytes, without consuming it. */
  #peekUint32(): number {
    const first = this.#pending[this.#head] as Uint8Array
    if (first.byteLength >= FRAME_HEADER_BYTES) {
      // `>>> 0` keeps lengths of 2 GiB and up positive.
      return (((first[0] as number) << 24) | ((first[1] as number) << 16) | ((first[2] as number) << 8) | (first[3] as number)) >>> 0
    }
    let value = 0
    let read = 0
    for (let index = this.#head; read < FRAME_HEADER_BYTES; index++) {
      const piece = this.#pending[index] as Uint8Array
      for (let i = 0; i < piece.byteLength && read < FRAME_HEADER_BYTES; i++, read++) {
        value = value * 256 + (piece[i] as number)
      }
    }
    return value
  }

  /** Drops the first `n` pending bytes. */
  #skip(n: number): void {
    let left = n
    while (left > 0) {
      const piece = this.#pending[this.#head] as Uint8Array
      if (piece.byteLength <= left) {
        left -= piece.byteLength
        this.#shift()
      } else {
        this.#pending[this.#head] = piece.subarray(left)
        left = 0
      }
    }
    this.#pendingBytes -= n
  }

  /** Removes and returns the first `n` pending bytes. */
  #take(n: number): Uint8Array {
    if (n === 0) return new Uint8Array(0)
    const first = this.#pending[this.#head] as Uint8Array
    if (first.byteLength >= n) {
      // Fast path: no copy when the bytes sit in one piece.
      const out = first.subarray(0, n)
      if (first.byteLength === n) this.#shift()
      else this.#pending[this.#head] = first.subarray(n)
      this.#pendingBytes -= n
      return out
    }
    const out = new Uint8Array(n)
    let filled = 0
    while (filled < n) {
      const piece = this.#pending[this.#head] as Uint8Array
      const used = Math.min(piece.byteLength, n - filled)
      out.set(used === piece.byteLength ? piece : piece.subarray(0, used), filled)
      filled += used
      if (used === piece.byteLength) this.#shift()
      else this.#pending[this.#head] = piece.subarray(used)
    }
    this.#pendingBytes -= n
    return out
  }

  #shift(): void {
    this.#pending[this.#head] = EMPTY
    this.#head++
    if (this.#head === this.#pending.length) {
      this.#pending = []
      this.#head = 0
    }
  }

  /** Drops consumed slots and copies small remainders out of large buffers. */
  #compact(): void {
    if (this.#head > 0) {
      this.#pending = this.#pending.slice(this.#head)
      this.#head = 0
    }
    for (let i = 0; i < this.#pending.length; i++) {
      const piece = this.#pending[i] as Uint8Array
      if (piece.byteLength * COMPACT_RATIO < piece.buffer.byteLength) {
        // Not `slice()`: on a Node Buffer that's a view, not a copy.
        const copy = new Uint8Array(piece.byteLength)
        copy.set(piece)
        this.#pending[i] = copy
      }
    }
  }
}

const EMPTY = new Uint8Array(0)

/**
 * A codec threw, or returned something unusable, while an object-mode spool
 * was writing records to storage or reading them back. The codec's own error,
 * if any, is the `cause`.
 */
export class SpoolCodecError extends SykeError {
  override readonly code: 'ERR_SYKE_SPOOL_CODEC' = 'ERR_SYKE_SPOOL_CODEC'
  override name = 'SpoolCodecError'
}

/** Encodes one record with `codec`, failing with a `SpoolCodecError` if the codec throws or returns non-bytes. */
export function encodeRecord(codec: Codec, record: unknown): Uint8Array {
  let bytes: unknown
  try {
    bytes = codec.encode(record)
  } catch (cause) {
    throw new SpoolCodecError(`syke: codec failed to encode a record: ${messageOf(cause)}`, { cause })
  }
  if (!isUint8Array(bytes)) {
    throw new SpoolCodecError(`syke: codec.encode() must return a Uint8Array, got ${typeName(bytes)}`)
  }
  return bytes
}

/** Decodes one record with `codec`, failing with a `SpoolCodecError` if the codec throws. */
export function decodeRecord(codec: Codec, bytes: Uint8Array): unknown {
  try {
    return codec.decode(bytes)
  } catch (cause) {
    throw new SpoolCodecError(`syke: codec failed to decode a record: ${messageOf(cause)}`, { cause })
  }
}

/** A `Uint8Array` (or Node `Buffer`) from any realm. */
function isUint8Array(value: unknown): value is Uint8Array {
  return (
    value instanceof Uint8Array ||
    (ArrayBuffer.isView(value) && (value as { [Symbol.toStringTag]?: unknown })[Symbol.toStringTag] === 'Uint8Array')
  )
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function typeName(value: unknown): string {
  if (value === null) return 'null'
  if (typeof value !== 'object') return typeof value
  return value.constructor?.name ?? 'object'
}
