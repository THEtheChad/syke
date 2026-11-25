const HEX = Array.from({ length: 256 }, (_, i) => i.toString(16).padStart(2, '0'))

export function toHex(bytes: Uint8Array): string {
  let out = ''
  for (const byte of bytes) out += HEX[byte]
  return out
}

export function fromHex(hex: string): Uint8Array {
  if (hex.length % 2 !== 0 || /[^0-9a-f]/i.test(hex)) {
    throw new TypeError(`syke: not a hex string: '${hex}'`)
  }
  const bytes = new Uint8Array(hex.length / 2)
  for (let i = 0; i < bytes.length; i++) bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16)
  return bytes
}

export function toBase64(bytes: Uint8Array): string {
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary)
}

export function equalBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.byteLength !== b.byteLength) return false
  for (let i = 0; i < a.byteLength; i++) if (a[i] !== b[i]) return false
  return true
}

/** True for a `Uint8Array` (a Node `Buffer` is one), even from another realm. */
export function isUint8Array(value: unknown): value is Uint8Array {
  return (
    value instanceof Uint8Array ||
    (ArrayBuffer.isView(value) && Object.prototype.toString.call(value) === '[object Uint8Array]')
  )
}

/**
 * A chunk of bytes in any of the forms the platform hands them out: an
 * `ArrayBuffer`, a `SharedArrayBuffer` or any view of one (`Uint8Array`,
 * `Buffer`, `DataView`, other typed arrays). What the web utilities that only
 * count or decode bytes (`LengthCheck`, `Lines`, `NdjsonParse`) accept.
 */
export type ByteChunk = ArrayBufferLike | ArrayBufferView

// The byteLength getters check for the real internal slot, so they tell a
// buffer from another realm apart from an object that only looks like one.
const bufferByteLength = byteLengthGetter(globalThis.ArrayBuffer)
const sharedByteLength = byteLengthGetter(globalThis.SharedArrayBuffer)

/** True for an `ArrayBuffer`, `SharedArrayBuffer` or any view of one, even from another realm. */
export function isByteChunk(value: unknown): value is ByteChunk {
  return ArrayBuffer.isView(value) || isBuffer(value, bufferByteLength) || isBuffer(value, sharedByteLength)
}

/** A `Uint8Array` over the same bytes as `source`: `source` itself if it is one, otherwise a view, not a copy. */
export function asUint8Array(source: ByteChunk): Uint8Array {
  if (isUint8Array(source)) return source
  if (ArrayBuffer.isView(source)) return new Uint8Array(source.buffer, source.byteOffset, source.byteLength)
  return new Uint8Array(source)
}

/** The size of a chunk in bytes, failing a byte-counting utility with a clear `TypeError` when it isn't bytes. */
export function byteLengthOf(utility: string, chunk: unknown): number {
  if (!isByteChunk(chunk)) {
    throw new TypeError(`syke: ${utility} takes byte chunks (an ArrayBuffer or a view of one), got ${describeChunk(chunk)}`)
  }
  return chunk.byteLength
}

/**
 * Fails a utility that reads or slices bytes with a clear `TypeError` when
 * handed anything but a `Uint8Array`: indexing or `subarray()` on another
 * typed array works in elements, not bytes, and silently gives wrong results.
 */
export function assertUint8Array(utility: string, chunk: unknown): asserts chunk is Uint8Array {
  if (!isUint8Array(chunk)) {
    throw new TypeError(`syke: ${utility} takes Uint8Array chunks, got ${describeChunk(chunk)}`)
  }
}

export function describeChunk(chunk: unknown): string {
  if (chunk === null) return 'null'
  if (isByteChunk(chunk)) return Object.prototype.toString.call(chunk).slice(8, -1)
  if (typeof chunk === 'object') return (chunk as object).constructor?.name ?? 'object'
  return typeof chunk
}

function byteLengthGetter(constructor: { prototype: object } | undefined): ((this: unknown) => number) | undefined {
  return constructor && Object.getOwnPropertyDescriptor(constructor.prototype, 'byteLength')?.get
}

function isBuffer(value: unknown, byteLength: ((this: unknown) => number) | undefined): boolean {
  if (byteLength === undefined || typeof value !== 'object' || value === null) return false
  try {
    byteLength.call(value)
    return true
  } catch {
    return false
  }
}
