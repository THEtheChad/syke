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

/** Fails a byte-oriented utility with a clear `TypeError` when handed anything but bytes. */
export function assertBytes(utility: string, chunk: unknown): asserts chunk is Uint8Array {
  if (!isUint8Array(chunk)) {
    throw new TypeError(`syke: ${utility} takes Uint8Array chunks, got ${describeChunk(chunk)}`)
  }
}

function describeChunk(chunk: unknown): string {
  if (chunk === null) return 'null'
  if (ArrayBuffer.isView(chunk) || chunk instanceof ArrayBuffer) {
    return Object.prototype.toString.call(chunk).slice(8, -1)
  }
  if (typeof chunk === 'object') return (chunk as object).constructor?.name ?? 'object'
  return typeof chunk
}
