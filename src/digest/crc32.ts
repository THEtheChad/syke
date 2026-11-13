import type { Hasher } from './core.ts'

// Slicing-by-8 tables for CRC-32 (IEEE 802.3, the zlib/gzip/PNG polynomial).
const TABLES: Uint32Array[] = (() => {
  const tables = Array.from({ length: 8 }, () => new Uint32Array(256))
  const base = tables[0] as Uint32Array
  for (let n = 0; n < 256; n++) {
    let c = n
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1
    base[n] = c >>> 0
  }
  for (let n = 0; n < 256; n++) {
    let c = base[n] as number
    for (let t = 1; t < 8; t++) {
      c = (base[c & 0xff] as number) ^ (c >>> 8)
      ;(tables[t] as Uint32Array)[n] = c >>> 0
    }
  }
  return tables
})()

/** Updates a running CRC-32 (start from 0) with `data`. */
export function crc32(data: Uint8Array, value = 0): number {
  const [t0, t1, t2, t3, t4, t5, t6, t7] = TABLES as [
    Uint32Array, Uint32Array, Uint32Array, Uint32Array,
    Uint32Array, Uint32Array, Uint32Array, Uint32Array,
  ]
  let crc = ~value >>> 0
  let i = 0
  const end8 = data.byteLength - (data.byteLength % 8)
  for (; i < end8; i += 8) {
    const lo = (crc ^ ((data[i] as number) | ((data[i + 1] as number) << 8) | ((data[i + 2] as number) << 16) | ((data[i + 3] as number) << 24))) >>> 0
    crc =
      (t7[lo & 0xff] as number) ^
      (t6[(lo >>> 8) & 0xff] as number) ^
      (t5[(lo >>> 16) & 0xff] as number) ^
      (t4[lo >>> 24] as number) ^
      (t3[data[i + 4] as number] as number) ^
      (t2[data[i + 5] as number] as number) ^
      (t1[data[i + 6] as number] as number) ^
      (t0[data[i + 7] as number] as number)
  }
  for (; i < data.byteLength; i++) crc = (t0[(crc ^ (data[i] as number)) & 0xff] as number) ^ (crc >>> 8)
  return ~crc >>> 0
}

/** CRC-32 as a `Hasher`; the digest is the 4-byte big-endian checksum. */
export class Crc32 implements Hasher {
  #value = 0
  readonly #update: (data: Uint8Array, value: number) => number

  /** `update` swaps in a faster implementation, such as Node's `zlib.crc32`. */
  constructor(update: (data: Uint8Array, value: number) => number = crc32) {
    this.#update = update
  }

  update(data: Uint8Array): this {
    this.#value = this.#update(data, this.#value)
    return this
  }

  digest(): Uint8Array {
    const out = new Uint8Array(4)
    new DataView(out.buffer).setUint32(0, this.#value)
    return out
  }
}
