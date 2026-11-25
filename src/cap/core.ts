import { asUint8Array, byteLengthOf, type ByteChunk } from '../shared/bytes.ts'
import { SykeError } from '../shared/errors.ts'
import type { Sink, Transformer } from '../shared/transformer.ts'
import { nonNegativeInt, oneOf } from '../shared/validate.ts'

export type CapLimitAction = 'error' | 'truncate'

export type CapOptions = {
  /**
   * What to do when the stream goes past the cap: `'error'` (the default)
   * fails it with a `CapError`; `'truncate'` passes on what fits and ends
   * the output.
   */
  onLimit?: CapLimitAction
} & (
  | {
      /** Most bytes to pass on. */
      maxBytes: number
      maxItems?: never
    }
  | {
      /** Most chunks to pass on: records in Node (object mode), chunks of any type on the web. */
      maxItems: number
      maxBytes?: never
    }
)

/** The stream went past its `Cap` with `onLimit: 'error'`. */
export class CapError extends SykeError {
  override readonly code: 'ERR_SYKE_CAP' = 'ERR_SYKE_CAP'
  override name = 'CapError'
  /** The cap that was exceeded. */
  readonly limit: number
  /** What the cap counts. */
  readonly unit: 'bytes' | 'items'

  constructor(limit: number, unit: 'bytes' | 'items') {
    super(`syke: stream exceeds the cap of ${limit} ${unit}`)
    this.limit = limit
    this.unit = unit
  }
}

export class CapCore<T> implements Transformer<T, T> {
  /** Bytes passed on (byte caps only). */
  bytes = 0
  /** Chunks passed on. */
  items = 0
  /** True once input past the cap was discarded by `onLimit: 'truncate'`. */
  truncated = false
  readonly #limit: number
  readonly #unit: 'bytes' | 'items'
  readonly #onLimit: CapLimitAction

  constructor(options: CapOptions) {
    const { maxBytes, maxItems, onLimit } = options
    if ((maxBytes === undefined) === (maxItems === undefined)) {
      throw new TypeError('syke: Cap needs exactly one of maxBytes or maxItems')
    }
    this.#unit = maxBytes === undefined ? 'items' : 'bytes'
    this.#limit =
      maxBytes === undefined ? nonNegativeInt('maxItems', maxItems as number) : nonNegativeInt('maxBytes', maxBytes)
    this.#onLimit = oneOf('onLimit', onLimit ?? 'error', ['error', 'truncate'])
  }

  transform(chunk: T, sink: Sink<T>): void {
    if (this.#unit === 'items') {
      if (this.items >= this.#limit) return this.#overrun(sink)
      this.items++
      sink.push(chunk)
      return
    }
    const size = byteLengthOf('Cap with maxBytes', chunk)
    const room = this.#limit - this.bytes
    if (size <= room) {
      // Any ArrayBuffer or view that fits passes through untouched.
      this.bytes += size
      this.items++
      sink.push(chunk)
      return
    }
    // Fail before passing on any of the chunk that overruns; truncation passes on the part that fits.
    if (this.#onLimit === 'truncate' && room > 0) {
      this.bytes += room
      this.items++
      // Cut in bytes, not elements: a chunk that isn't a Uint8Array gives a Uint8Array view of its first bytes.
      sink.push(asUint8Array(chunk as ByteChunk).subarray(0, room) as T)
    }
    this.#overrun(sink)
  }

  #overrun(sink: Sink<T>): void {
    if (this.#onLimit === 'error') throw new CapError(this.#limit, this.#unit)
    this.truncated = true
    sink.end()
  }
}
