import { isByteChunk } from '../shared/bytes.ts'
import type { Sink, Transformer } from '../shared/transformer.ts'

/** A snapshot of a `Meter`'s counters. */
export interface MeterStats {
  /** Bytes seen: byte chunks and strings (as UTF-8) only. */
  bytes: number
  /** Chunks (or records) seen. */
  chunks: number
  /** Milliseconds from the first chunk until now, or until the stream ended. */
  elapsedMs: number
  /** Average `bytes` per second over `elapsedMs`; 0 until time has passed. */
  bytesPerSecond: number
  /** Average `chunks` per second over `elapsedMs`; 0 until time has passed. */
  chunksPerSecond: number
}

export interface MeterOptions {
  /**
   * Called with fresh stats on the first chunk, then at most once every
   * `interval` ms as chunks arrive, and once more when the stream ends.
   * Throwing fails the stream.
   */
  onProgress?: (stats: MeterStats) => void
  /** Minimum milliseconds between `onProgress` calls. Defaults to 1000; 0 reports every chunk. */
  interval?: number
  /**
   * Count records rather than bytes: in Node, puts the stream in object mode.
   * Web streams take any chunk either way.
   */
  objectMode?: boolean
}

export class MeterCore<T> implements Transformer<T, T> {
  bytes = 0
  chunks = 0
  readonly #onProgress: ((stats: MeterStats) => void) | undefined
  readonly #interval: number
  #startedAt: number | undefined
  #endedAt: number | undefined
  #reportedAt = Number.NEGATIVE_INFINITY

  constructor({ onProgress, interval }: MeterOptions) {
    if (onProgress !== undefined && typeof onProgress !== 'function') {
      throw new TypeError('syke: onProgress must be a function')
    }
    if (interval !== undefined && !(typeof interval === 'number' && interval >= 0 && Number.isFinite(interval))) {
      throw new RangeError(`syke: interval must be a non-negative number, got ${interval}`)
    }
    this.#onProgress = onProgress
    this.#interval = interval ?? 1000
  }

  get elapsedMs(): number {
    if (this.#startedAt === undefined) return 0
    return (this.#endedAt ?? performance.now()) - this.#startedAt
  }

  get bytesPerSecond(): number {
    return perSecond(this.bytes, this.elapsedMs)
  }

  get chunksPerSecond(): number {
    return perSecond(this.chunks, this.elapsedMs)
  }

  get stats(): MeterStats {
    const elapsedMs = this.elapsedMs
    return {
      bytes: this.bytes,
      chunks: this.chunks,
      elapsedMs,
      bytesPerSecond: perSecond(this.bytes, elapsedMs),
      chunksPerSecond: perSecond(this.chunks, elapsedMs),
    }
  }

  transform(chunk: T, sink: Sink<T>): void {
    const now = performance.now()
    this.#startedAt ??= now
    this.chunks++
    this.bytes += byteLength(chunk)
    sink.push(chunk)
    if (this.#onProgress && now - this.#reportedAt >= this.#interval) {
      this.#reportedAt = now
      this.#onProgress(this.stats)
    }
  }

  flush(): void {
    this.#end()
    this.#onProgress?.(this.stats)
  }

  close(): void {
    this.#end()
  }

  #end() {
    if (this.#startedAt !== undefined) this.#endedAt ??= performance.now()
  }
}

function perSecond(count: number, elapsedMs: number): number {
  return elapsedMs > 0 ? (count * 1000) / elapsedMs : 0
}

function byteLength(chunk: unknown): number {
  // Any ArrayBuffer, SharedArrayBuffer or view, from any realm, as LengthCheck counts them.
  if (isByteChunk(chunk)) return chunk.byteLength
  if (typeof chunk === 'string') return utf8Length(chunk)
  return 0
}

/** UTF-8 length of a string, without encoding it. */
function utf8Length(text: string): number {
  let length = 0
  for (let i = 0; i < text.length; i++) {
    const code = text.charCodeAt(i)
    if (code < 0x80) length += 1
    else if (code < 0x800) length += 2
    else if (code >= 0xd800 && code <= 0xdbff && i + 1 < text.length) {
      const next = text.charCodeAt(i + 1)
      if (next >= 0xdc00 && next <= 0xdfff) {
        // A surrogate pair: one 4-byte code point.
        length += 4
        i++
      } else length += 3
    } else length += 3
  }
  return length
}
