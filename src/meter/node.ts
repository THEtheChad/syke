import { CoreTransform, type NodeStreamOptions } from '../shared/node.ts'
import { MeterCore, type MeterOptions, type MeterStats } from './core.ts'

/**
 * Passes data through unchanged while counting bytes and chunks, with an
 * optional throttled `onProgress` callback.
 */
export class Meter<T = Uint8Array> extends CoreTransform<T, T> {
  readonly #core: MeterCore<T>

  constructor(options: MeterOptions & NodeStreamOptions = {}) {
    const { onProgress, interval, objectMode = false, ...streamOptions } = options
    const core = new MeterCore<T>({ onProgress, interval })
    super(core, { ...streamOptions, objectMode })
    this.#core = core
  }

  /** Bytes seen so far: byte chunks and strings (as UTF-8) only. */
  get bytes(): number {
    return this.#core.bytes
  }

  /** Chunks (or records) seen so far. */
  get chunks(): number {
    return this.#core.chunks
  }

  /** Milliseconds since the first chunk; stops when the stream ends. */
  get elapsedMs(): number {
    return this.#core.elapsedMs
  }

  /** Average throughput in bytes per second over `elapsedMs`. */
  get bytesPerSecond(): number {
    return this.#core.bytesPerSecond
  }

  /** Average chunks (or records) per second over `elapsedMs`. */
  get chunksPerSecond(): number {
    return this.#core.chunksPerSecond
  }

  /** All of the above as one snapshot. */
  get stats(): MeterStats {
    return this.#core.stats
  }
}
