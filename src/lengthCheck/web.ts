import { CoreTransformStream } from '../shared/web.ts'
import { LengthCheckCore, type LengthCheckOptions } from './core.ts'

/**
 * Passes bytes through unchanged, failing the stream with a
 * `LengthCheckError` if it carries more than `max` (or `expected`) bytes, or
 * ends short of `min` (or `expected`). An overrun fails before the offending
 * chunk is passed on; a short stream fails before the consumer sees the end.
 */
export class LengthCheck extends CoreTransformStream<Uint8Array, Uint8Array> {
  readonly #core: LengthCheckCore

  constructor(options: LengthCheckOptions) {
    const core = new LengthCheckCore(options)
    super(core)
    this.#core = core
  }

  /** Bytes that have passed through so far. */
  get bytes(): number {
    return this.#core.bytes
  }
}
