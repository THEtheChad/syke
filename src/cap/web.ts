import { CoreTransformStream } from '../shared/web.ts'
import { CapCore, type CapOptions } from './core.ts'

/**
 * Passes on at most `maxBytes` bytes or `maxItems` chunks. Past the cap it
 * fails with a `CapError`, or with `onLimit: 'truncate'` passes on what fits
 * and ends the output, which cancels a piped source.
 */
export class Cap<T = Uint8Array> extends CoreTransformStream<T, T> {
  readonly #core: CapCore<T>

  constructor(options: CapOptions) {
    const core = new CapCore<T>(options)
    super(core)
    this.#core = core
  }

  /** Bytes passed on so far (byte caps only). */
  get bytes(): number {
    return this.#core.bytes
  }

  /** Chunks passed on so far. */
  get items(): number {
    return this.#core.items
  }

  /** True once `onLimit: 'truncate'` has cut the stream short. */
  get truncated(): boolean {
    return this.#core.truncated
  }
}
