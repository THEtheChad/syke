import { CoreTransform, type NodeStreamOptions, streamOptions } from '../shared/node.ts'
import { CapCore, type CapOptions } from './core.ts'

/**
 * Passes on at most `maxBytes` bytes or `maxItems` records (object mode).
 * Past the cap it fails with a `CapError`, or with `onLimit: 'truncate'`
 * passes on what fits and ends the output. After truncating, it keeps
 * reading and discarding the rest of the input so `pipeline()` can finish
 * normally; Node has no clean way to stop an upstream source early.
 */
export class Cap<T = Uint8Array> extends CoreTransform<T, T> {
  readonly #core: CapCore<T>

  constructor(options: CapOptions & NodeStreamOptions) {
    const { maxBytes, maxItems, onLimit } = options
    const core = new CapCore<T>({ maxBytes, maxItems, onLimit } as CapOptions)
    super(core, { ...streamOptions(options), objectMode: maxItems !== undefined })
    this.#core = core
  }

  /** Bytes passed on so far (byte caps only). */
  get bytes(): number {
    return this.#core.bytes
  }

  /** Chunks (or records) passed on so far. */
  get items(): number {
    return this.#core.items
  }

  /** True once `onLimit: 'truncate'` has cut the stream short. */
  get truncated(): boolean {
    return this.#core.truncated
  }
}
