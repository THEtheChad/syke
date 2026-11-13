import { CoreTransformStream } from '../shared/web.ts'
import { RechunkCore, type RechunkOptions } from './core.ts'

/**
 * Re-cuts a byte stream into chunks of exactly `size` bytes; only the last
 * may be shorter. Whole chunks inside an input chunk pass as views; bytes
 * are copied only to join pieces of different input chunks.
 */
export class Rechunk extends CoreTransformStream<Uint8Array, Uint8Array> {
  constructor(options: RechunkOptions) {
    super(new RechunkCore(options))
  }
}
