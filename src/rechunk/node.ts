import { CoreTransform, type NodeStreamOptions } from '../shared/node.ts'
import { RechunkCore, type RechunkOptions } from './core.ts'

/**
 * Re-cuts a byte stream into chunks of exactly `size` bytes; only the last
 * may be shorter. Whole chunks inside an input chunk pass as views; bytes
 * are copied only to join pieces of different input chunks.
 *
 * The readable side is in object mode, so the chunks keep their size even
 * when read with `read()` or `for await` (a byte-mode stream would join
 * them). `highWaterMark` stays in bytes on both sides.
 */
export class Rechunk extends CoreTransform<Uint8Array, Uint8Array> {
  constructor(options: RechunkOptions & NodeStreamOptions) {
    const { size, highWaterMark = 64 * 1024, signal } = options
    const core = new RechunkCore({ size }, (n) => Buffer.allocUnsafe(n))
    super(core, {
      signal,
      writableHighWaterMark: highWaterMark,
      readableObjectMode: true,
      // Buffer about highWaterMark bytes of output, and at least one chunk.
      readableHighWaterMark: Math.max(1, Math.floor(highWaterMark / size)),
    })
  }
}
