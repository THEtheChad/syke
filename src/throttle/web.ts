import { CoreTransformStream } from '../shared/web.ts'
import { ThrottleCore, type ThrottleOptions } from './core.ts'

/**
 * Limits a stream to an average of `bytesPerSecond` bytes or
 * `itemsPerSecond` chunks, holding back the source rather than buffering.
 * Chunks are never split.
 */
export class Throttle<T = Uint8Array> extends CoreTransformStream<T, T> {
  constructor(options: ThrottleOptions) {
    super(new ThrottleCore<T>(options))
  }
}
