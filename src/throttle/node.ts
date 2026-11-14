import { CoreTransform, type NodeStreamOptions } from '../shared/node.ts'
import { ThrottleCore, type ThrottleOptions } from './core.ts'

/**
 * Limits a stream to an average of `bytesPerSecond` bytes or
 * `itemsPerSecond` records (object mode), holding back the source rather
 * than buffering. Chunks are never split.
 */
export class Throttle<T = Uint8Array> extends CoreTransform<T, T> {
  constructor(options: ThrottleOptions & NodeStreamOptions) {
    const { bytesPerSecond, itemsPerSecond, burst, ...streamOptions } = options
    const core = new ThrottleCore<T>({ bytesPerSecond, itemsPerSecond, burst } as ThrottleOptions)
    super(core, { ...streamOptions, objectMode: itemsPerSecond !== undefined })
  }
}
