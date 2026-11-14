import { CoreTransformStream } from '../shared/web.ts'
import { WatchdogCore, type WatchdogOptions } from './core.ts'

/**
 * Passes data through unchanged, failing the stream with a `WatchdogError`
 * if no chunk arrives for `timeout` ms. Time spent waiting on a slow
 * consumer doesn't count. The clock starts at construction.
 */
export class Watchdog<T = Uint8Array> extends CoreTransformStream<T, T> {
  constructor(options: WatchdogOptions) {
    // A readable high-water mark of 1 (not the default 0) makes `desiredSize`,
    // and so `sink.full`, tell an idle consumer from a slow one.
    super(new WatchdogCore<T>(options), undefined, { highWaterMark: 1 })
  }
}
