import { CoreTransform, type NodeStreamOptions, streamOptions } from '../shared/node.ts'
import { WatchdogCore, type WatchdogOptions } from './core.ts'

/**
 * Passes data through unchanged, failing the stream with a `WatchdogError`
 * if no chunk arrives for `timeout` ms. Time spent waiting on a slow
 * consumer doesn't count. The clock starts at construction.
 */
export class Watchdog<T = Uint8Array> extends CoreTransform<T, T> {
  constructor(options: WatchdogOptions & NodeStreamOptions) {
    const { timeout, firstChunkTimeout, objectMode = false } = options
    super(new WatchdogCore<T>({ timeout, firstChunkTimeout }), { ...streamOptions(options), objectMode })
  }
}
