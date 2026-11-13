import { CoreTransformStream } from '../shared/web.ts'
import { BatchCore, type BatchOptions } from './core.ts'

/**
 * Groups records into arrays of up to `size`, for bulk inserts and batched
 * API calls. With `maxWait`, a partial batch is emitted once its first record
 * has waited that many milliseconds. The last partial batch is emitted when
 * the input ends.
 */
export class Batch<T = unknown> extends CoreTransformStream<T, T[]> {
  constructor(options: BatchOptions) {
    super(new BatchCore<T>(options))
  }
}
