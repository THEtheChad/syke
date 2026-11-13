import { CoreTransform, type NodeStreamOptions } from '../shared/node.ts'
import { BatchCore, type BatchOptions } from './core.ts'

/**
 * Groups records into arrays of up to `size`, for bulk inserts and batched
 * API calls. With `maxWait`, a partial batch is emitted once its first record
 * has waited that many milliseconds. The last partial batch is emitted when
 * the input ends. Both sides are in object mode.
 */
export class Batch<T = unknown> extends CoreTransform<T, T[]> {
  constructor(options: BatchOptions & NodeStreamOptions) {
    const { size, maxWait, ...streamOptions } = options ?? ({} as BatchOptions)
    super(new BatchCore<T>({ size, maxWait }), { ...streamOptions, objectMode: true })
  }
}
