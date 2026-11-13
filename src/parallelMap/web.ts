import { CoreTransformStream } from '../shared/web.ts'
import { ParallelMapCore, type ParallelMapFn, type ParallelMapOptions } from './core.ts'

/**
 * Maps records through `fn` (sync or async) with up to `concurrency` calls in
 * flight, emitting results in input order unless `ordered: false`. A result of
 * `null` or `undefined` is dropped, so it can filter too. If `fn` throws, the
 * stream fails and the other calls' `signal` is aborted; so it is when the
 * stream is aborted or cancelled.
 */
export class ParallelMap<I = unknown, O = unknown> extends CoreTransformStream<I, O> {
  constructor(fn: ParallelMapFn<I, O>, options: ParallelMapOptions) {
    super(new ParallelMapCore<I, O>(fn, options))
  }
}
