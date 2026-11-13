import { type StreamSource, open, toReadableStream } from '../shared/combinator/web.ts'
import { MergeCore } from './core.ts'

/**
 * Interleaves the chunks of every source as they arrive, as one
 * `ReadableStream` that ends when they all have. Sources are only read while
 * the output wants data. `sources` may be an iterable or an async iterable
 * (whose sources join as they arrive). An error in any source errors the
 * output and cancels the rest; cancelling the output does the same.
 */
export function merge<T>(sources: Iterable<StreamSource<T>> | AsyncIterable<StreamSource<T>>): ReadableStream<T> {
  return toReadableStream(new MergeCore(sources, open)) as ReadableStream<T>
}
