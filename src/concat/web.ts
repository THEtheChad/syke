import { type StreamSource, open, toReadableStream } from '../shared/combinator/web.ts'
import { ConcatCore, type Lazy } from './core.ts'

/** A source for `concat()`, or a function that opens one when it's reached. */
export type ConcatSource<T = unknown> = Lazy<StreamSource<T>>

/**
 * Emits each source in turn, as one `ReadableStream`. `sources` may be an
 * array, an iterable or an async iterable; a function in it is called (and
 * may return a promise) only when its turn comes, so later sources aren't
 * opened early. An error in any source errors the output and cancels every
 * source still open; cancelling the output does the same.
 */
export function concat<T>(sources: Iterable<ConcatSource<T>> | AsyncIterable<ConcatSource<T>>): ReadableStream<T> {
  return toReadableStream(new ConcatCore(sources, open)) as ReadableStream<T>
}
