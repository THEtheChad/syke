import type { Readable } from 'node:stream'
import { type StreamSource, open, toReadable } from '../shared/combinator/node.ts'
import { ConcatCore, type Lazy } from './core.ts'

/** A source for `concat()`, or a function that opens one when it's reached. */
export type ConcatSource = Lazy<StreamSource>

/**
 * Emits each source in turn, as one object-mode `Readable`. `sources` may be
 * an array, an iterable or an async iterable; a function in it is called (and
 * may return a promise) only when its turn comes, so later sources aren't
 * opened early. An error in any source errors the output and destroys every
 * source still open; destroying the output does the same.
 */
export function concat(sources: Iterable<ConcatSource> | AsyncIterable<ConcatSource>): Readable {
  return toReadable(new ConcatCore(sources, open), { objectMode: true })
}
