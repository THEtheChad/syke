import type { Readable } from 'node:stream'
import { type StreamSource, open, toReadable } from '../shared/combinator/node.ts'
import { MergeCore } from './core.ts'

/**
 * Interleaves the chunks of every source as they arrive, as one object-mode
 * `Readable` that ends when they all have. Sources are only read while the
 * output wants data. `sources` may be an iterable or an async iterable (whose
 * sources join as they arrive). An error in any source errors the output and
 * destroys the rest; destroying the output does the same.
 */
export function merge(sources: Iterable<StreamSource> | AsyncIterable<StreamSource>): Readable {
  return toReadable(new MergeCore(sources, open), { objectMode: true })
}
