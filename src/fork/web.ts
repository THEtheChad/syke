import type { Puller } from '../shared/combinator/core.ts'
import { type StreamSource, open, toReadableStream } from '../shared/combinator/web.ts'
import { ForkCore, assertBufferFactory, forkCount } from './core.ts'

export interface ForkOptions<T = unknown> {
  /** Number of branches. Defaults to 2. */
  count?: number
  /**
   * Returns a new `{ writable, readable }` pair (such as a `TransformStream`)
   * to pipe each branch through, so a slow branch buffers there instead of
   * holding back the source (and so every other branch).
   */
  buffer?: () => ReadableWritablePair<T, T>
}

/**
 * Splits `source` into `count` branches that each get every chunk. Unlike
 * `ReadableStream#tee()`, memory stays bounded: the source advances at the
 * pace of the slowest branch. Branches share chunk objects; don't mutate them.
 */
export function fork<T>(source: StreamSource<T>, options: ForkOptions<T> = {}): ReadableStream<T>[] {
  const count = forkCount(options.count)
  const { buffer } = options
  assertBufferFactory(buffer)
  // Make the buffers before opening the source, so a bad factory leaks nothing.
  const pairs = buffer && Array.from({ length: count }, () => checkPair(buffer()))
  const core = new ForkCore((onError) => open(source, onError) as Puller<T>, count)
  return core.producers.map((producer, i) => {
    const branch = toReadableStream(producer)
    const pair = pairs?.[i]
    return pair ? branch.pipeThrough(pair) : branch
  })
}

function checkPair<T>(pair: ReadableWritablePair<T, T>): ReadableWritablePair<T, T> {
  if (typeof pair?.writable?.getWriter !== 'function' || typeof pair.readable?.getReader !== 'function') {
    throw new TypeError('syke: buffer() must return a { writable, readable } pair')
  }
  return pair
}
