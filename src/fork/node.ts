import { type Duplex, type Readable, pipeline } from 'node:stream'
import { noop } from '../shared/combinator/core.ts'
import { type StreamSource, mirror, open, toReadable } from '../shared/combinator/node.ts'
import { ForkCore, assertBufferFactory, forkCount } from './core.ts'

export interface ForkOptions {
  /** Number of branches. Defaults to 2. */
  count?: number
  /**
   * Returns a new duplex to pipe each branch through, such as
   * `() => new Spool()`, so a slow branch buffers there instead of holding
   * back the source (and so every other branch).
   */
  buffer?: () => Duplex
}

/**
 * Splits `source` into `count` branches that each get every chunk. Unlike
 * `ReadableStream#tee()`, memory stays bounded: the source advances at the
 * pace of the slowest branch. Branches share chunk objects; don't mutate them.
 */
export function fork(source: StreamSource, options: ForkOptions = {}): Readable[] {
  const count = forkCount(options.count)
  const { buffer } = options
  assertBufferFactory(buffer)
  // Make the buffers before opening the source, so a bad factory leaks nothing.
  const throughs = buffer && Array.from({ length: count }, () => checkDuplex(buffer()))
  const outputOptions = mirror(source)
  const core = new ForkCore((onError) => open(source, onError), count)
  return core.producers.map((producer, i) => {
    const branch = toReadable(producer, outputOptions)
    const through = throughs?.[i]
    if (!through) return branch
    // Errors reach `through` (pipeline destroys it with them); the callback
    // only stops pipeline reporting them a second time.
    pipeline(branch, through, noop)
    return through
  })
}

function checkDuplex(through: Duplex): Duplex {
  if (typeof through?.write !== 'function' || typeof through.pipe !== 'function') {
    throw new TypeError('syke: buffer() must return a duplex stream')
  }
  return through
}
