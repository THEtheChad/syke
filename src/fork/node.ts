import { type Duplex, type Readable, finished } from 'node:stream'
import { assertSource } from '../shared/combinator/core.ts'
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
  assertSource(source)
  const throughs: Duplex[] = []
  let core: ForkCore<unknown>
  try {
    // Make the buffers before opening the source, so a bad factory leaves the source untouched.
    if (buffer) for (let i = 0; i < count; i++) throughs.push(checkDuplex(buffer()))
    core = new ForkCore((onError) => open(source, onError), count)
  } catch (error) {
    // Don't leak the buffers already made (a Spool holds a file).
    for (const through of throughs) through.destroy()
    throw error
  }
  const outputOptions = mirror(source)
  return core.producers.map((producer, i) => {
    const branch = toReadable(producer, outputOptions)
    const through = throughs[i]
    if (!through) return branch
    connect(branch, through)
    return through
  })
}

/**
 * Pipes `branch` into `through`, tying their lifetimes together: an error in
 * `branch` destroys `through` with it, and `through` ending or being
 * destroyed destroys `branch` (detaching it from the fork), with the same
 * error if it had one.
 *
 * Unlike `pipeline()`, this adds no `'error'` listener of its own to
 * `through`, so its errors reach the caller as they would from any stream:
 * `through.pipe(dest)` with no error handler throws rather than stalling.
 */
function connect(branch: Readable, through: Duplex): void {
  // The branch is ours, so only this listener sees its errors.
  branch.on('error', (error) => through.destroy(error))
  branch.pipe(through)
  finished(through, { error: false }, () => {
    // Pass on why `through` was destroyed, so a source cancelled because of it
    // gets the reason. Not finished()'s own error: a plain destroy() isn't a
    // failure.
    if (!branch.destroyed) branch.destroy(through.errored ?? undefined)
  })
}

function checkDuplex(through: Duplex): Duplex {
  if (typeof through?.write !== 'function' || typeof through.pipe !== 'function') {
    throw new TypeError('syke: buffer() must return a duplex stream')
  }
  return through
}
