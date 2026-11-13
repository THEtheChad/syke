import type { Readable } from 'node:stream'
import { type StreamSource, mirror, open, toReadable } from '../shared/combinator/node.ts'
import { PeekCore, peekBytes } from './core.ts'

export interface PeekResult {
  /** The first bytes of the source (fewer if it's shorter): a copy, safe to keep. */
  head: Uint8Array
  /** The whole source, head included, read on demand. */
  stream: Readable
}

/**
 * Reads the first `bytes` bytes of `source` without consuming them: `head`
 * holds them, and `stream` replays them followed by the rest of the source.
 * Use it to sniff magic bytes before choosing how to read a stream. Rejects
 * if the source fails (or yields something other than bytes) while peeking.
 */
export function peek(source: StreamSource, bytes: number): Promise<PeekResult> {
  peekBytes(bytes)
  const core = new PeekCore((onError) => open(source, onError))
  return core.peek(bytes).then((head) => ({ head, stream: toReadable(core, mirror(source)) }))
}
