import { createHash } from 'node:crypto'
import * as zlib from 'node:zlib'
import { CoreTransform, type NodeStreamOptions } from '../shared/node.ts'
import { DigestCore, type DigestEncoding, type Hasher } from './core.ts'
import { Crc32 } from './crc32.ts'

export type DigestOptions = NodeStreamOptions & {
  /** Fail the stream with a `DigestError` if the digest isn't this: a string in `encoding`, or raw bytes. */
  expected?: string | Uint8Array
  /** How `digest` is encoded, and how a string `expected` is read. Defaults to `'hex'`. */
  encoding?: DigestEncoding
} & (
    | {
        /** `'crc32'`, or any `crypto.createHash()` algorithm (`'sha256'`, `'md5'`, ...). */
        algorithm: string
        hasher?: never
      }
    | {
        /** Your own incremental hash, or a promise of one. */
        hasher: Hasher | Promise<Hasher>
        algorithm?: never
      }
  )

// zlib.crc32 arrived in Node 22.2; older 22.x falls back to the JS version.
const nativeCrc32 = (zlib as { crc32?: (data: Uint8Array, value?: number) => number }).crc32

/**
 * Passes bytes through unchanged while hashing them. `digest` resolves to the
 * encoded hash once the stream ends. With `expected`, a mismatch fails the
 * stream with a `DigestError` before the consumer sees the end.
 */
export class Digest extends CoreTransform<Uint8Array, Uint8Array> {
  /** The encoded digest. Rejects if the stream fails or is destroyed first. */
  readonly digest: Promise<string>

  constructor(options: DigestOptions) {
    const { algorithm, hasher, expected, encoding, ...streamOptions } = options
    const core = new DigestCore({ hasher: hasher ?? nodeHasher(algorithm), expected, encoding })
    super(core, streamOptions)
    this.digest = core.digest
  }
}

function nodeHasher(algorithm: string | undefined): Hasher {
  if (algorithm === undefined) throw new TypeError('syke: Digest needs an algorithm or a hasher')
  if (algorithm.toLowerCase() === 'crc32') return new Crc32(nativeCrc32)
  return createHash(algorithm)
}
