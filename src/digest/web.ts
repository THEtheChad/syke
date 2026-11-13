import { CoreTransformStream } from '../shared/web.ts'
import { DigestCore, type DigestEncoding, type Hasher } from './core.ts'
import { Crc32 } from './crc32.ts'

export type DigestOptions = {
  /** Fail the stream with a `DigestError` if the digest isn't this: a string in `encoding`, or raw bytes. */
  expected?: string | Uint8Array
  /** How `digest` is encoded, and how a string `expected` is read. Defaults to `'hex'`. */
  encoding?: DigestEncoding
} & (
  | {
      /**
       * A built-in algorithm. Browsers can't hash a stream incrementally
       * (WebCrypto only digests whole buffers), so syke ships CRC-32 and
       * takes anything stronger as a `hasher`.
       */
      algorithm: 'crc32'
      hasher?: never
    }
  | {
      /**
       * An incremental hash, or a promise of one: `sha256.create()` from
       * `@noble/hashes`, or `createSHA256()` from `hash-wasm`.
       */
      hasher: Hasher | Promise<Hasher>
      algorithm?: never
    }
)

/**
 * Passes bytes through unchanged while hashing them. `digest` resolves to the
 * encoded hash once the stream ends. With `expected`, a mismatch fails the
 * stream with a `DigestError` before the consumer sees the end.
 */
export class Digest extends CoreTransformStream<Uint8Array, Uint8Array> {
  /** The encoded digest. Rejects if the stream fails or is cancelled first. */
  readonly digest: Promise<string>

  constructor(options: DigestOptions) {
    const { algorithm, hasher, expected, encoding } = options
    if (hasher === undefined && algorithm?.toLowerCase() !== 'crc32') {
      throw new TypeError(
        `syke: Digest in syke/web supports algorithm 'crc32'; pass a hasher for anything else, got ${String(algorithm)}`,
      )
    }
    const core = new DigestCore({ hasher: hasher ?? new Crc32(), expected, encoding })
    super(core)
    this.digest = core.digest
  }
}
