import { assertUint8Array, equalBytes, fromHex, isUint8Array, toBase64, toHex } from '../shared/bytes.ts'
import { SykeError } from '../shared/errors.ts'
import { type Sink, type Transformer, toFailure } from '../shared/transformer.ts'
import { oneOf } from '../shared/validate.ts'

/**
 * An incremental hash. Node's `crypto.createHash()`, `@noble/hashes`
 * (`sha256.create()`) and `hash-wasm` (`createSHA256()`) all fit. A string
 * digest is read as hex.
 */
export interface Hasher {
  update(data: Uint8Array): unknown
  digest(): Uint8Array | string
}

export type DigestEncoding = 'hex' | 'base64'

export interface DigestCoreOptions {
  hasher: Hasher | Promise<Hasher>
  /** Fail the stream if the digest isn't this: a string in `encoding`, or raw bytes. */
  expected?: string | Uint8Array
  /** How `digest` is encoded, and how a string `expected` is read. Defaults to `'hex'`. */
  encoding?: DigestEncoding
}

/** The stream's digest didn't match `expected`. */
export class DigestError extends SykeError {
  override readonly code: 'ERR_SYKE_DIGEST' = 'ERR_SYKE_DIGEST'
  override name = 'DigestError'
  readonly expected: string
  readonly actual: string

  constructor(expected: string, actual: string) {
    super(`syke: digest mismatch: expected ${expected}, got ${actual}`)
    this.expected = expected
    this.actual = actual
  }
}

export class DigestCore implements Transformer<Uint8Array, Uint8Array> {
  readonly digest: Promise<string>
  #hasher: Hasher | Promise<Hasher>
  readonly #expected: string | Uint8Array | undefined
  readonly #encoding: DigestEncoding
  #resolve!: (digest: string) => void
  #reject!: (error: Error) => void
  #settled = false

  constructor({ hasher, expected, encoding }: DigestCoreOptions) {
    if (isPromise(hasher)) {
      // The stream may fail or be destroyed before it ever awaits the hasher;
      // its rejection then mustn't go unhandled. Awaiting it later still sees the error.
      const ready = Promise.resolve(hasher)
      ready.catch(() => {})
      this.#hasher = ready
    } else if (typeof hasher?.update !== 'function' || typeof hasher.digest !== 'function') {
      throw new TypeError('syke: hasher must have update() and digest() methods, or be a promise of one')
    } else this.#hasher = hasher
    if (expected !== undefined && typeof expected !== 'string' && !isUint8Array(expected)) {
      throw new TypeError(`syke: expected must be a string or a Uint8Array, got ${typeof expected}`)
    }
    this.#expected = expected
    this.#encoding = oneOf('encoding', encoding ?? 'hex', ['hex', 'base64'])
    this.digest = new Promise<string>((resolve, reject) => {
      this.#resolve = resolve
      this.#reject = reject
    })
    // Nobody has to await `digest`; a failed stream mustn't become an unhandled rejection.
    this.digest.catch(() => {})
  }

  transform(chunk: Uint8Array, sink: Sink<Uint8Array>): void | Promise<void> {
    try {
      assertUint8Array('Digest', chunk)
    } catch (error) {
      this.#fail(error)
    }
    const hasher = this.#hasher
    if (isPromise(hasher)) {
      return hasher.then((ready) => {
        this.#hasher = ready
        this.#update(ready, chunk)
        sink.push(chunk)
      }, this.#fail)
    }
    this.#update(hasher, chunk)
    sink.push(chunk)
  }

  async flush(): Promise<void> {
    let actual: string
    let bytes: Uint8Array
    try {
      const raw = (await this.#hasher).digest()
      bytes = typeof raw === 'string' ? fromHex(raw) : raw
      actual = this.#encoding === 'hex' ? toHex(bytes) : toBase64(bytes)
    } catch (error) {
      return this.#fail(error)
    }
    const expected = this.#expected
    if (expected !== undefined) {
      const matches =
        typeof expected === 'string'
          ? this.#encoding === 'hex'
            ? expected.toLowerCase() === actual
            : expected === actual
          : equalBytes(expected, bytes)
      if (!matches) {
        const shown =
          typeof expected === 'string' ? expected : this.#encoding === 'hex' ? toHex(expected) : toBase64(expected)
        return this.#fail(new DigestError(shown, actual))
      }
    }
    this.#settle(() => this.#resolve(actual))
  }

  close(): void {
    this.#settle(() => this.#reject(new Error('syke: stream closed before its digest was computed')))
  }

  #update(hasher: Hasher, chunk: Uint8Array) {
    try {
      hasher.update(chunk)
    } catch (error) {
      this.#fail(error)
    }
  }

  /**
   * Rejects `digest` with the error that failed the stream (not a generic
   * one), then throws it. A falsy error is wrapped first, so both see the
   * same `Error`.
   */
  readonly #fail = (error: unknown): never => {
    const failure = toFailure(error)
    this.#settle(() => this.#reject(failure))
    throw failure
  }

  #settle(settle: () => void) {
    if (this.#settled) return
    this.#settled = true
    settle()
  }
}

function isPromise<T>(value: T | Promise<T>): value is Promise<T> {
  return typeof (value as Promise<T>)?.then === 'function'
}
