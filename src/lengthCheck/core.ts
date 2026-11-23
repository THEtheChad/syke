import { assertBytes } from '../shared/bytes.ts'
import { SykeError } from '../shared/errors.ts'
import type { Sink, Transformer } from '../shared/transformer.ts'
import { nonNegativeInt } from '../shared/validate.ts'

export interface LengthCheckOptions {
  /** Exact number of bytes the stream must carry, such as a `Content-Length`. */
  expected?: number
  /** Fewest bytes allowed. Checked when the stream ends. */
  min?: number
  /** Most bytes allowed. Checked as data arrives. */
  max?: number
}

/** The stream carried more or fewer bytes than its `LengthCheck` allows. */
export class LengthCheckError extends SykeError {
  override readonly code: 'ERR_SYKE_LENGTH' = 'ERR_SYKE_LENGTH'
  override name = 'LengthCheckError'
  /** Bytes seen when the check failed (for a long stream, including the chunk that overran). */
  readonly bytes: number

  constructor(message: string, bytes: number) {
    super(message)
    this.bytes = bytes
  }
}

export class LengthCheckCore implements Transformer<Uint8Array, Uint8Array> {
  bytes = 0
  readonly #expected: number | undefined
  readonly #min: number
  readonly #max: number

  constructor({ expected, min, max }: LengthCheckOptions) {
    if (expected === undefined && min === undefined && max === undefined) {
      throw new TypeError('syke: LengthCheck needs at least one of expected, min or max')
    }
    this.#expected = expected === undefined ? undefined : nonNegativeInt('expected', expected)
    this.#min = Math.max(min === undefined ? 0 : nonNegativeInt('min', min), expected ?? 0)
    this.#max = Math.min(
      max === undefined ? Number.POSITIVE_INFINITY : nonNegativeInt('max', max),
      expected ?? Number.POSITIVE_INFINITY,
    )
    if (this.#min > this.#max) {
      throw new RangeError(`syke: LengthCheck allows no length (min ${this.#min} > max ${this.#max})`)
    }
  }

  transform(chunk: Uint8Array, sink: Sink<Uint8Array>): void {
    assertBytes('LengthCheck', chunk)
    this.bytes += chunk.byteLength
    // Fail before passing on the chunk that overruns, so it never reaches the consumer.
    if (this.bytes > this.#max) {
      throw new LengthCheckError(`syke: stream is longer than ${this.#describe()}`, this.bytes)
    }
    sink.push(chunk)
  }

  flush(): void {
    if (this.bytes < this.#min) {
      throw new LengthCheckError(
        `syke: stream ended after ${this.bytes} bytes, short of ${this.#describe()}`,
        this.bytes,
      )
    }
  }

  #describe() {
    if (this.#expected !== undefined) return `the expected ${this.#expected} bytes`
    if (this.bytes > this.#max) return `the maximum of ${this.#max} bytes`
    return `the minimum of ${this.#min} bytes`
  }
}
