import type { ByteChunk } from '../shared/bytes.ts'
import { CoreTransformStream } from '../shared/web.ts'
import { LengthCheckCore, type LengthCheckOptions } from './core.ts'

/**
 * The chunk type a `LengthCheck<T>` passes on: `Uint8Array` for any
 * `Uint8Array` (or an untyped `any` stream), as in 0.1.0, so its output can be
 * reassigned and chained like any other byte stream; `T` itself for other
 * byte chunks (`ArrayBuffer`, `DataView`, `Uint16Array`, ...).
 */
export type LengthCheckOutput<T> = 0 extends 1 & T ? Uint8Array : T extends Uint8Array ? Uint8Array : T

/**
 * Passes bytes through unchanged, failing the stream with a
 * `LengthCheckError` if it carries more than `max` (or `expected`) bytes, or
 * ends short of `min` (or `expected`). An overrun fails before the offending
 * chunk is passed on; a short stream fails before the consumer sees the end.
 *
 * Takes any `ByteChunk` (an `ArrayBuffer`, `SharedArrayBuffer` or any view of
 * one) and passes each on as it came. `T` is inferred when the check is passed
 * straight to `pipeThrough()`, and defaults to `Uint8Array`. Any `Uint8Array`
 * comes out typed as a plain `Uint8Array`; other chunks keep their type (see
 * `LengthCheckOutput`).
 */
export interface LengthCheck<T extends ByteChunk = Uint8Array> extends CoreTransformStream<T, LengthCheckOutput<T>> {
  /** Bytes that have passed through so far. */
  readonly bytes: number
}

/**
 * `LengthCheck`'s constructor. The non-generic signature is the 0.1.0 one:
 * with two construct signatures, TypeScript compares a subclass against
 * `typeof LengthCheck` with the generic one erased, so
 * `const C: typeof LengthCheck = class extends LengthCheck {}` still compiles.
 */
export interface LengthCheckConstructor {
  new <T extends ByteChunk = Uint8Array>(options: LengthCheckOptions): LengthCheck<T>
  new (options: LengthCheckOptions): LengthCheck
  readonly prototype: LengthCheck<any>
}

// A plain class at runtime. Its chunk types are left to LengthCheckConstructor,
// which gives each instance its own; the core still checks they're bytes.
export const LengthCheck: LengthCheckConstructor = class LengthCheck extends CoreTransformStream<any, any> {
  readonly #core: LengthCheckCore<ByteChunk>

  constructor(options: LengthCheckOptions) {
    const core = new LengthCheckCore<ByteChunk>(options)
    super(core)
    this.#core = core
  }

  /** Bytes that have passed through so far. */
  get bytes(): number {
    return this.#core.bytes
  }
}
