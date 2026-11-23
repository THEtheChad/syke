import { Transform, type TransformOptions } from 'node:stream'
import { type Sink, type Transformer, settle, toFailure } from './transformer.ts'

/** Stream options every Node utility passes through to `stream.Transform`. */
export type NodeStreamOptions = Pick<TransformOptions, 'highWaterMark' | 'signal'>

/**
 * Picks just the `NodeStreamOptions` out of a caller's options, so a stray
 * property at runtime (`objectMode`, `autoDestroy`, `transform`, ...) can't
 * reach `stream.Transform` and change how the utility behaves.
 */
export function streamOptions(options: NodeStreamOptions | undefined): NodeStreamOptions {
  const picked: NodeStreamOptions = {}
  if (options?.highWaterMark !== undefined) picked.highWaterMark = options.highWaterMark
  if (options?.signal !== undefined) picked.signal = options.signal
  return picked
}

/** A Node `Transform` driven by a runtime-neutral `Transformer`. */
export class CoreTransform<I, O> extends Transform {
  readonly #core: Transformer<I, O>
  readonly #sink: Sink<O>
  #ended = false
  #closed = false

  constructor(core: Transformer<I, O>, options?: TransformOptions) {
    super(options)
    this.#core = core
    const stream = this
    this.#sink = {
      push: (chunk) => {
        if (!this.#ended && !this.destroyed) this.push(chunk)
      },
      end: () => {
        if (this.#ended) return
        this.#ended = true
        this.push(null)
        this.#close()
      },
      error: (error) => this.destroy(toFailure(error)),
      get full() {
        // With a high-water mark of 0 an empty buffer is not "full": only
        // output that's actually waiting for a reader is backpressure.
        return stream.readableLength >= Math.max(1, stream.readableHighWaterMark)
      },
    }
    core.start?.(this.#sink)
  }

  override _transform(chunk: I, _encoding: BufferEncoding, done: (error?: Error | null) => void): void {
    if (this.#ended) return done()
    settle(() => this.#core.transform(chunk, this.#sink), done)
  }

  override _flush(done: (error?: Error | null) => void): void {
    if (this.#ended) return done()
    settle(
      () => this.#core.flush?.(this.#sink),
      (error) => {
        this.#close()
        done(error)
      },
    )
  }

  override _destroy(error: Error | null, done: (error?: Error | null) => void): void {
    this.#close()
    done(error)
  }

  #close() {
    if (this.#closed) return
    this.#closed = true
    this.#core.close?.()
  }
}
