import { Transform, type TransformOptions } from 'node:stream'
import { type Sink, type Transformer, settle } from './transformer.ts'

/** Stream options every Node utility passes through to `stream.Transform`. */
export type NodeStreamOptions = Pick<TransformOptions, 'highWaterMark' | 'signal'>

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
      error: (error) => this.destroy(error),
      get full() {
        return stream.readableLength >= stream.readableHighWaterMark
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
