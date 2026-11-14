import { SykeError } from '../shared/errors.ts'
import type { Sink, Transformer } from '../shared/transformer.ts'
import { positiveNumber } from '../shared/validate.ts'

export interface WatchdogOptions {
  /** Fail if no chunk arrives for this many milliseconds while downstream is ready for one. */
  timeout: number
  /** Milliseconds to wait for the first chunk, counted from construction. Defaults to `timeout`. */
  firstChunkTimeout?: number
  /** In Node, puts the stream in object mode. Web streams take any chunk anyway. */
  objectMode?: boolean
}

/** No chunk arrived within a `Watchdog`'s timeout. */
export class WatchdogError extends SykeError {
  override readonly code: 'ERR_SYKE_TIMEOUT' = 'ERR_SYKE_TIMEOUT'
  override name = 'WatchdogError'
  /** The timeout that expired, in milliseconds. */
  readonly timeout: number
  /** True if it was the wait for the first chunk that expired. */
  readonly firstChunk: boolean

  constructor(timeout: number, firstChunk: boolean) {
    super(
      firstChunk
        ? `syke: no data arrived within ${timeout} ms`
        : `syke: stream stalled: no data for ${timeout} ms`,
    )
    this.timeout = timeout
    this.firstChunk = firstChunk
  }
}

export class WatchdogCore<T> implements Transformer<T, T> {
  readonly #timeout: number
  readonly #firstChunkTimeout: number
  #sink!: Sink<T>
  #timer: ReturnType<typeof setTimeout> | undefined
  #closed = false
  #gotChunk = false
  // When the idle clock last restarted: a chunk arrived, or downstream was seen full.
  #idleSince = 0

  constructor({ timeout, firstChunkTimeout }: WatchdogOptions) {
    this.#timeout = positiveNumber('timeout', timeout)
    this.#firstChunkTimeout =
      firstChunkTimeout === undefined ? this.#timeout : positiveNumber('firstChunkTimeout', firstChunkTimeout)
  }

  start(sink: Sink<T>): void {
    this.#sink = sink
    this.#idleSince = performance.now()
    this.#arm(this.#firstChunkTimeout)
  }

  transform(chunk: T, sink: Sink<T>): void {
    this.#idleSince = performance.now()
    if (!this.#gotChunk) {
      this.#gotChunk = true
      // The first-chunk timer may be longer than `timeout`; switch to the idle timer.
      this.#arm(this.#timeout)
    }
    sink.push(chunk)
  }

  close(): void {
    this.#closed = true
    clearTimeout(this.#timer)
    this.#timer = undefined
  }

  // One timer, re-armed only when it fires, so chunks don't each reset a timer.
  #arm(ms: number) {
    clearTimeout(this.#timer)
    this.#timer = this.#closed ? undefined : setTimeout(this.#check, ms)
  }

  #check = () => {
    this.#timer = undefined
    if (this.#closed) return
    const now = performance.now()
    if (this.#sink.full) {
      // A slow consumer isn't a stalled source: restart the clock, and look
      // again soon so a source that stalls once downstream drains is caught
      // within a quarter of the timeout.
      this.#idleSince = now
      return this.#arm(this.#timeout / 4)
    }
    const limit = this.#gotChunk ? this.#timeout : this.#firstChunkTimeout
    const remaining = this.#idleSince + limit - now
    if (remaining > 0) return this.#arm(remaining)
    this.close()
    this.#sink.error(new WatchdogError(limit, !this.#gotChunk))
  }
}
