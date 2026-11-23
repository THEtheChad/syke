import { assertBytes } from '../shared/bytes.ts'
import { MAX_TIMEOUT_MS, type Sink, type Transformer } from '../shared/transformer.ts'
import { positiveNumber } from '../shared/validate.ts'

export type ThrottleOptions = {
  /**
   * Bucket size: how far ahead of the rate the stream may run after a pause,
   * in bytes or items. Defaults to one second's worth of the rate.
   */
  burst?: number
} & (
  | {
      /** Average bytes per second to let through. */
      bytesPerSecond: number
      itemsPerSecond?: never
    }
  | {
      /** Average chunks per second: records in Node (object mode), chunks of any type on the web. */
      itemsPerSecond: number
      bytesPerSecond?: never
    }
)

/**
 * A token bucket that allows debt. A chunk waits until the bucket holds its
 * cost (or is full, for a chunk bigger than the bucket), then takes its cost
 * out, possibly leaving the bucket negative for later chunks to wait off.
 * Chunks are never split, and the average rate holds.
 */
export class ThrottleCore<T> implements Transformer<T, T> {
  readonly #perMs: number
  readonly #burst: number
  readonly #items: boolean
  #tokens: number
  #updatedAt: number | undefined
  #timer: ReturnType<typeof setTimeout> | undefined
  #release: (() => void) | undefined
  #closed = false

  constructor(options: ThrottleOptions) {
    const { bytesPerSecond, itemsPerSecond, burst } = options
    if ((bytesPerSecond === undefined) === (itemsPerSecond === undefined)) {
      throw new TypeError('syke: Throttle needs exactly one of bytesPerSecond or itemsPerSecond')
    }
    this.#items = itemsPerSecond !== undefined
    const rate = this.#items
      ? positiveNumber('itemsPerSecond', itemsPerSecond as number)
      : positiveNumber('bytesPerSecond', bytesPerSecond as number)
    this.#perMs = rate / 1000
    this.#burst = burst === undefined ? rate : positiveNumber('burst', burst)
    this.#tokens = this.#burst
  }

  transform(chunk: T, sink: Sink<T>): void | Promise<void> {
    let cost = 1
    if (!this.#items) {
      assertBytes('Throttle with bytesPerSecond', chunk)
      cost = chunk.byteLength
    }
    const need = Math.min(cost, this.#burst)
    this.#refill()
    if (this.#tokens >= need) {
      this.#tokens -= cost
      sink.push(chunk)
      return
    }
    let wait = Math.ceil((need - this.#tokens) / this.#perMs)
    // Holding the promise holds back the source until the chunk may pass.
    return new Promise<void>((resolve) => {
      this.#release = resolve
      // A wait longer than setTimeout can take is served in steps.
      const step = () => {
        if (wait > MAX_TIMEOUT_MS) {
          wait -= MAX_TIMEOUT_MS
          this.#timer = setTimeout(step, MAX_TIMEOUT_MS)
          return
        }
        this.#timer = setTimeout(() => {
          this.#timer = undefined
          this.#release = undefined
          this.#refill()
          this.#tokens -= cost
          sink.push(chunk)
          resolve()
        }, wait)
      }
      step()
    })
  }

  close(): void {
    if (this.#closed) return
    this.#closed = true
    clearTimeout(this.#timer)
    this.#timer = undefined
    // Settle a pending wait without passing its chunk on, so nothing upstream waits on it forever.
    this.#release?.()
    this.#release = undefined
  }

  #refill() {
    const now = performance.now()
    if (this.#updatedAt !== undefined) {
      this.#tokens = Math.min(this.#burst, this.#tokens + (now - this.#updatedAt) * this.#perMs)
    }
    this.#updatedAt = now
  }
}
