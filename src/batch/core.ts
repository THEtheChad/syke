import { MAX_TIMEOUT_MS, type Sink, type Transformer } from '../shared/transformer.ts'
import { positiveInt, positiveNumber } from '../shared/validate.ts'

export interface BatchOptions {
  /** Most records per batch. A batch is emitted as soon as it's full. */
  size: number
  /**
   * Emit a partial batch once this many milliseconds have passed since its
   * first record arrived, so a slow trickle of records isn't held back.
   */
  maxWait?: number
}

export class BatchCore<T> implements Transformer<T, T[]> {
  readonly #size: number
  readonly #maxWait: number | undefined
  #batch: T[] = []
  #timer: ReturnType<typeof setTimeout> | undefined
  #sink!: Sink<T[]>

  constructor(options: BatchOptions) {
    const { size, maxWait } = options ?? {}
    if (size === undefined) throw new TypeError('syke: Batch needs a size')
    this.#size = positiveInt('size', size)
    this.#maxWait = maxWait === undefined ? undefined : positiveNumber('maxWait', maxWait)
  }

  start(sink: Sink<T[]>): void {
    this.#sink = sink
  }

  transform(record: T): void {
    this.#batch.push(record)
    if (this.#batch.length >= this.#size) this.#emit()
    else if (this.#batch.length === 1 && this.#maxWait !== undefined) this.#wait(this.#maxWait)
  }

  flush(): void {
    if (this.#batch.length > 0) this.#emit()
  }

  close(): void {
    this.#clearTimer()
    // Don't hold on to records nobody will receive.
    this.#batch = []
  }

  /** Emits the batch after `ms`, in steps if it's longer than setTimeout can take. */
  #wait(ms: number) {
    const step = Math.min(ms, MAX_TIMEOUT_MS)
    this.#timer = setTimeout(() => {
      this.#timer = undefined
      if (ms > step) this.#wait(ms - step)
      else this.#emit()
    }, step)
  }

  #emit() {
    this.#clearTimer()
    const batch = this.#batch
    this.#batch = []
    this.#sink.push(batch)
  }

  #clearTimer() {
    if (this.#timer === undefined) return
    clearTimeout(this.#timer)
    this.#timer = undefined
  }
}
