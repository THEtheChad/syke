import type { Sink, Transformer } from '../shared/transformer.ts'
import { positiveInt } from '../shared/validate.ts'

export interface ParallelMapContext {
  /**
   * Aborted when the stream fails (another call threw), is destroyed or is
   * cancelled. Pass it to `fetch`, your database driver and so on.
   */
  signal: AbortSignal
}

/** Maps one item. Returning `null` or `undefined` drops the item. */
export type ParallelMapFn<I, O> = (
  item: I,
  context: ParallelMapContext,
) => O | null | undefined | PromiseLike<O | null | undefined>

export interface ParallelMapOptions {
  /** Most calls to `fn` in flight at once. */
  concurrency: number
  /**
   * Emit results in input order (the default), or as they finish. In order, a
   * finished result waits for every earlier one, and still holds its slot
   * until it's emitted, so memory stays bounded by `concurrency`.
   */
  ordered?: boolean
}

interface Task<O> {
  settled: boolean
  value: O | null | undefined
}

interface Waiter {
  /** True once the transform or flush that's waiting may continue. */
  ready(): boolean
  resolve(): void
  reject(error: unknown): void
}

export class ParallelMapCore<I, O> implements Transformer<I, O> {
  readonly #fn: ParallelMapFn<I, O>
  readonly #concurrency: number
  readonly #ordered: boolean
  readonly #abort = new AbortController()
  /** Calls in flight, plus (in order) finished results waiting for an earlier one. */
  #active = 0
  /** In order: one entry per active slot, oldest first. */
  readonly #queue: Task<O>[] = []
  #waiter: Waiter | undefined
  #failed = false
  #error: unknown
  #closed = false
  #sink!: Sink<O>

  constructor(fn: ParallelMapFn<I, O>, options: ParallelMapOptions) {
    if (typeof fn !== 'function') throw new TypeError('syke: ParallelMap needs a function')
    const { concurrency, ordered = true } = options ?? {}
    if (concurrency === undefined) throw new TypeError('syke: ParallelMap needs a concurrency')
    if (typeof ordered !== 'boolean') throw new TypeError(`syke: ordered must be a boolean, got ${String(ordered)}`)
    this.#fn = fn
    this.#concurrency = positiveInt('concurrency', concurrency)
    this.#ordered = ordered
  }

  start(sink: Sink<O>): void {
    this.#sink = sink
  }

  transform(item: I): void | Promise<void> {
    if (this.#failed) throw this.#error
    this.#launch(item)
    // Accept the next item only once there's a free slot.
    if (this.#active < this.#concurrency) return
    return this.#wait(() => this.#active < this.#concurrency)
  }

  flush(): void | Promise<void> {
    if (this.#failed) throw this.#error
    if (this.#active === 0) return
    return this.#wait(() => this.#active === 0)
  }

  close(): void {
    this.#closed = true
    if (!this.#abort.signal.aborted) this.#abort.abort()
    // Nothing will free a slot now; don't leave a transform or flush hanging.
    const waiter = this.#waiter
    this.#waiter = undefined
    waiter?.resolve()
  }

  #launch(item: I) {
    this.#active++
    const task: Task<O> = { settled: false, value: undefined }
    if (this.#ordered) this.#queue.push(task)
    let result: ReturnType<ParallelMapFn<I, O>>
    try {
      result = this.#fn(item, { signal: this.#abort.signal })
    } catch (error) {
      result = Promise.reject(error)
    }
    Promise.resolve(result).then(
      (value) => this.#finish(task, value),
      (error) => this.#fail(error),
    )
  }

  #finish(task: Task<O>, value: O | null | undefined) {
    if (this.#failed || this.#closed) return
    if (!this.#ordered) {
      this.#active--
      this.#emit(value)
    } else {
      task.settled = true
      task.value = value
      // Emit every finished result at the head of the line.
      while (this.#queue[0]?.settled) {
        const head = this.#queue.shift() as Task<O>
        this.#active--
        this.#emit(head.value)
      }
    }
    if (this.#waiter?.ready()) {
      const waiter = this.#waiter
      this.#waiter = undefined
      waiter.resolve()
    }
  }

  #emit(value: O | null | undefined) {
    if (value !== null && value !== undefined) this.#sink.push(value)
  }

  #fail(error: unknown) {
    if (this.#failed || this.#closed) return
    this.#failed = true
    this.#error = error
    this.#queue.length = 0
    this.#abort.abort(error)
    // Fail through the waiting transform or flush if there is one, else directly.
    const waiter = this.#waiter
    this.#waiter = undefined
    if (waiter) waiter.reject(error)
    else this.#sink.error(error as Error)
  }

  #wait(ready: () => boolean): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.#waiter = { ready, resolve, reject }
    })
  }
}
