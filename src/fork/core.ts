import type { OnError, Outlet, Producer, Puller } from '../shared/combinator/core.ts'
import { positiveInt } from '../shared/validate.ts'

interface Branch<T> {
  outlet: Outlet<T> | null
  live: boolean
  /** Resolves this branch's pending pull, if it has one. */
  waiting: (() => void) | null
}

/**
 * Feeds one source to `count` branches. The source is read only when every
 * live branch has asked for more, so it advances at the slowest branch's
 * pace and memory stays bounded by the branches' own stream buffers.
 */
export class ForkCore<T> {
  readonly producers: Producer<T>[]
  readonly #source: Puller<T>
  readonly #branches: Branch<T>[]
  #reading = false
  #finished = false
  /** A failure that came before some branch's output stream was made. */
  #error: { reason: unknown } | null = null

  constructor(open: (onError: OnError) => Puller<T>, count: number) {
    this.#branches = Array.from({ length: count }, () => ({ outlet: null, live: true, waiting: null }))
    this.producers = this.#branches.map((branch) => this.#producer(branch))
    this.#source = open((error) => this.#fail(error))
  }

  #producer(branch: Branch<T>): Producer<T> {
    return {
      start: (outlet) => {
        branch.outlet = outlet
        if (this.#error && branch.live) outlet.error(this.#error.reason)
      },
      pull: () =>
        new Promise<void>((resolve) => {
          branch.waiting = resolve
          this.#maybeRead()
        }),
      cancel: (reason) => {
        branch.live = false
        this.#release(branch)
        if (this.#branches.some((other) => other.live)) this.#maybeRead()
        else if (!this.#finished) {
          // Every branch is gone: nobody wants the source any more.
          this.#finished = true
          this.#source.cancel(reason)
        }
      },
    }
  }

  #maybeRead() {
    if (this.#reading || this.#finished) return
    const live = this.#branches.filter((branch) => branch.live)
    if (live.length === 0 || !live.every((branch) => branch.waiting)) return
    this.#reading = true
    this.#source.next().then(
      (result) => {
        // Still `#reading` while handing the chunk out: a branch that cancels
        // as it gets it (a Node branch given null does) mustn't start the next
        // read while the branches after it still hold this one.
        try {
          if (this.#finished) return
          if (result.done) this.#finished = true
          for (const branch of this.#branches) {
            if (!branch.live) continue
            if (result.done) branch.outlet?.end()
            else branch.outlet?.push(result.value)
            this.#release(branch)
          }
        } finally {
          this.#reading = false
        }
        this.#maybeRead()
      },
      (error) => this.#fail(error),
    )
  }

  #fail(error: unknown) {
    if (this.#finished) return
    this.#finished = true
    this.#error = { reason: error }
    for (const branch of this.#branches) {
      if (!branch.live) continue
      // A branch not started yet gets the error in start().
      branch.outlet?.error(error)
      this.#release(branch)
    }
  }

  #release(branch: Branch<T>) {
    const resolve = branch.waiting
    branch.waiting = null
    resolve?.()
  }
}

export function forkCount(count: number | undefined): number {
  return count === undefined ? 2 : positiveInt('count', count)
}

export function assertBufferFactory(buffer: unknown): void {
  if (buffer !== undefined && typeof buffer !== 'function') {
    throw new TypeError(`syke: buffer must be a function that returns a new stream, got ${typeof buffer}`)
  }
}
