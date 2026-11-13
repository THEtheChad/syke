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

  constructor(open: (onError: OnError) => Puller<T>, count: number) {
    this.#branches = Array.from({ length: count }, () => ({ outlet: null, live: true, waiting: null }))
    this.producers = this.#branches.map((branch) => this.#producer(branch))
    this.#source = open((error) => this.#fail(error))
  }

  #producer(branch: Branch<T>): Producer<T> {
    return {
      start: (outlet) => {
        branch.outlet = outlet
      },
      pull: () =>
        new Promise<void>((resolve) => {
          branch.waiting = resolve
          this.#maybeRead()
        }),
      cancel: () => {
        branch.live = false
        this.#release(branch)
        if (this.#branches.some((other) => other.live)) this.#maybeRead()
        else if (!this.#finished) {
          // Every branch is gone: nobody wants the source any more.
          this.#finished = true
          this.#source.cancel()
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
        this.#reading = false
        if (this.#finished) return
        if (result.done) this.#finished = true
        for (const branch of this.#branches) {
          if (!branch.live) continue
          if (result.done) branch.outlet?.end()
          else branch.outlet?.push(result.value)
          this.#release(branch)
        }
      },
      (error) => this.#fail(error),
    )
  }

  #fail(error: unknown) {
    if (this.#finished) return
    this.#finished = true
    for (const branch of this.#branches) {
      if (!branch.live) continue
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
