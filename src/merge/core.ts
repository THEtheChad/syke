import {
  type Opener,
  type Outlet,
  type Producer,
  type Puller,
  assertIterable,
  assertSource,
  isAsyncIterable,
  iterablePuller,
} from '../shared/combinator/core.ts'

type Ready =
  | { lane: Puller<unknown>; result: IteratorResult<unknown, undefined> }
  | { lane: null; result: IteratorResult<unknown, undefined> }

/**
 * Interleaves sources as their chunks arrive. Each source has at most one
 * read in flight, and reads are only issued while the consumer is waiting,
 * so at most one chunk per source is held beyond the output's own buffer.
 *
 * Sources from a plain iterable are opened (watched for errors) at once; an
 * async iterable of sources is read as the output is, so sources can join
 * while it runs.
 */
export class MergeCore<S> implements Producer<unknown> {
  readonly #open: Opener<S>
  /** Open sources that haven't ended. */
  readonly #lanes = new Set<Puller<unknown>>()
  /** Lanes (and the feed) with a read in flight. */
  readonly #pending = new Set<Puller<unknown>>()
  /** Settled reads, taken from `#head` on (shift() would make draining quadratic). */
  #ready: Ready[] = []
  #head = 0
  /** An async iterable of sources still being read, if any. */
  #feed: Puller<S> | null = null
  #outlet: Outlet<unknown> | null = null
  #wake: (() => void) | null = null
  #finished = false
  /** A failure that came before the output stream was made. */
  #error: { reason: unknown } | null = null

  constructor(sources: Iterable<S> | AsyncIterable<S>, open: Opener<S>) {
    assertIterable(sources, 'sources')
    this.#open = open
    if (isAsyncIterable<S>(sources)) {
      this.#feed = iterablePuller(sources)
    } else {
      // Check them all before opening any, so a bad one leaves the rest untouched.
      const list = [...sources]
      for (const source of list) assertSource(source)
      try {
        for (const source of list) this.#add(source)
      } catch (error) {
        // A later source failed to open (the same ReadableStream twice, say):
        // hand the earlier ones back untouched, as if we'd never seen them.
        for (const lane of this.#lanes) lane.release()
        this.#lanes.clear()
        throw error
      }
    }
  }

  start(outlet: Outlet<unknown>): void {
    this.#outlet = outlet
    if (this.#error) outlet.error(this.#error.reason)
  }

  async pull(): Promise<void> {
    while (!this.#finished) {
      const item = this.#take()
      if (item?.lane === null) {
        if (item.result.done) this.#feed = null
        else {
          try {
            this.#add(item.result.value as S)
          } catch (error) {
            this.#fail(error)
          }
        }
      } else if (item) {
        if (!item.result.done) {
          this.#outlet?.push(item.result.value)
          return
        }
        this.#lanes.delete(item.lane)
      } else if (this.#lanes.size === 0 && !this.#feed) {
        this.#finished = true
        this.#outlet?.end()
      } else {
        for (const lane of this.#lanes) this.#request(lane, lane)
        if (this.#feed) this.#request(this.#feed, null)
        await new Promise<void>((resolve) => {
          this.#wake = resolve
        })
      }
    }
  }

  cancel(reason: unknown): void {
    if (this.#finished) return
    this.#finished = true
    this.#cancelAll(reason)
  }

  #add(source: S) {
    const lane: Puller<unknown> = this.#open(source, (error) => {
      if (this.#lanes.has(lane)) this.#fail(error)
    })
    this.#lanes.add(lane)
  }

  #request(puller: Puller<unknown>, lane: Puller<unknown> | null) {
    if (this.#pending.has(puller)) return
    this.#pending.add(puller)
    puller.next().then(
      (result) => {
        this.#pending.delete(puller)
        if (this.#finished) return
        this.#ready.push(lane ? { lane, result } : { lane: null, result })
        this.#notify()
      },
      (error) => this.#fail(error),
    )
  }

  #take(): Ready | undefined {
    if (this.#head === this.#ready.length) return undefined
    const item = this.#ready[this.#head++]
    if (this.#head === this.#ready.length) this.#clearReady()
    return item
  }

  #clearReady() {
    this.#ready = []
    this.#head = 0
  }

  #notify() {
    const wake = this.#wake
    this.#wake = null
    wake?.()
  }

  #fail(error: unknown) {
    if (this.#finished) return
    this.#finished = true
    this.#cancelAll(error)
    if (this.#outlet) this.#outlet.error(error)
    else this.#error = { reason: error }
  }

  #cancelAll(reason: unknown) {
    for (const lane of this.#lanes) lane.cancel(reason)
    this.#lanes.clear()
    this.#feed?.cancel(reason)
    this.#feed = null
    this.#clearReady()
    this.#notify()
  }
}
