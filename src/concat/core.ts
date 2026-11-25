import {
  type Opener,
  type Outlet,
  type Producer,
  type Puller,
  assertIterable,
  assertSource,
  iterablePuller,
  noop,
} from '../shared/combinator/core.ts'

/** A source, or a function that opens one when it's needed. */
export type Lazy<S> = S | (() => S | Promise<S>)

/**
 * Reads each source in turn. A factory isn't called until the sources before
 * it have ended. Sources in an array are opened (watched for errors) up
 * front, so one that fails while waiting its turn fails the output at once,
 * and so they can be cancelled with the rest.
 */
export class ConcatCore<S> implements Producer<unknown> {
  readonly #open: Opener<S>
  readonly #items: Puller<Lazy<S> | Puller<unknown>>
  /** Opened sources that haven't ended. */
  readonly #opened = new Set<Puller<unknown>>()
  #outlet: Outlet<unknown> | null = null
  #current: Puller<unknown> | null = null
  #finished = false
  /** Why the output was cancelled or failed, for sources a factory returns after that. */
  #reason: unknown = undefined
  /** A failure that came before the output stream was made. */
  #error: { reason: unknown } | null = null

  constructor(sources: Iterable<Lazy<S>> | AsyncIterable<Lazy<S>>, open: Opener<S>) {
    assertIterable(sources, 'sources')
    this.#open = open
    if (Array.isArray(sources)) {
      // Check them all before opening any, so a bad one leaves the rest untouched.
      for (const item of sources) if (typeof item !== 'function') assertSource(item)
      let items: (Lazy<S> | Puller<unknown>)[]
      try {
        items = sources.map((item) => (typeof item === 'function' ? item : this.#openSource(item)))
      } catch (error) {
        // A later source failed to open (the same ReadableStream twice, say):
        // hand the earlier ones back untouched, as if we'd never seen them.
        for (const puller of this.#opened) puller.release()
        this.#opened.clear()
        throw error
      }
      this.#items = iterablePuller<Lazy<S> | Puller<unknown>>(items)
    } else {
      // Any other iterable is read lazily, one source per turn (a generator
      // may make them on demand), so nothing is opened here.
      this.#items = iterablePuller(sources as Iterable<Lazy<S>>)
    }
  }

  start(outlet: Outlet<unknown>): void {
    this.#outlet = outlet
    if (this.#error) outlet.error(this.#error.reason)
  }

  async pull(): Promise<void> {
    try {
      while (!this.#finished) {
        if (!this.#current) {
          const next = await this.#items.next()
          if (this.#finished) return
          if (next.done) {
            this.#finished = true
            this.#outlet?.end()
            return
          }
          const current = await this.#resolve(next.value)
          if (!current || this.#finished) return
          this.#current = current
        }
        const result = await this.#current.next()
        if (this.#finished) return
        if (!result.done) {
          this.#outlet?.push(result.value)
          return
        }
        this.#opened.delete(this.#current)
        this.#current = null
      }
    } catch (error) {
      this.#fail(error)
    }
  }

  cancel(reason: unknown): void {
    if (this.#finished) return
    this.#finished = true
    this.#reason = reason
    this.#cancelOpened(reason)
  }

  /** Opens an item, or returns null if the output finished while a factory ran. */
  async #resolve(item: Lazy<S> | Puller<unknown>): Promise<Puller<unknown> | null> {
    if (this.#opened.has(item as Puller<unknown>)) return item as Puller<unknown>
    const source = typeof item === 'function' ? await (item as () => S | Promise<S>)() : (item as S)
    if (this.#finished) {
      // Cancelled (or failed) while the factory ran: release what it returned
      // without making it part of the output.
      try {
        this.#open(source, noop).cancel(this.#reason)
      } catch {
        // Not a source we can open, so nothing to release.
      }
      return null
    }
    return this.#openSource(source)
  }

  #openSource(source: S): Puller<unknown> {
    const puller: Puller<unknown> = this.#open(source, (error) => {
      if (this.#opened.has(puller)) this.#fail(error)
    })
    this.#opened.add(puller)
    return puller
  }

  #fail(error: unknown) {
    if (this.#finished) return
    this.#finished = true
    this.#reason = error
    this.#cancelOpened(error)
    if (this.#outlet) this.#outlet.error(error)
    else this.#error = { reason: error }
  }

  #cancelOpened(reason: unknown) {
    for (const puller of this.#opened) puller.cancel(reason)
    this.#opened.clear()
    this.#current = null
    // Stops a generator of sources; an array's remaining sources were opened above.
    this.#items.cancel(reason)
  }
}
