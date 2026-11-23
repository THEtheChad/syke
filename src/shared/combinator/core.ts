/**
 * Runtime-neutral plumbing for combinators (`fork`, `peek`, `concat`,
 * `merge`): functions that read from sources and produce new readable
 * streams, rather than transforming one stream.
 *
 * - A `Puller` reads one source a chunk at a time, on demand.
 * - A `Producer` drives an output stream: the runtime wrapper calls `pull()`
 *   when the consumer wants data, and `cancel()` if the consumer gives up.
 *
 * No Node or DOM APIs here; `node.ts` and `web.ts` adapt these to streams.
 */

/** Reads one source on demand. */
export interface Puller<T> {
  /** The next chunk, or `done` at the end. Rejects if the source fails. */
  next(): Promise<IteratorResult<T, undefined>>
  /** Stops the source early (cancel or destroy). Safe while `next()` is pending, and idempotent. */
  cancel(reason?: unknown): void
}

/** Called once if a source fails, even while nobody is reading it. */
export type OnError = (error: unknown) => void

/** Where a producer sends its output. */
export interface Outlet<T> {
  push(chunk: T): void
  end(): void
  error(error: unknown): void
}

/** Drives one output stream. */
export interface Producer<T> {
  /** Called once, before any pull, with the outlet this producer writes to. */
  start(outlet: Outlet<T>): void
  /**
   * The consumer wants data. Pushes at least one chunk or ends (or errors)
   * the outlet before settling; rejecting errors the output. Never called
   * again before the previous call settles.
   */
  pull(): Promise<void>
  /** The consumer cancelled or destroyed the output. Not called after `end`/`error`. */
  cancel(reason: unknown): void
}

/** A `Puller` over an iterable or async iterable. */
export function iterablePuller<T>(source: Iterable<T> | AsyncIterable<T>): Puller<T> {
  const iterator = isAsyncIterable<T>(source)
    ? source[Symbol.asyncIterator]()
    : (source as Iterable<T>)[Symbol.iterator]()
  let done = false
  return {
    async next() {
      if (done) return { done: true, value: undefined }
      const result = await iterator.next()
      if (result.done) {
        done = true
        return { done: true, value: undefined }
      }
      return { done: false, value: result.value }
    },
    cancel() {
      if (done) return
      done = true
      // An async generator suspended in an `await` finishes that step first.
      try {
        Promise.resolve(iterator.return?.()).catch(noop)
      } catch {
        // A throwing return() has nothing left for us to clean up.
      }
    },
  }
}

/** A `Puller` over a web `ReadableStream` (anything with `getReader()`). */
export function readerPuller<T>(stream: ReadableStream<T>, onError: OnError): Puller<T> {
  const reader = stream.getReader()
  reader.closed.then(noop, onError)
  return {
    async next() {
      const result = await reader.read()
      return result.done ? { done: true, value: undefined } : { done: false, value: result.value }
    },
    cancel(reason) {
      reader.cancel(reason).catch(noop)
    },
  }
}

/**
 * True if `value` has a callable `[Symbol.asyncIterator]`. Checking the
 * property's type, not just its presence, keeps `{ [Symbol.asyncIterator]:
 * undefined }` from passing.
 */
export function isAsyncIterable<T = unknown>(value: unknown): value is AsyncIterable<T> {
  return typeof (value as AsyncIterable<unknown> | null)?.[Symbol.asyncIterator] === 'function'
}

/** True if `value` has a callable `[Symbol.iterator]`. */
export function isIterable<T = unknown>(value: unknown): value is Iterable<T> {
  return typeof (value as Iterable<unknown> | null)?.[Symbol.iterator] === 'function'
}

export function isReadableStream(value: unknown): value is ReadableStream {
  return typeof (value as ReadableStream | null)?.getReader === 'function'
}

/**
 * Checks `source` is a stream or an iterable of chunks. A string or byte
 * array is iterable too, but almost certainly meant as one chunk, and the
 * runtimes disagree on how to read one, so it's refused.
 */
export function assertSource(source: unknown, name = 'source'): void {
  if (typeof source === 'string' || ArrayBuffer.isView(source)) {
    throw new TypeError(`syke: ${name} must be a stream or an iterable of chunks, got a single chunk; wrap it in an array`)
  }
  if (
    source === null ||
    (typeof source !== 'object' && typeof source !== 'function') ||
    !(isAsyncIterable(source) || isIterable(source) || isReadableStream(source))
  ) {
    throw new TypeError(`syke: ${name} must be a stream or an iterable of chunks, got ${describe(source)}`)
  }
}

/** Opens a source: turns it into a `Puller`, with runtime-specific stream support. */
export type Opener<S> = (source: S, onError: OnError) => Puller<unknown>

/** Checks `source` is an iterable or async iterable (of sources). */
export function assertIterable(source: unknown, name: string): void {
  if (
    source === null ||
    typeof source !== 'object' ||
    !(isAsyncIterable(source) || isIterable(source))
  ) {
    throw new TypeError(`syke: ${name} must be an iterable or async iterable, got ${describe(source)}`)
  }
}

export function describe(value: unknown): string {
  if (value === null) return 'null'
  if (typeof value === 'object') return value.constructor?.name ?? 'object'
  return typeof value
}

export function noop(): void {}
