import { Readable } from 'node:stream'
import type { Runtime } from './harness.ts'

/**
 * Dual-runtime helpers for combinators, which take and return readable
 * streams rather than transforms: instrumented sources, readers and
 * collectors that work on Node `Readable`s and web `ReadableStream`s alike.
 */

export interface ProbeOptions {
  /** Chunks to emit before ending. Defaults to no end. */
  length?: number
  /** Chunk `i`. Defaults to `i` itself. */
  chunk?: (i: number) => unknown
  /** Fail with this error instead of emitting chunk `failAt`. */
  failAt?: number
  error?: Error
  /** Node only: a byte-mode stream. */
  objectMode?: boolean
}

/** A pull-driven source (high-water mark 1) that records how far it has been read. */
export interface Probe {
  stream: any
  /** Times the source has been asked for data. */
  readonly pulls: number
  /** Chunks it has emitted. */
  readonly emitted: number
  /** True once cancelled (web) or destroyed (Node) by the consumer. */
  readonly cancelled: boolean
}

export function probe(runtime: Runtime, options: ProbeOptions = {}): Probe {
  const { length = Number.POSITIVE_INFINITY, chunk = (i) => i, failAt, error = new Error('boom'), objectMode = true } =
    options
  const state = { pulls: 0, emitted: 0, cancelled: false }
  // Returns the next chunk, null at the end; throws at failAt.
  const next = () => {
    state.pulls++
    if (state.emitted === failAt) throw error
    if (state.emitted >= length) return null
    return chunk(state.emitted++)
  }
  let stream: unknown
  if (runtime === 'node') {
    stream = new Readable({
      objectMode,
      highWaterMark: 1,
      read() {
        try {
          this.push(next())
        } catch (e) {
          this.destroy(e as Error)
        }
      },
      destroy(err, done) {
        if (!err && !this.readableEnded) state.cancelled = true
        done(err)
      },
    })
  } else {
    stream = new ReadableStream(
      {
        pull(controller) {
          try {
            const value = next()
            if (value === null) controller.close()
            else controller.enqueue(value)
          } catch (e) {
            controller.error(e)
          }
        },
        cancel() {
          state.cancelled = true
        },
      },
      { highWaterMark: 1 },
    )
  }
  return {
    stream,
    get pulls() {
      return state.pulls
    },
    get emitted() {
      return state.emitted
    },
    get cancelled() {
      return state.cancelled
    },
  }
}

/** Reads a Node `Readable` or web `ReadableStream` one chunk at a time. */
export interface Reader {
  read(): Promise<IteratorResult<any>>
  /** Destroys (Node) or cancels (web) the stream. */
  cancel(reason?: unknown): Promise<void>
  /** A read that stalled in `readUpTo()`, which the next `read()` returns. */
  stalled: Promise<IteratorResult<any>> | null
}

export function reader(stream: any): Reader {
  let next: () => Promise<IteratorResult<any>>
  let cancel: (reason?: unknown) => Promise<void>
  if (typeof stream.getReader === 'function') {
    const r = stream.getReader()
    next = () => r.read()
    cancel = (reason) => r.cancel(reason)
  } else {
    const iterator = stream[Symbol.asyncIterator]()
    next = () => iterator.next()
    cancel = async (reason) => {
      stream.destroy(reason)
      // Let the destroy settle.
      await settle()
    }
  }
  return {
    stalled: null,
    read() {
      const pending = this.stalled ?? next()
      this.stalled = null
      return pending
    },
    cancel(reason) {
      this.stalled?.catch(() => {})
      return cancel(reason)
    },
  }
}

/** Reads up to `n` chunks, stopping early if a read stalls (and keeping it for later). */
export async function readUpTo(r: Reader, n: number): Promise<unknown[]> {
  const out: unknown[] = []
  for (let i = 0; i < n; i++) {
    const pending = r.read()
    const result = await Promise.race([pending, settle().then(() => 'stalled' as const)])
    if (result === 'stalled') {
      r.stalled = pending
      break
    }
    if (result.done) break
    out.push(result.value)
  }
  return out
}

/** Collects every chunk; rejects if the stream errors. */
export async function collect<T = unknown>(stream: any): Promise<T[]> {
  const { output, error } = await collectSettled<T>(stream)
  if (error !== undefined) throw error
  return output
}

/** Collects chunks until the stream ends or errors. */
export async function collectSettled<T = unknown>(stream: any): Promise<{ output: T[]; error: any }> {
  const output: T[] = []
  const r = reader(stream)
  try {
    while (true) {
      const result = await r.read()
      if (result.done) return { output, error: undefined }
      output.push(result.value)
    }
  } catch (error) {
    return { output, error }
  }
}

/** Waits a few turns of the event loop for streams to settle. */
export async function settle(turns = 10): Promise<void> {
  for (let i = 0; i < turns; i++) await new Promise((resolve) => setImmediate(resolve))
}

/** A never-ending async iterable of chunks that records whether it was stopped. */
export function generator(chunk: (i: number) => unknown = (i) => i): AsyncIterable<unknown> & { returned: boolean } {
  const state = {
    returned: false,
    async *[Symbol.asyncIterator]() {
      try {
        for (let i = 0; ; i++) yield chunk(i)
      } finally {
        state.returned = true
      }
    },
  }
  return state
}
