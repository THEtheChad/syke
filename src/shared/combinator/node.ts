import { Readable } from 'node:stream'
import {
  type OnError,
  type Outlet,
  type Producer,
  type Puller,
  assertSource,
  isReadableStream,
  iterablePuller,
  readerPuller,
} from './core.ts'

/** What a `syke/node` combinator reads: a Node readable or an (async) iterable of chunks. */
export type StreamSource = Readable | NodeJS.ReadableStream | Iterable<unknown> | AsyncIterable<unknown>

/** Options for a combinator's output `Readable`. */
export interface OutputOptions {
  objectMode?: boolean
  highWaterMark?: number
}

/** Opens a source for reading. Also takes a web `ReadableStream`, which Node supports. */
export function open(source: unknown, onError: OnError): Puller<unknown> {
  assertSource(source)
  if (isNodeReadable(source)) return readablePuller(source, onError)
  if (isReadableStream(source)) return readerPuller(source, onError)
  return iterablePuller(source as Iterable<unknown>)
}

export function isNodeReadable(value: unknown): value is Readable {
  const stream = value as Readable | null
  return (
    typeof stream?.on === 'function' &&
    typeof stream.read === 'function' &&
    typeof stream.destroy === 'function' &&
    typeof stream[Symbol.asyncIterator] === 'function'
  )
}

/**
 * Output options that mirror a single Node source, so a byte stream stays a
 * byte stream. Anything else gets an object-mode output.
 */
export function mirror(source: unknown): OutputOptions {
  if (isNodeReadable(source) && source.readableObjectMode === false) {
    return { objectMode: false, highWaterMark: source.readableHighWaterMark }
  }
  return { objectMode: true }
}

function readablePuller(stream: Readable, onError: OnError): Puller<unknown> {
  // Listening also stops an error before the first read crashing the process.
  stream.on('error', onError)
  const iterator = stream[Symbol.asyncIterator]()
  return {
    async next() {
      const result = await iterator.next()
      return result.done ? { done: true, value: undefined } : { done: false, value: result.value }
    },
    cancel() {
      // A pending next() rejects with a premature close, which the caller ignores.
      if (!stream.destroyed) stream.destroy()
    },
  }
}

/** A Node `Readable` driven by a `Producer`. */
export function toReadable<T>(producer: Producer<T>, options: OutputOptions): Readable {
  let finished = false
  let pulling = false
  let wanted = false

  const readable = new Readable({
    ...options,
    read() {
      wanted = true
      if (!pulling) pump()
    },
    destroy(error, done) {
      if (!finished) {
        finished = true
        producer.cancel(error ?? undefined)
      }
      done(error)
    },
  })

  const outlet: Outlet<T> = {
    push(chunk) {
      if (!finished) readable.push(chunk)
    },
    end() {
      if (finished) return
      finished = true
      readable.push(null)
    },
    error(error) {
      if (finished) return
      finished = true
      readable.destroy(error as Error)
    },
  }

  // Node calls read() again once a push leaves room, possibly while the
  // previous pull is still settling, so pulls are serialized here.
  async function pump() {
    pulling = true
    try {
      while (wanted && !finished) {
        wanted = false
        await producer.pull()
      }
    } catch (error) {
      outlet.error(error)
    } finally {
      pulling = false
    }
  }

  producer.start(outlet)
  return readable
}
