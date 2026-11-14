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

/** What a `syke/web` combinator reads: a `ReadableStream` or an (async) iterable of chunks. */
export type StreamSource<T = unknown> = ReadableStream<T> | Iterable<T> | AsyncIterable<T>

/** Opens a source for reading. */
export function open(source: unknown, onError: OnError): Puller<unknown> {
  assertSource(source)
  if (isReadableStream(source)) return readerPuller(source, onError)
  return iterablePuller(source as Iterable<unknown>)
}

/** A `ReadableStream` driven by a `Producer`. */
export function toReadableStream<T>(producer: Producer<T>): ReadableStream<T> {
  let finished = false
  return new ReadableStream<T>({
    start(controller) {
      const outlet: Outlet<T> = {
        push(chunk) {
          if (!finished) controller.enqueue(chunk)
        },
        end() {
          if (finished) return
          finished = true
          controller.close()
        },
        error(error) {
          if (finished) return
          finished = true
          controller.error(error)
        },
      }
      producer.start(outlet)
    },
    pull() {
      if (!finished) return producer.pull()
    },
    cancel(reason) {
      if (finished) return
      finished = true
      producer.cancel(reason)
    },
  })
}
