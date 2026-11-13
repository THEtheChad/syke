import type { Duplex } from 'node:stream'
import type { Runtime } from './harness.ts'

/**
 * Drives a Node Transform or a web TransformStream by hand, for tests that
 * need to interleave writes with timers or watch what's in flight. Output is
 * collected as it arrives; `reading: false` leaves the output unread, to test
 * backpressure.
 */
export interface Driver<O> {
  output: O[]
  /** Writes one chunk. Resolves when the stream has accepted it. */
  write(chunk: unknown): Promise<void>
  /** Ends the input. */
  end(): Promise<void>
  /** Destroys (node) or aborts and cancels (web) the stream. */
  abort(reason?: Error): void
  /** Starts reading the output, if `reading: false` held it back. */
  read(): void
  /** Settles when the output ends or fails; resolves to the error, if any. */
  done: Promise<unknown>
}

export function drive<O>(runtime: Runtime, stream: unknown, { reading = true }: { reading?: boolean } = {}): Driver<O> {
  return runtime === 'node' ? driveNode(stream as Duplex, reading) : driveWeb(stream as TransformStream, reading)
}

function driveNode<O>(stream: Duplex, reading: boolean): Driver<O> {
  const output: O[] = []
  const done = new Promise<unknown>((resolve) => {
    stream.on('error', resolve)
    stream.on('end', () => resolve(undefined))
    stream.on('close', () => resolve(stream.errored ?? undefined))
  })
  const read = () => stream.on('data', (chunk: O) => output.push(chunk))
  if (reading) read()
  return {
    output,
    done,
    read,
    write: (chunk) =>
      new Promise<void>((resolve, reject) => {
        stream.write(chunk, (error) => (error ? reject(error) : resolve()))
      }),
    end: () => new Promise<void>((resolve) => stream.end(resolve)),
    abort: (reason) => stream.destroy(reason),
  }
}

function driveWeb<O>(stream: TransformStream, reading: boolean): Driver<O> {
  const output: O[] = []
  const writer = stream.writable.getWriter()
  const reader = stream.readable.getReader()
  let start!: () => void
  const started = new Promise<void>((resolve) => {
    start = resolve
  })
  if (reading) start()
  const done = (async () => {
    await started
    try {
      for (;;) {
        const { done, value } = await reader.read()
        if (done) return undefined
        output.push(value)
      }
    } catch (error) {
      return error
    }
  })()
  return {
    output,
    done,
    read: start,
    write: (chunk) => writer.write(chunk),
    end: () => writer.close(),
    abort: (reason) => {
      writer.abort(reason).catch(() => {})
      reader.cancel(reason).catch(() => {})
    },
  }
}
