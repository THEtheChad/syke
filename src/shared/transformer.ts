/**
 * The runtime-neutral heart of a syke transform. Each utility implements its
 * logic once as a `Transformer`, then wraps it in a Node `Transform`
 * (`CoreTransform`, in `node.ts`) and a web `TransformStream`
 * (`CoreTransformStream`, in `web.ts`), so the two versions can't drift.
 *
 * A transformer must not touch Node or DOM APIs: no `Buffer`, `process`,
 * `node:*` imports or `window`. Byte data is `Uint8Array` (a Node `Buffer` is
 * one, so Node input works unchanged).
 */
export interface Transformer<I, O> {
  /** Called once, before any chunk, with the sink this stream writes to. */
  start?(sink: Sink<O>): void
  /**
   * Handles one input chunk. Returning a promise holds back the next chunk
   * (and so the source) until it settles. Throwing or rejecting fails the
   * stream.
   */
  transform(chunk: I, sink: Sink<O>): void | Promise<void>
  /** Called after the last chunk, before the output ends. */
  flush?(sink: Sink<O>): void | Promise<void>
  /**
   * Releases timers and other resources. Called exactly once, however the
   * stream finishes: flushed, errored, destroyed or cancelled.
   */
  close?(): void
}

/** Where a transformer sends its output. */
export interface Sink<O> {
  /** Sends a chunk downstream. Ignored once the output has ended. */
  push(chunk: O): void
  /**
   * Ends the output early. Later input is discarded: a web stream closes its
   * writable side (which cancels a piped source); a Node stream keeps
   * accepting writes but drops them.
   */
  end(): void
  /** Fails the stream from outside `transform` or `flush` (a timer, say). */
  error(error: Error): void
  /**
   * True while downstream has all it asked for (backpressure is applied).
   *
   * On the web this reads the readable side's `desiredSize`. With the default
   * readable high-water mark of 0 that is never positive, so `full` is always
   * true and can't tell an idle consumer from a slow one. A transformer that
   * relies on `full` must give its `CoreTransformStream` a readable strategy
   * with `highWaterMark >= 1` (as `Watchdog` does).
   */
  readonly full: boolean
}

/**
 * Longest delay `setTimeout` honours (about 24.8 days). Engines store the
 * delay as a signed 32-bit integer and fire a longer one after 1 ms instead,
 * so a longer wait must be split into steps of at most this.
 */
export const MAX_TIMEOUT_MS = 0x7fff_ffff

/**
 * Makes `error` safe to report as a failure. Node treats any falsy error
 * (`undefined`, `null`, `0`, `''`, `false`, `NaN`) handed to a stream
 * callback or `destroy()` as success, so a falsy one is wrapped in an `Error`
 * that keeps the original value as its `cause`. Anything truthy passes through.
 */
export function toFailure(error: unknown): Error {
  if (error) return error as Error
  return new Error(`syke: stream failed with ${describeValue(error)}`, { cause: error })
}

function describeValue(value: unknown): string {
  if (typeof value === 'string') return `'${value}'`
  return String(value)
}

/** Runs `step` and reports its outcome, sync or async, to `done`; a failure is always truthy. */
export function settle(
  step: () => void | Promise<void>,
  done: (error?: Error | null) => void,
): void {
  let result: void | Promise<void>
  try {
    result = step()
  } catch (error) {
    done(toFailure(error))
    return
  }
  if (result) result.then(() => done(), (error: unknown) => done(toFailure(error)))
  else done()
}
