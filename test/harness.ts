import { Readable, type Transform } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { ReadableStream as NodeReadableStream } from 'node:stream/web'

/**
 * Runs the same behaviour tests against a utility's Node and web versions:
 *
 *   describe.each(runtimes)('%s', (runtime) => {
 *     const LengthCheck = pick(runtime, { node: NodeLengthCheck, web: WebLengthCheck })
 *     ...await run(new LengthCheck({ max: 4 }), [bytes])
 *   })
 */
export type Runtime = 'node' | 'web'
export const runtimes: Runtime[] = ['node', 'web']

/** Picks the class (or anything else) for `runtime`. Typed loosely so both share one test body. */
export function pick<T>(runtime: Runtime, impls: { node: T; web: unknown }): T {
  return (runtime === 'node' ? impls.node : impls.web) as T
}

/** Writes `input` through `stream` (a Node Transform or a web TransformStream) and collects the output. */
export async function run<O = unknown>(stream: unknown, input: Iterable<unknown> | AsyncIterable<unknown>): Promise<O[]> {
  const { output, error } = await runSettled<O>(stream, input)
  if (error !== undefined) throw error
  return output
}

/** Like `run()`, but returns what was delivered before any error alongside the error. */
export async function runSettled<O = unknown>(
  stream: unknown,
  input: Iterable<unknown> | AsyncIterable<unknown>,
): Promise<{ output: O[]; error: any }> {
  const output: O[] = []
  try {
    if (isNodeTransform(stream)) {
      await pipeline(Readable.from(input, { objectMode: true }), stream, async (source: AsyncIterable<O>) => {
        for await (const chunk of source) output.push(chunk)
      })
    } else {
      // syke/web is typed against lib.dom, Node's ReadableStream against node:stream/web.
      const pair = stream as Parameters<NodeReadableStream['pipeThrough']>[0]
      for await (const chunk of NodeReadableStream.from(input).pipeThrough(pair)) output.push(chunk as O)
    }
    return { output, error: undefined }
  } catch (error) {
    return { output, error }
  }
}

/** `run()` for byte streams: the output joined into one buffer. */
export async function runBytes(stream: unknown, input: Iterable<Uint8Array> | AsyncIterable<Uint8Array>): Promise<Buffer> {
  return Buffer.concat(await run<Uint8Array>(stream, input))
}

/** Splits `data` into chunks of `size` bytes. */
export function chunk(data: Uint8Array, size: number): Uint8Array[] {
  const chunks: Uint8Array[] = []
  for (let i = 0; i < data.byteLength; i += size) chunks.push(data.subarray(i, i + size))
  return chunks
}

function isNodeTransform(stream: unknown): stream is Transform {
  return typeof (stream as Transform).pipe === 'function'
}
