import type { Transform } from 'node:stream'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Watchdog as NodeWatchdog, WatchdogError } from '../../src/node.ts'
import { Watchdog as WebWatchdog, WatchdogError as WebWatchdogError } from '../../src/web.ts'
import { pick, runtimes, type Runtime } from '../harness.ts'

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] })
})
afterEach(() => {
  vi.useRealTimers()
})

/** Lets stream internals (nextTick, setImmediate, promises) catch up. */
const tick = () => new Promise((resolve) => setImmediate(resolve))
const advance = async (ms: number) => {
  await vi.advanceTimersByTimeAsync(ms)
  await tick()
}

/** Drives a Node Transform or web TransformStream by hand: write, end, read on demand, see errors. */
function drive(runtime: Runtime, stream: unknown) {
  const output: unknown[] = []
  const state: { error: any; ended: boolean } = { error: undefined, ended: false }
  if (runtime === 'node') {
    const node = stream as Transform
    node.on('error', (error) => (state.error = error))
    node.on('end', () => (state.ended = true))
    return {
      output,
      state,
      write: (chunk: unknown) => void node.write(chunk),
      end: () => void node.end(),
      read: () => void node.on('data', (chunk) => output.push(chunk)),
      destroy: (error?: Error) => void node.destroy(error),
    }
  }
  const web = stream as TransformStream
  const writer = web.writable.getWriter()
  const reader = web.readable.getReader()
  writer.closed.catch(() => {})
  reader.closed.then(
    () => (state.ended = true),
    (error) => (state.error = error),
  )
  return {
    output,
    state,
    write: (chunk: unknown) => void writer.write(chunk).catch(() => {}),
    end: () => void writer.close().catch(() => {}),
    read: () => {
      void (async () => {
        for (;;) {
          const { done, value } = await reader.read().catch(() => ({ done: true, value: undefined }))
          if (done) return
          output.push(value)
        }
      })()
    },
    // With an error, the source fails (aborts the writable side); without one, the consumer cancels.
    destroy: (error?: Error) => {
      if (error) void writer.abort(error).catch(() => {})
      else void reader.cancel().catch(() => {})
    },
  }
}

const bytes = (n: number) => new Uint8Array(n)

describe.each(runtimes)('Watchdog (%s)', (runtime) => {
  const Watchdog = pick(runtime, { node: NodeWatchdog, web: WebWatchdog })

  it('passes a steady stream through and clears its timer at the end', async () => {
    const stream = drive(runtime, new Watchdog({ timeout: 100 }))
    stream.read()
    for (let i = 0; i < 10; i++) {
      stream.write(bytes(10))
      await advance(90)
    }
    stream.end()
    await advance(0)
    expect(stream.state).toEqual({ error: undefined, ended: true })
    expect(stream.output.length).toBe(10)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('fails a stream that stalls', async () => {
    const stream = drive(runtime, new Watchdog({ timeout: 100 }))
    stream.read()
    stream.write(bytes(10))
    await advance(99)
    expect(stream.state.error).toBeUndefined()
    await advance(1)
    expect(stream.state.error).toBeInstanceOf(WatchdogError)
    expect(stream.state.error).toMatchObject({ code: 'ERR_SYKE_TIMEOUT', timeout: 100, firstChunk: false })
    expect(stream.state.error.message).toBe('syke: stream stalled: no data for 100 ms')
    expect(stream.output.length).toBe(1)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('waits firstChunkTimeout for the first chunk, counted from construction', async () => {
    const stream = drive(runtime, new Watchdog({ timeout: 100, firstChunkTimeout: 500 }))
    stream.read()
    await advance(499)
    expect(stream.state.error).toBeUndefined()
    await advance(1)
    expect(stream.state.error).toMatchObject({ code: 'ERR_SYKE_TIMEOUT', timeout: 500, firstChunk: true })
    expect(stream.state.error.message).toBe('syke: no data arrived within 500 ms')
  })

  it('waits timeout for the first chunk by default', async () => {
    const stream = drive(runtime, new Watchdog({ timeout: 100 }))
    stream.read()
    await advance(100)
    expect(stream.state.error).toMatchObject({ timeout: 100, firstChunk: true })
  })

  it('switches to timeout after the first chunk', async () => {
    const stream = drive(runtime, new Watchdog({ timeout: 100, firstChunkTimeout: 1000 }))
    stream.read()
    await advance(10)
    stream.write(bytes(1))
    await advance(99)
    expect(stream.state.error).toBeUndefined()
    await advance(1)
    expect(stream.state.error).toMatchObject({ timeout: 100, firstChunk: false })
  })

  it('does not trip while a slow consumer applies backpressure', async () => {
    const watchdog = new Watchdog({ timeout: 100, ...(runtime === 'node' ? { highWaterMark: 10 } : {}) })
    const stream = drive(runtime, watchdog)
    // Nobody reads: downstream fills up and holds the source back.
    for (let i = 0; i < 5; i++) stream.write(bytes(10))
    await advance(10_000)
    expect(stream.state.error).toBeUndefined()
    // The consumer catches up; then the source stalls for real.
    stream.read()
    await advance(0)
    expect(stream.output.length).toBe(5)
    await advance(99)
    expect(stream.state.error).toBeUndefined()
    // Caught within a quarter of the timeout after the stall's own timeout.
    await advance(26)
    expect(stream.state.error).toMatchObject({ code: 'ERR_SYKE_TIMEOUT', firstChunk: false })
  })

  it('accepts an empty stream', async () => {
    const stream = drive(runtime, new Watchdog({ timeout: 100 }))
    stream.read()
    stream.end()
    await advance(0)
    expect(stream.state).toEqual({ error: undefined, ended: true })
    expect(vi.getTimerCount()).toBe(0)
  })

  it('clears its timer when destroyed or cancelled', async () => {
    const stream = drive(runtime, new Watchdog({ timeout: 100 }))
    stream.write(bytes(1))
    stream.destroy()
    await advance(0)
    expect(vi.getTimerCount()).toBe(0)
    await advance(1000)
    expect(stream.state.error?.code).not.toBe('ERR_SYKE_TIMEOUT')
  })

  it('clears its timer when the source fails', async () => {
    const stream = drive(runtime, new Watchdog({ timeout: 100 }))
    stream.read()
    stream.destroy(new Error('boom'))
    await advance(0)
    expect(stream.state.error?.message).toBe('boom')
    expect(vi.getTimerCount()).toBe(0)
  })

  it('handles a timeout longer than setTimeout can take', async () => {
    const day = 24 * 60 * 60 * 1000
    const stream = drive(runtime, new Watchdog({ timeout: 30 * day }))
    stream.read()
    stream.write(bytes(1))
    // An unclamped timer would wrap around and fire after 1 ms.
    await advance(1000)
    expect(stream.state.error).toBeUndefined()
    await advance(30 * day - 1001)
    expect(stream.state.error).toBeUndefined()
    await advance(1)
    expect(stream.state.error).toMatchObject({ code: 'ERR_SYKE_TIMEOUT', timeout: 30 * day })
  })

  it('rejects invalid options', () => {
    // @ts-expect-error timeout is required
    expect(() => new Watchdog({})).toThrow(RangeError)
    expect(() => new Watchdog({ timeout: 0 })).toThrow(RangeError)
    expect(() => new Watchdog({ timeout: Number.POSITIVE_INFINITY })).toThrow(RangeError)
    expect(() => new Watchdog({ timeout: 100, firstChunkTimeout: -1 })).toThrow(RangeError)
    expect(vi.getTimerCount()).toBe(0)
  })
})

describe('Watchdog (node)', () => {
  it('still trips with highWaterMark: 0', async () => {
    const stream = drive('node', new NodeWatchdog({ timeout: 100, highWaterMark: 0 }))
    stream.read()
    stream.write(bytes(10))
    await advance(99)
    expect(stream.state.error).toBeUndefined()
    await advance(1)
    expect(stream.state.error).toMatchObject({ code: 'ERR_SYKE_TIMEOUT', firstChunk: false })
  })

  it('trips waiting for the first chunk with highWaterMark: 0', async () => {
    const stream = drive('node', new NodeWatchdog({ timeout: 100, highWaterMark: 0 }))
    stream.read()
    await advance(100)
    expect(stream.state.error).toMatchObject({ code: 'ERR_SYKE_TIMEOUT', firstChunk: true })
  })

  it('tells backpressure from a stall with highWaterMark: 0', async () => {
    const stream = drive('node', new NodeWatchdog({ timeout: 100, highWaterMark: 0 }))
    for (let i = 0; i < 5; i++) stream.write(bytes(10))
    await advance(10_000)
    expect(stream.state.error).toBeUndefined()
    stream.read()
    await advance(0)
    expect(stream.output.length).toBe(5)
    await advance(125)
    expect(stream.state.error).toMatchObject({ code: 'ERR_SYKE_TIMEOUT', firstChunk: false })
  })

  it('passes records through in object mode', async () => {
    const stream = drive('node', new NodeWatchdog<{ n: number }>({ timeout: 100, objectMode: true }))
    stream.read()
    stream.write({ n: 1 })
    stream.end()
    await advance(0)
    expect(stream.output).toEqual([{ n: 1 }])
    expect(stream.state.ended).toBe(true)
  })
})

it('shares one WatchdogError class between syke/node and syke/web', () => {
  expect(WebWatchdogError).toBe(WatchdogError)
})
