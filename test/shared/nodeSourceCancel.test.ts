import { once } from 'node:events'
import { createServer, get, type IncomingMessage } from 'node:http'
import type { AddressInfo } from 'node:net'
import { addAbortSignal, PassThrough, pipeline, Readable } from 'node:stream'
import { runInNewContext } from 'node:vm'
import { describe, expect, it } from 'vitest'
import * as node from '../../src/node.ts'
import { open } from '../../src/shared/combinator/node.ts'
import * as web from '../../src/web.ts'
import { reader, readUpTo, settle } from '../streamHarness.ts'

/**
 * What a combinator does to a Node `Readable` source when it stops it: a bare
 * `destroy()`, whatever the reason, so nothing wired to the source sees an
 * error it didn't cause.
 */

interface EmittedError {
  error: unknown
  /** Whether anything was listening. */
  handled: boolean
}

/**
 * Records every `'error'` the emitter emits. One nobody listens for is
 * recorded and swallowed, so the test fails on it instead of crashing.
 */
function watchErrors(emitter: NodeJS.EventEmitter): EmittedError[] {
  const errors: EmittedError[] = []
  const emit = emitter.emit
  emitter.emit = function (this: NodeJS.EventEmitter, event: string | symbol, ...args: unknown[]) {
    if (event === 'error') {
      const handled = this.listenerCount('error') > 0
      errors.push({ error: args[0], handled })
      if (!handled) return false
    }
    return emit.call(this, event, ...args)
  } as typeof emitter.emit
  return errors
}

interface Seen {
  destroyed: boolean
  /** What `_destroy()` got: the error the stream was destroyed with, or null. */
  destroyedWith: unknown
  errors: EmittedError[]
}

/** An endless pull-driven byte (or object) source, recording how it was destroyed. */
function spySource(objectMode = false): { stream: Readable; seen: Seen } {
  let i = 0
  const stream = new Readable({
    objectMode,
    highWaterMark: 1,
    read() {
      this.push(objectMode ? i++ : Buffer.from([i++ & 0xff]))
    },
    destroy(error, done) {
      seen.destroyed = true
      seen.destroyedWith = error
      done(error)
    },
  })
  const seen: Seen = { destroyed: false, destroyedWith: undefined, errors: watchErrors(stream) }
  return { stream, seen }
}

const reasons: [name: string, reason: () => unknown][] = [
  ['an Error', () => new Error('consumer gave up')],
  ['an Error from another realm', () => runInNewContext('new Error("from a vm context")')],
  ["an AbortSignal's reason (a DOMException)", () => AbortSignal.abort().reason],
  ['a string', () => 'not an Error'],
  ['no reason', () => undefined],
]

/** Reads a little from the source, then returns how to stop it: cancel or destroy the output(s). */
type Scenario = [name: string, start: (source: Readable) => Promise<(reason: unknown) => Promise<void>>]

const nodeScenarios: Scenario[] = [
  [
    'concat',
    async (source) => {
      const r = reader(node.concat([source]))
      await readUpTo(r, 2)
      return (reason) => r.cancel(reason)
    },
  ],
  [
    'merge',
    async (source) => {
      const r = reader(node.merge([source]))
      await readUpTo(r, 2)
      return (reason) => r.cancel(reason)
    },
  ],
  [
    'peek',
    async (source) => {
      const { stream } = await node.peek(source, 1)
      const r = reader(stream)
      await readUpTo(r, 3)
      return (reason) => r.cancel(reason)
    },
  ],
  [
    'fork',
    async (source) => {
      const [a, b] = node.fork(source).map(reader)
      await Promise.all([readUpTo(a!, 2), readUpTo(b!, 2)])
      // The source is stopped once every branch is gone.
      return async (reason) => {
        await a!.cancel(new Error('first branch'))
        await b!.cancel(reason)
      }
    },
  ],
  [
    'fork with a buffer',
    async (source) => {
      const [a, b] = node.fork(source, { buffer: () => new PassThrough() }).map(reader)
      await Promise.all([readUpTo(a!, 2), readUpTo(b!, 2)])
      return async (reason) => {
        await a!.cancel(new Error('first branch'))
        await b!.cancel(reason)
      }
    },
  ],
]

describe.each(nodeScenarios)('syke/node %s, stopping a Node readable source', (_, start) => {
  it.each(reasons)('destroys it with no error for %s', async (_, makeReason) => {
    const { stream, seen } = spySource()
    const stop = await start(stream)
    await stop(makeReason())
    await settle()
    expect(seen.destroyed).toBe(true)
    expect(seen.destroyedWith).toBeNull()
    expect(stream.errored).toBeNull()
    expect(seen.errors).toEqual([])
  })

  it("lets events.once(source, 'close') resolve, and never calls the source's own 'error' listener", async () => {
    const { stream } = spySource()
    const own: unknown[] = []
    stream.on('error', (error) => own.push(error))
    const closed = once(stream, 'close')
    const stop = await start(stream)
    await stop(new Error('consumer gave up'))
    await expect(closed).resolves.toEqual([])
    expect(own).toEqual([])
  })

  it('leaves an upstream pipeline() into the source with a premature close, not the reason', async () => {
    const upstream = new Readable({
      highWaterMark: 1,
      read() {
        this.push(Buffer.of(1))
      },
    })
    const source = new PassThrough({ highWaterMark: 1 })
    const result = new Promise<NodeJS.ErrnoException | null | undefined>((resolve) => {
      pipeline(upstream, source, resolve)
    })
    const stop = await start(source)
    await stop(new Error('consumer gave up'))
    const error = await result
    expect(error?.code).toBe('ERR_STREAM_PREMATURE_CLOSE')
    expect(upstream.destroyed).toBe(true)
  })

  it('stops a Readable.from(generator) source through return(), not throw()', async () => {
    const state: { caught: unknown; returned: boolean } = { caught: undefined, returned: false }
    async function* chunks() {
      try {
        for (let i = 0; ; i++) yield Buffer.of(i & 0xff)
      } catch (error) {
        state.caught = error
      } finally {
        state.returned = true
      }
    }
    const source = Readable.from(chunks(), { objectMode: false, highWaterMark: 1 })
    const errors = watchErrors(source)
    const stop = await start(source)
    await stop(new Error('consumer gave up'))
    await settle()
    expect(state.returned).toBe(true)
    expect(state.caught).toBeUndefined()
    expect(errors).toEqual([])
  })
})

describe('syke/node concat, aborting the output through an AbortSignal', () => {
  it('destroys the source with no error', async () => {
    const { stream, seen } = spySource()
    const controller = new AbortController()
    const output = addAbortSignal(controller.signal, node.concat([stream]))
    output.on('error', () => {})
    await readUpTo(reader(output), 2)
    controller.abort()
    await settle()
    expect(output.errored).toBeTruthy()
    expect(seen.destroyedWith).toBeNull()
    expect(seen.errors).toEqual([])
  })
})

describe('syke/node, a Node readable source shared by two concat()s', () => {
  it('stops it with no error when one is cancelled, so the other gets no error it did not cause', async () => {
    const { stream, seen } = spySource(true)
    const first = reader(node.concat([stream]))
    // Never read, and with no 'error' listener of its own.
    const other = node.concat([stream])
    const otherErrors = watchErrors(other)
    await readUpTo(first, 2)
    await first.cancel(new Error('consumer gave up'))
    await settle()
    expect(seen.destroyedWith).toBeNull()
    expect(seen.errors).toEqual([])
    expect(otherErrors).toEqual([])
  })

  it('ends the other one reading it with a premature close, not the reason', async () => {
    const { stream, seen } = spySource(true)
    const first = reader(node.concat([stream]))
    const otherOutput = node.concat([stream])
    const otherErrors = watchErrors(otherOutput)
    const other = reader(otherOutput)
    otherOutput.on('error', () => {})
    await Promise.all([readUpTo(first, 2), readUpTo(other, 2)])
    await first.cancel(new Error('consumer gave up'))
    await settle()
    expect(seen.errors).toEqual([])
    expect(otherErrors.map(({ error }) => (error as NodeJS.ErrnoException).code)).toEqual([
      'ERR_STREAM_PREMATURE_CLOSE',
    ])
  })
})

describe('syke/node, stopping a Node readable source because another failed', () => {
  const boom = new Error('another source failed')
  const failing = () =>
    new ReadableStream({
      start(controller) {
        controller.error(boom)
      },
    })

  it.each([
    ['merge', (source: Readable) => node.merge([source, failing()])],
    ['concat', (source: Readable) => node.concat([source, failing()])],
  ])('%s destroys it with no error', async (_, combine) => {
    const { stream, seen } = spySource()
    const output = combine(stream)
    const errored = new Promise((resolve) => output.on('error', resolve))
    expect(await errored).toBe(boom)
    await settle()
    expect(seen.destroyed).toBe(true)
    expect(seen.destroyedWith).toBeNull()
    expect(seen.errors).toEqual([])
  })

  it('peek destroys it with no error for a chunk that is not bytes', async () => {
    const { stream, seen } = spySource(true)
    const error = await node.peek(stream, 1).catch((e: unknown) => e)
    expect(error).toBeInstanceOf(TypeError)
    await settle()
    expect(seen.destroyed).toBe(true)
    expect(seen.destroyedWith).toBeNull()
    expect(seen.errors).toEqual([])
  })
})

describe('syke/node, a Node readable source with autoDestroy: false that fails', () => {
  it('emits its error once, and is then destroyed with no error', async () => {
    let reads = 0
    const failure = new Error('read failed')
    const source = new Readable({
      objectMode: true,
      autoDestroy: false,
      read() {
        if (++reads === 3) throw failure
        this.push(reads)
      },
    })
    const errors = watchErrors(source)
    const output = node.merge([source])
    const errored = new Promise((resolve) => output.on('error', resolve))
    output.resume()
    expect(await errored).toBe(failure)
    await settle()
    expect(source.destroyed).toBe(true)
    expect(errors).toEqual([{ error: failure, handled: true }])
  })
})

describe('syke/node concat, a Node readable a factory returns after the output was cancelled', () => {
  it('is destroyed with no error', async () => {
    let resolveFactory: (source: Readable) => void = () => {}
    const factory = () =>
      new Promise<Readable>((resolve) => {
        resolveFactory = resolve
      })
    const r = reader(node.concat([[1], factory]))
    expect(await readUpTo(r, 10)).toEqual([1])
    await r.cancel(new Error('stop'))
    const { stream, seen } = spySource()
    resolveFactory(stream)
    await settle()
    expect(seen.destroyed).toBe(true)
    expect(seen.destroyedWith).toBeNull()
    expect(seen.errors).toEqual([])
  })
})

describe('syke/node peek, over an HTTP response', () => {
  it('destroying the output with an Error ends the response without erroring the request', async () => {
    const server = createServer((_, res) => {
      res.write('abcdefgh')
      const timer = setInterval(() => res.write('x'), 5)
      res.on('close', () => clearInterval(timer))
    })
    server.listen(0, '127.0.0.1')
    await once(server, 'listening')
    try {
      const { port } = server.address() as AddressInfo
      // No 'error' listener on the request: one it emitted would crash the process.
      let requestErrors: EmittedError[] = []
      const response = await new Promise<IncomingMessage>((resolve) => {
        const request = get(`http://127.0.0.1:${port}/`, resolve)
        requestErrors = watchErrors(request)
      })
      const responseErrors = watchErrors(response)
      const { head, stream } = await node.peek(response, 4)
      expect(Buffer.from(head).toString()).toBe('abcd')
      stream.on('error', () => {})
      stream.destroy(new Error('consumer gave up'))
      await settle(20)
      expect(response.destroyed).toBe(true)
      expect(response.errored).toBeNull()
      expect(responseErrors).toEqual([])
      expect(requestErrors).toEqual([])
    } finally {
      server.closeAllConnections()
      server.close()
    }
  })
})

describe('the Node readable puller', () => {
  it('leaves a released stream alone if cancelled afterwards', async () => {
    const { stream, seen } = spySource()
    const puller = open(stream, () => {})
    puller.release()
    puller.cancel(new Error('too late'))
    await settle()
    expect(seen.destroyed).toBe(false)
    expect(stream.listenerCount('error')).toBe(0)
    stream.destroy()
  })
})

describe('syke/web, with a Node readable source', () => {
  // syke/web reads a Node readable as the async iterable it is, so stopping
  // it calls the iterator's return(), as for any async iterable; Node then
  // destroys the stream itself (with an AbortError), not with our reason.
  it.each([
    ['concat', (source: Readable) => web.concat([source])],
    ['merge', (source: Readable) => web.merge([source])],
  ])('%s stops it through return(), without an uncaught error', async (_, combine) => {
    const { stream, seen } = spySource(true)
    const r = reader(combine(stream))
    await readUpTo(r, 2)
    const reason = new Error('consumer gave up')
    await r.cancel(reason)
    await settle()
    expect(seen.destroyed).toBe(true)
    expect(seen.destroyedWith).not.toBe(reason)
    expect(seen.errors.every((e) => e.handled)).toBe(true)
  })
})
