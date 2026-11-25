import { execFileSync } from 'node:child_process'
import { randomBytes } from 'node:crypto'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'
import { ReadableStream as NodeReadableStream } from 'node:stream/web'
import { describe, expect, expectTypeOf, it, vi } from 'vitest'
import { chunk } from '../../harness.ts'
import { freshPage, installFakeBrowser, useFakeBrowser, waitFor } from './fakeBrowser.ts'
import type { Codec, Spool as SpoolClass } from '../../../src/web.ts'

const KiB = 1024
const MiB = 1024 * KiB

useFakeBrowser()

async function readAll<T>(readable: ReadableStream<T>): Promise<T[]> {
  const out: T[] = []
  for await (const value of readable as unknown as AsyncIterable<T>) out.push(value)
  return out
}

describe('Spool.from()', () => {
  it('reads an iterable, accepting all of it while the consumer is stalled', async () => {
    const browser = installFakeBrowser()
    const { Spool } = await freshPage()
    const data = randomBytes(3 * MiB)
    const spool = Spool.from(chunk(data, 64 * KiB))
    await waitFor(() => browser.opfsFiles().length === 1)
    // Nothing has read yet, but the iterable is fully consumed into storage.
    await waitFor(async () => browser.root.bytes() >= 3 * MiB - 64 * KiB - MiB)
    expect(Buffer.concat(await readAll(spool.readable)).equals(data)).toBe(true)
  })

  it('reads an async iterable and a ReadableStream', async () => {
    installFakeBrowser()
    const { Spool } = await freshPage()
    const data = randomBytes(2 * MiB)
    async function* generate() {
      yield* chunk(data, 50_000)
    }
    expect(Buffer.concat(await readAll(Spool.from(generate()).readable)).equals(data)).toBe(true)
    // lib.dom's ReadableStream lacks from(); Node's is the same class.
    const stream = NodeReadableStream.from(chunk(data, 50_000)) as unknown as ReadableStream<Uint8Array>
    expect(Buffer.concat(await readAll(Spool.from(stream).readable)).equals(data)).toBe(true)
  })

  it('fails the readable side when the source errors', async () => {
    installFakeBrowser()
    const { Spool } = await freshPage()
    async function* failing() {
      yield* chunk(randomBytes(2 * MiB), 64 * KiB)
      throw new Error('socket reset')
    }
    const spool = Spool.from(failing())
    await expect(readAll(spool.readable)).rejects.toThrow('socket reset')
    await spool.closed
  })

  it('stops the source when the readable side is cancelled', async () => {
    installFakeBrowser()
    const { Spool } = await freshPage()
    let returned = false
    function* endless() {
      try {
        for (;;) yield new Uint8Array(64 * KiB)
      } finally {
        returned = true
      }
    }
    const spool = Spool.from(endless(), { maxDiskBytes: MiB })
    await waitFor(() => spool.storage !== null)
    await spool.readable.cancel(new Error('enough'))
    await waitFor(() => returned)
    await spool.closed
  })
})

describe('Spool in the browser', () => {
  it('passes chunks straight through while the consumer keeps up, never touching storage', async () => {
    const browser = installFakeBrowser()
    const { Spool } = await freshPage()
    const data = randomBytes(MiB)
    const spool = new Spool()
    const chunks = chunk(data, 10_000)
    const source = NodeReadableStream.from(chunks) as unknown as ReadableStream<Uint8Array>
    const out = await readAll(source.pipeThrough(spool))
    expect(Buffer.concat(out).equals(data)).toBe(true)
    // pipeThrough reads as fast as it writes: the same chunk objects come out.
    expect(out.every((piece, i) => piece === chunks[i])).toBe(true)
    expect(spool.storage).toBeNull()
    expect(browser.workers.size).toBe(0)
    expect(browser.estimates).toBe(0)
  })

  it('holds bursts under memoryThreshold in memory', async () => {
    const browser = installFakeBrowser()
    const { Spool } = await freshPage()
    const spool = new Spool({ memoryThreshold: 512 * KiB })
    const writer = spool.writable.getWriter()
    const data = randomBytes(500 * KiB)
    for (const piece of chunk(data, 50 * KiB)) await writer.write(piece)
    await writer.close()
    expect(Buffer.concat(await readAll(spool.readable)).equals(data)).toBe(true)
    expect(spool.storage).toBeNull()
    expect(browser.opfsFiles()).toEqual([])
  })

  it('counts memoryThreshold in records in object mode', async () => {
    installFakeBrowser()
    const { Spool } = await freshPage()
    const spool = new Spool<{ n: number }>({ objectMode: true, memoryThreshold: 10 })
    const records = Array.from({ length: 11 }, (_, n) => ({ n }))
    const writer = spool.writable.getWriter()
    for (const record of records) await writer.write(record)
    await writer.close()
    const out = await readAll(spool.readable)
    expect(out).toEqual(records)
    // 1 passed through and 10 held in memory: all the same objects.
    expect(out.every((record, i) => record === records[i])).toBe(true)
  })

  it("copies spilled chunks: the caller's buffers are never detached or kept", async () => {
    installFakeBrowser()
    const { Spool } = await freshPage()
    const spool = new Spool()
    const writer = spool.writable.getWriter()
    const data = randomBytes(3 * MiB)
    const chunks = chunk(new Uint8Array(data), 64 * KiB).map((piece) => piece.slice())
    for (const piece of chunks) await writer.write(piece)
    await writer.close()
    // Reusing the buffers after write() resolves must not change what was spooled.
    for (const piece of chunks.slice(1)) {
      expect(piece.byteLength).toBe(64 * KiB)
      piece.fill(0)
    }
    expect(Buffer.concat(await readAll(spool.readable)).equals(data)).toBe(true)
  })

  it('ends at once for an empty stream', async () => {
    installFakeBrowser()
    const { Spool } = await freshPage()
    const spool = new Spool()
    await spool.writable.close()
    expect(await readAll(spool.readable)).toEqual([])
    await spool.closed
  })

  it('fails on a chunk that is not a Uint8Array, unless in object mode', async () => {
    installFakeBrowser()
    const { Spool } = await freshPage()
    const spool = new Spool()
    const writer = spool.writable.getWriter()
    await expect(writer.write('text' as never)).rejects.toThrow(/must be Uint8Array.*got string/)
    await expect(spool.readable.getReader().read()).rejects.toThrow(TypeError)
  })

  it('validates options in the constructor', async () => {
    const { Spool } = await freshPage()
    expect(() => new Spool({ chunkSize: 0 })).toThrow(RangeError)
    expect(() => new Spool({ memoryThreshold: -1 })).toThrow(RangeError)
    expect(() => new Spool({ maxDiskBytes: 1.5 })).toThrow(RangeError)
    expect(() => new Spool({ minFreeSpace: '150%' })).toThrow(RangeError)
    expect(() => new Spool({ onLimit: 'drop' as never })).toThrow(RangeError)
    expect(() => new Spool({ storage: 'localStorage' as never })).toThrow(/storage must be 'auto' or 'opfs' or 'indexeddb'/)
    expect(() => new Spool({ objectMode: 'yes' as never })).toThrow(TypeError)
    expect(() => new Spool({ codec: { encode: () => new Uint8Array(), decode: () => 0 } } as never)).toThrow(
      /codec only applies with objectMode/,
    )
    expect(() => new Spool({ objectMode: true, codec: {} as never })).toThrow(TypeError)
  })

  it('deletes the storage of a spool garbage-collected without ending', () => {
    const entry = pathToFileURL(join(import.meta.dirname, '../../../src/web.ts')).href
    const child = `
      import { IDBFactory, IDBKeyRange } from 'fake-indexeddb'
      globalThis.indexedDB = new IDBFactory()
      globalThis.IDBKeyRange = IDBKeyRange
      const { Spool } = await import(${JSON.stringify(entry)})

      const count = () => new Promise((resolve) => {
        const open = indexedDB.open('syke-spool')
        open.onsuccess = () => {
          const req = open.result.transaction('chunks').objectStore('chunks').count()
          req.onsuccess = () => { open.result.close(); resolve(req.result) }
        }
      })

      // A spool with no consumer, dropped without being ended or cancelled.
      async function abandon() {
        const spool = new Spool({ storage: 'indexeddb' })
        const writer = spool.writable.getWriter()
        for (let i = 0; i < 48; i++) await writer.write(new Uint8Array(64 * 1024))
        while ((await count()) === 0) await new Promise((resolve) => setTimeout(resolve, 5))
      }

      await abandon()
      const before = await count()
      let after = before
      for (let i = 0; i < 100 && after > 0; i++) {
        globalThis.gc()
        await new Promise((resolve) => setTimeout(resolve, 10))
        after = await count()
      }
      console.log(JSON.stringify({ before, after }))
      process.exit(0)
    `
    const output = execFileSync(process.execPath, ['--expose-gc', '--input-type=module', '-e', child], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'inherit'],
      timeout: 20_000,
    })
    const result = JSON.parse(output)
    expect(result.before).toBeGreaterThan(0)
    expect(result.after).toBe(0)
  })
})

describe('Spool.from() edge cases', () => {
  it('throws at once for a locked ReadableStream instead of hanging', async () => {
    installFakeBrowser()
    const { Spool } = await freshPage()
    const stream = new ReadableStream<Uint8Array>()
    stream.getReader()
    expect(() => Spool.from(stream)).toThrow(/locked/)
  })

  it('throws a TypeError for something it cannot read', async () => {
    installFakeBrowser()
    const { Spool } = await freshPage()
    expect(() => Spool.from(42 as never)).toThrow(/needs a ReadableStream, an iterable or an async iterable/)
    expect(() => Spool.from({ [Symbol.asyncIterator]: 'no' } as never)).toThrow(TypeError)
  })

  it("calls the iterator's return() with no arguments when cancelled", async () => {
    installFakeBrowser()
    const { Spool } = await freshPage()
    const returned = vi.fn(async (..._args: unknown[]) => ({ done: true as const, value: undefined }))
    let n = 0
    const iterator: AsyncIterableIterator<Uint8Array> = {
      next: async () => ({ done: false, value: new Uint8Array([n++]) }),
      return: returned,
      [Symbol.asyncIterator]() {
        return this
      },
    }
    const spool = Spool.from(iterator)
    const reader = spool.readable.getReader()
    await reader.read()
    await reader.cancel(new Error('enough'))
    await waitFor(() => returned.mock.calls.length > 0)
    expect(returned).toHaveBeenCalledWith()
  })
})

describe('chunk handling', () => {
  it('drops empty chunks rather than queueing them', async () => {
    installFakeBrowser()
    const { Spool } = await freshPage()
    const spool = new Spool({ memoryThreshold: 1024 })
    const writer = spool.writable.getWriter()
    await writer.write(new Uint8Array([1]))
    // Empty chunks cost nothing against memoryThreshold, so they must not be queued at all.
    for (let i = 0; i < 10_000; i++) await writer.write(new Uint8Array(0))
    await writer.write(new Uint8Array([2]))
    await writer.close()
    const out = await readAll(spool.readable)
    expect(out.map((piece) => [...piece])).toEqual([[1], [2]])
    expect(spool.storage).toBeNull()
  })

  it('decodes spilled records one at a time, failing with SpoolCodecError at the bad one', async () => {
    installFakeBrowser()
    const { Spool, SpoolCodecError, jsonCodec } = await freshPage()
    const codec: Codec<{ i: number }> = {
      encode: (value) => jsonCodec.encode(value),
      decode: (bytes) => {
        const value = jsonCodec.decode(bytes) as { i: number }
        if (value.i === 500) throw new Error('bad record')
        return value
      },
    }
    const spool = new Spool<{ i: number }>({ storage: 'indexeddb', objectMode: true, codec })
    const writer = spool.writable.getWriter()
    for (let i = 0; i < 1000; i++) await writer.write({ i })
    await writer.close()
    const reader = spool.readable.getReader()
    const seen: number[] = []
    const error = await (async () => {
      for (;;) {
        const { value, done } = await reader.read()
        if (done) return null
        seen.push(value.i)
      }
    })().catch((e: unknown) => e)
    expect(error).toBeInstanceOf(SpoolCodecError)
    expect(seen).toEqual(Array.from({ length: 500 }, (_, i) => i))
    await spool.closed
  })

  it('does not pin large storage buffers in the small chunks it emits', async () => {
    installFakeBrowser()
    const { Spool } = await freshPage()
    const data = randomBytes(3 * MiB)
    const spool = new Spool({ storage: 'indexeddb', chunkSize: 16 * KiB })
    const writer = spool.writable.getWriter()
    for (const piece of chunk(data, 64 * KiB)) await writer.write(piece)
    await writer.close()
    const out = await readAll(spool.readable)
    expect(Buffer.concat(out).equals(data)).toBe(true)
    for (const piece of out.slice(1)) expect(piece.buffer.byteLength).toBeLessThanOrEqual(2 * piece.byteLength)
  })
})

describe('the OPFS worker', () => {
  it('retries getDirectory() after a passing failure instead of caching it for every spool', async () => {
    installFakeBrowser({ opfsGetDirectoryFailures: 1 })
    const { Spool } = await freshPage()
    const a = new Spool()
    const b = new Spool()
    const writers = [a.writable.getWriter(), b.writable.getWriter()]
    // Both open storage at once, sharing the worker; the first open fails.
    await Promise.all(
      writers.map(async (writer) => {
        for (const piece of chunk(randomBytes(2 * MiB), 64 * KiB)) await writer.write(piece)
      }),
    )
    await waitFor(() => a.storage !== null && b.storage !== null)
    expect([a.storage, b.storage].sort()).toEqual(['indexeddb', 'opfs'])
    await a.readable.cancel()
    await b.readable.cancel()
  })

  it('fails the spool, rather than hang, when a worker reply cannot be deserialized', async () => {
    const browser = installFakeBrowser()
    const { Spool } = await freshPage()
    const spool = new Spool({ storage: 'opfs' })
    const writer = spool.writable.getWriter()
    for (const piece of chunk(randomBytes(2 * MiB), 64 * KiB)) await writer.write(piece)
    await waitFor(() => browser.opfsFiles().length === 1)
    for (const worker of browser.workers) worker.dispatchEvent(new MessageEvent('messageerror'))
    expect(browser.workers.size).toBe(0)
    await expect(readAll(spool.readable)).rejects.toThrow(/could not be read|terminated/)
  })
})

describe('Spool types', () => {
  it('types byte and object-mode spools', () => {
    expectTypeOf<SpoolClass>().toExtend<ReadableWritablePair<Uint8Array, Uint8Array>>()
    expectTypeOf<SpoolClass<{ a: number }>>().toExtend<ReadableWritablePair<{ a: number }, { a: number }>>()
    // Typechecked only (pnpm typecheck): records need objectMode. (A codec
    // without objectMode still compiles, as in 0.1.0, and fails at runtime.)
    // More in types.test.ts.
    const check = (S: typeof SpoolClass, codec: Codec<{ a: number }>) => {
      expectTypeOf(S.from([new Uint8Array(1)])).toEqualTypeOf<SpoolClass<Uint8Array<ArrayBuffer>>>()
      const records = S.from([{ a: 1 }], { objectMode: true })
      expectTypeOf(records).toEqualTypeOf<SpoolClass<{ a: number }>>()
      expectTypeOf(new S({ objectMode: true, codec })).toEqualTypeOf<SpoolClass<{ a: number }>>()
      // @ts-expect-error records without objectMode
      S.from([{ a: 1 }])
    }
    expect(typeof check).toBe('function')
  })
})

describe('garbage collection while paused', () => {
  it('collects a spool paused at minFreeSpace (polling the quota) and deletes its storage', () => {
    const entry = pathToFileURL(join(import.meta.dirname, '../../../src/web.ts')).href
    const child = `
      import { IDBFactory, IDBKeyRange } from 'fake-indexeddb'
      globalThis.indexedDB = new IDBFactory()
      globalThis.IDBKeyRange = IDBKeyRange
      const MiB = 1024 * 1024
      // 4 MiB quota, 1 MiB used besides the spool's 1 MiB records, a 25%
      // floor: the spool stays paused at 2 MiB stored.
      Object.defineProperty(navigator, 'storage', {
        configurable: true,
        value: { estimate: async () => ({ quota: 4 * MiB, usage: MiB + (await count()) * MiB }) },
      })
      const { Spool } = await import(${JSON.stringify(entry)})

      const count = async () => {
        // Don't create the database before the spool does.
        if (!(await indexedDB.databases()).some((d) => d.name === 'syke-spool')) return 0
        return countRecords()
      }
      const countRecords = () => new Promise((resolve) => {
        const open = indexedDB.open('syke-spool')
        open.onsuccess = () => {
          const req = open.result.transaction('chunks').objectStore('chunks').count()
          req.onsuccess = () => { open.result.close(); resolve(req.result) }
        }
      })

      // A spool paused at the floor, with no consumer, dropped without being ended or cancelled.
      async function abandon() {
        const spool = new Spool({ storage: 'indexeddb', minFreeSpace: '25%' })
        const writer = spool.writable.getWriter()
        for (let i = 0; i < 80; i++) writer.write(new Uint8Array(64 * 1024)).catch(() => {})
        while ((await count()) < 2) await new Promise((resolve) => setTimeout(resolve, 5))
        // Paused: quota polls have come and gone, and the source is held back.
        await new Promise((resolve) => setTimeout(resolve, 1500))
        if ((await count()) !== 2) throw new Error('the spool did not pause at the floor')
      }

      await abandon()
      const before = await count()
      let after = before
      for (let i = 0; i < 100 && after > 0; i++) {
        globalThis.gc()
        await new Promise((resolve) => setTimeout(resolve, 30))
        after = await count()
      }
      console.log(JSON.stringify({ before, after }))
      process.exit(0)
    `
    const output = execFileSync(process.execPath, ['--expose-gc', '--input-type=module', '-e', child], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'inherit'],
      timeout: 30_000,
    })
    const result = JSON.parse(output)
    expect(result.before).toBeGreaterThan(0)
    expect(result.after).toBe(0)
  })
})
