import { spawnSync } from 'node:child_process'
import { randomBytes } from 'node:crypto'
import { getEventListeners } from 'node:events'
import { readdirSync, statSync } from 'node:fs'
import { type FileHandle, open, readdir, statfs } from 'node:fs/promises'
import { join } from 'node:path'
import { Duplex, Readable, Stream, Writable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { pathToFileURL } from 'node:url'
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { jsonCodec, Spool, SpoolCodecError, SpoolLimitError, type SpoolOptions } from '../../../src/node.ts'
import { closed, fileSize, sink, sleep, source, until, useTempDir } from '../../nodeHelpers.ts'

vi.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>()
  return { ...actual, statfs: vi.fn(actual.statfs), open: vi.fn(actual.open) }
})

const tmp = useTempDir()
const KiB = 1024
const MiB = 1024 * KiB
const entry = pathToFileURL(join(import.meta.dirname, '../../../src/node.ts')).href

// FileHandle's methods live on a prototype shared by every handle, so a spy
// there reaches the spool's own handle.
let FileHandleProto: FileHandle
beforeAll(async () => {
  const handle = await open(import.meta.filename, 'r')
  FileHandleProto = Object.getPrototypeOf(handle)
  await handle.close()
})

beforeEach(() => {
  vi.mocked(statfs).mockReset()
  vi.mocked(open).mockReset()
})
afterEach(() => {
  vi.restoreAllMocks()
})

/** Fakes the test's temp dir filesystem: `free` bytes minus whatever is written there. */
function fakeDisk({ total, free }: { total: number; free: number }) {
  vi.mocked(statfs).mockImplementation((async () => {
    const used = readdirSync(tmp.path).reduce((n, f) => n + statSync(join(tmp.path, f)).size, 0)
    const available = Math.max(0, free - used)
    return { type: 0xef53, bsize: 1, blocks: total, bfree: available, bavail: available, files: 0, ffree: 0 }
  }) as never)
}

const noSpace = () => Object.assign(new Error('ENOSPC: no space left on device, write'), { code: 'ENOSPC' })

/** Writes `data` into `spool` with no consumer attached, so what memory can't hold spills. */
async function spillWithoutConsumer(spool: Spool, data: Buffer, size = 4 * KiB) {
  for (let i = 0; i < data.length; i += size) spool.write(data.subarray(i, i + size))
  spool.end()
  await until(() => spool.writableFinished)
}

function runChild(code: string) {
  return spawnSync(process.execPath, ['--input-type=module', '-e', code], {
    encoding: 'utf8',
    timeout: 10_000,
  })
}

describe('re-entrant listeners', () => {
  it("doesn't start a second disk read when a 'data' listener calls read()", async () => {
    const data = randomBytes(MiB)
    const spool = new Spool({ dir: tmp.path, memoryThreshold: 64 * KiB, chunkSize: 64 * KiB })
    await spillWithoutConsumer(spool, data)
    expect(fileSize(spool)).toBeGreaterThan(0)

    const chunks: Buffer[] = []
    spool.on('data', (chunk: Buffer) => {
      chunks.push(chunk)
      spool.read(0)
    })
    await new Promise((resolve, reject) => {
      spool.on('end', resolve)
      spool.on('error', reject)
    })
    expect(Buffer.concat(chunks).equals(data)).toBe(true)
  })

  it("keeps records in order when a 'data' listener calls read()", async () => {
    const records = Array.from({ length: 3000 }, (_, i) => ({ i }))
    const spool = new Spool({ objectMode: true, dir: tmp.path, memoryThreshold: 50, chunkSize: 512 })
    for (const record of records) spool.write(record)
    spool.end()
    await until(() => spool.writableFinished)

    const received: unknown[] = []
    spool.on('data', (record) => {
      received.push(record)
      spool.read(0)
    })
    await new Promise((resolve, reject) => {
      spool.on('end', resolve)
      spool.on('error', reject)
    })
    expect(received).toEqual(records)
  })

  it("stops pushing when a 'data' listener destroys the stream mid-backlog", async () => {
    const spool = new Spool({ dir: tmp.path, memoryThreshold: 64 * KiB, chunkSize: 64 * KiB })
    await spillWithoutConsumer(spool, randomBytes(MiB))

    const push = spool.push.bind(spool)
    const pushedAfterDestroy: unknown[] = []
    vi.spyOn(spool, 'push').mockImplementation((chunk) => {
      if (spool.destroyed) pushedAfterDestroy.push(chunk)
      return push(chunk)
    })
    const events: string[] = []
    spool.on('error', (error) => events.push(`error: ${error.message}`))
    spool.on('end', () => events.push('end'))
    let seen = 0
    spool.on('data', () => {
      if (++seen === 2) spool.destroy()
    })
    await closed(spool)
    await sleep(20)

    expect(pushedAfterDestroy).toEqual([])
    expect(events).toEqual([])
    expect(await readdir(tmp.path)).toEqual([])
  })

  it("doesn't end the stream when a 'data' listener destroys it mid-way through a disk read", async () => {
    // One read brings back every record, so they're pushed (and their 'data'
    // events run) from the read's callback, and the file is drained by then.
    const spool = new Spool({ objectMode: true, dir: tmp.path })
    for (let i = 0; i < 10; i++) spool.write({ i })
    spool.end()
    await until(() => spool.writableFinished)
    expect(fileSize(spool)).toBeGreaterThan(0)

    const push = spool.push.bind(spool)
    const pushedAfterDestroy: unknown[] = []
    vi.spyOn(spool, 'push').mockImplementation((chunk) => {
      if (spool.destroyed) pushedAfterDestroy.push(chunk)
      return push(chunk)
    })
    let seen = 0
    spool.on('data', () => {
      if (++seen === 2) spool.destroy()
    })
    await closed(spool)
    expect(seen).toBe(2)
    expect(pushedAfterDestroy).toEqual([])
  })
})

describe('empty chunks in byte mode', () => {
  it('are dropped rather than queued while the consumer is stalled', async () => {
    const spool = new Spool({ dir: tmp.path })
    const pushedEmpty = vi.fn()
    const push = spool.push.bind(spool)
    vi.spyOn(spool, 'push').mockImplementation((chunk) => {
      if (chunk !== null && (chunk as Buffer).length === 0) pushedEmpty()
      return push(chunk)
    })
    const out = sink()
    out.pause()
    const done = pipeline(spool, out.stream)
    for (let i = 0; i < 10_000; i++) {
      spool.write(Buffer.alloc(0))
      spool.write('')
    }
    spool.end('tail')
    await until(() => spool.writableFinished)
    out.resume()
    await done
    expect(out.received.toString()).toBe('tail')
    // Queued empties would be pushed out once the consumer resumed.
    expect(pushedEmpty).not.toHaveBeenCalled()
  })
})

describe('waiting for space', () => {
  const pollTimers = (spy: { mock: { calls: unknown[][]; results: { value: unknown }[] } }) =>
    spy.mock.calls.flatMap(([, ms], i) => (ms === 250 ? [spy.mock.results[i]?.value as NodeJS.Timeout] : []))

  it('sets no timer at maxDiskBytes, where only the consumer can free space', async () => {
    fakeDisk({ total: 1000 * MiB, free: 1000 * MiB })
    const timers = vi.spyOn(globalThis, 'setTimeout')
    const spool = new Spool({ dir: tmp.path, maxDiskBytes: 64 * KiB })
    const out = sink()
    out.pause()
    const done = pipeline(source(randomBytes(MiB)), spool, out.stream)
    await until(() => fileSize(spool) === 64 * KiB)
    await sleep(300)
    expect(pollTimers(timers)).toEqual([])
    out.resume()
    await done
  })

  it("polls minFreeSpace with a timer that doesn't hold the process open", async () => {
    fakeDisk({ total: 100 * MiB, free: MiB })
    const timers = vi.spyOn(globalThis, 'setTimeout')
    const spool = new Spool({ dir: tmp.path })
    const out = sink()
    out.pause()
    const done = pipeline(source(randomBytes(MiB)), spool, out.stream)
    await until(() => pollTimers(timers).length > 0)
    expect(pollTimers(timers).every((timer) => !timer.hasRef())).toBe(true)
    out.resume()
    await done
  })

  it('lets the process exit while a spool waits on a disk limit', () => {
    const result = runChild(`
      import { Spool } from ${JSON.stringify(entry)}
      // Neither spool has a consumer, so both wait for space forever.
      const capped = new Spool({ dir: ${JSON.stringify(tmp.path)}, maxDiskBytes: 1024, minFreeSpace: false })
      capped.write(Buffer.alloc(4096))
      const floored = new Spool({ dir: ${JSON.stringify(tmp.path)}, minFreeSpace: Number.MAX_SAFE_INTEGER })
      floored.write(Buffer.alloc(4096))
      setTimeout(() => console.log('waiting'), 600)
    `)
    expect(result.status).toBe(0)
    expect(result.stdout.trim()).toBe('waiting')
    expect(readdirSync(tmp.path)).toEqual([])
  })
})

describe('Spool.from() errors', () => {
  const failingSource = () => {
    let sent = 0
    return new Readable({
      read() {
        if (sent++ < 4) this.push(randomBytes(16 * KiB))
        else this.destroy(new Error('socket reset'))
      },
    })
  }

  it('surfaces a source error to a .pipe() consumer as an uncaught error, not silence', () => {
    const result = runChild(`
      import { Readable, Writable } from 'node:stream'
      import { Spool } from ${JSON.stringify(entry)}
      const failing = new Readable({ read() { this.destroy(new Error('socket reset')) } })
      Spool.from(failing, { dir: ${JSON.stringify(tmp.path)} }).pipe(new Writable({ write(c, e, cb) { cb() } }))
    `)
    expect(result.status).not.toBe(0)
    expect(result.stderr).toMatch(/socket reset/)
  })

  it("emits a source error once to an 'error' listener on a .pipe()d spool", async () => {
    const spool = Spool.from(failingSource(), { dir: tmp.path })
    const errors: Error[] = []
    spool.on('error', (error) => errors.push(error))
    spool.pipe(new Writable({ write: (_chunk, _encoding, done) => done() }))
    await closed(spool)
    expect(errors.map((e) => e.message)).toEqual(['socket reset'])
    expect(await readdir(tmp.path)).toEqual([])
  })

  it('rejects a for await loop with the source error', async () => {
    const consume = async () => {
      for await (const _chunk of Spool.from(failingSource(), { dir: tmp.path })) {
        // drain
      }
    }
    await expect(consume()).rejects.toThrow('socket reset')
  })

  it('destroys the source when the consumer fails', async () => {
    const src = new Readable({ read() {} })
    src.push(Buffer.from('x'))
    const failing = new Writable({ write: (_chunk, _encoding, done) => done(new Error('disk full')) })
    await expect(pipeline(Spool.from(src, { dir: tmp.path }), failing)).rejects.toThrow('disk full')
    await until(() => src.destroyed)
  })

  it('keeps the backlog when a Duplex source is destroyed after its readable side ends', async () => {
    // Socket-like: the readable side ends while the writable side is still
    // open, then the app destroys it, as `socket.destroy()` after 'end' does.
    const data = randomBytes(MiB)
    const src = new Duplex({ read() {}, write: (_chunk, _encoding, done) => done() })
    for (let i = 0; i < data.length; i += 16 * KiB) src.push(data.subarray(i, i + 16 * KiB))
    src.push(null)
    src.on('end', () => setTimeout(() => src.destroy(), 5))
    const spool = Spool.from(src, { dir: tmp.path, minFreeSpace: false })
    const errors: Error[] = []
    spool.on('error', (error) => errors.push(error))
    const out = sink()
    out.pause()
    spool.pipe(out.stream)
    await until(() => src.destroyed)
    await sleep(20)
    out.resume()
    await closed(spool)
    expect(errors).toEqual([])
    await until(() => out.stream.writableFinished)
    expect(out.received.equals(data)).toBe(true)
  })

  it('still fails when a Duplex source closes before its readable side ends', async () => {
    const src = new Duplex({ read() {}, write: (_chunk, _encoding, done) => done() })
    src.push(Buffer.from('partial'))
    const spool = Spool.from(src, { dir: tmp.path })
    const errors: Error[] = []
    spool.on('error', (error) => errors.push(error))
    spool.resume()
    await sleep(5)
    src.destroy()
    await closed(spool)
    expect(errors.map((e) => (e as NodeJS.ErrnoException).code)).toEqual(['ERR_STREAM_PREMATURE_CLOSE'])
  })

  it('reads an old-style stream', async () => {
    const legacy = new Stream() as Stream & NodeJS.ReadableStream
    const out = sink()
    const done = pipeline(Spool.from(legacy, { dir: tmp.path }), out.stream)
    legacy.emit('data', Buffer.from('old '))
    legacy.emit('data', Buffer.from('school'))
    legacy.emit('end')
    await done
    expect(out.received.toString()).toBe('old school')
  })

  it.each([null, undefined, 42, {}])(
    "rejects %o before creating a spool, so it can't leak a signal listener",
    (bad) => {
      const controller = new AbortController()
      expect(() => Spool.from(bad as never, { signal: controller.signal })).toThrow(TypeError)
      expect(() => Spool.from(bad as never)).toThrow(/^syke: Spool\.from\(\) needs/)
      expect(getEventListeners(controller.signal, 'abort')).toHaveLength(0)
    },
  )

  it('accepts options typed as plain SpoolOptions', async () => {
    const options: SpoolOptions = { dir: tmp.path, objectMode: true }
    const records: unknown[] = []
    await pipeline(
      Spool.from([{ a: 1 }, { b: 2 }], options),
      new Writable({
        objectMode: true,
        write: (record, _encoding, done) => {
          records.push(record)
          done()
        },
      }),
    )
    expect(records).toEqual([{ a: 1 }, { b: 2 }])
  })
})

describe('spilling while destroyed', () => {
  it("doesn't create a file once destroyed while free space was being read", async () => {
    let release: () => void = () => {}
    vi.mocked(statfs).mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          release = () =>
            resolve({ type: 0xef53, bsize: 1, blocks: 1e12, bfree: 1e12, bavail: 1e12, files: 0, ffree: 0 } as never)
        }),
    )
    const spool = new Spool({ dir: tmp.path })
    spool.write(Buffer.alloc(1000))
    await until(() => vi.mocked(statfs).mock.calls.length > 0)

    spool.destroy()
    release()
    await closed(spool)
    expect(open).not.toHaveBeenCalled()
    expect(spool.filepath).toBeNull()
    expect(await readdir(tmp.path)).toEqual([])
  })

  it('removes a file whose open finished after the spool was destroyed', async () => {
    const actual = vi.mocked(open).getMockImplementation() as typeof open
    let opened: () => void = () => {}
    const gate = new Promise<void>((resolve) => {
      opened = resolve
    })
    vi.mocked(open).mockImplementationOnce(async (...args) => {
      await gate
      return actual(...args)
    })
    const spool = new Spool({ dir: tmp.path, minFreeSpace: false })
    spool.write(Buffer.alloc(1000))
    await until(() => vi.mocked(open).mock.calls.length > 0)
    spool.destroy()
    opened()
    await closed(spool)
    expect(await readdir(tmp.path)).toEqual([])
  })

  it('fails instead of spinning when a write makes no progress', async () => {
    vi.spyOn(FileHandleProto, 'write').mockResolvedValue({ bytesWritten: 0, buffer: Buffer.alloc(0) } as never)
    const spool = new Spool({ dir: tmp.path, minFreeSpace: false })
    const out = sink()
    out.pause()
    await expect(pipeline(source(randomBytes(256 * KiB)), spool, out.stream)).rejects.toThrow(/made no progress/)
    await closed(spool)
    expect(await readdir(tmp.path)).toEqual([])
  })

  it('leaves filepath null when the file could not be created', async () => {
    const spool = new Spool({ dir: join(tmp.path, 'missing') })
    const out = sink()
    out.pause()
    await expect(pipeline(source(randomBytes(256 * KiB)), spool, out.stream)).rejects.toThrow(/ENOENT/)
    expect(spool.filepath).toBeNull()
  })
})

describe('running out of space (ENOSPC)', () => {
  it("fails with SpoolLimitError when onLimit is 'error'", async () => {
    vi.spyOn(FileHandleProto, 'write').mockRejectedValue(noSpace())
    const spool = new Spool({ dir: tmp.path, minFreeSpace: false, onLimit: 'error' })
    const out = sink()
    out.pause()
    const error = await pipeline(source(randomBytes(256 * KiB)), spool, out.stream).catch((e) => e)
    expect(error).toBeInstanceOf(SpoolLimitError)
    expect(error.message).toMatch(/out of space/)
    expect(error.cause).toMatchObject({ code: 'ENOSPC' })
    await closed(spool)
    expect(await readdir(tmp.path)).toEqual([])
  })

  it('applies backpressure until a write succeeds, without losing or reordering data', async () => {
    const write = FileHandleProto.write
    let calls = 0
    vi.spyOn(FileHandleProto, 'write').mockImplementation(function (this: FileHandle, ...args: unknown[]) {
      calls++
      // The second write lands half, then the filesystem fills up once.
      if (calls === 2) {
        const [buffer, offset, length, position] = args as [Buffer, number, number, number]
        return (write as (...a: unknown[]) => Promise<unknown>).call(this, buffer, offset, Math.ceil(length / 2), position)
      }
      if (calls === 3) return Promise.reject(noSpace())
      return (write as (...a: unknown[]) => Promise<unknown>).apply(this, args)
    } as never)
    const data = randomBytes(MiB)
    const spool = new Spool({ dir: tmp.path, minFreeSpace: false })
    const out = sink()
    out.pause()
    const done = pipeline(source(data), spool, out.stream)
    await until(() => spool.writableFinished)
    expect(calls).toBeGreaterThan(3)
    out.resume()
    await done
    expect(out.received.equals(data)).toBe(true)
  })

  it('retries creating the file when there was no room for it', async () => {
    const actual = vi.mocked(open).getMockImplementation() as typeof open
    vi.mocked(open).mockImplementationOnce(() => Promise.reject(noSpace()))
    vi.mocked(open).mockImplementation(actual)
    const data = randomBytes(256 * KiB)
    const spool = new Spool({ dir: tmp.path, minFreeSpace: false })
    const out = sink()
    out.pause()
    const done = pipeline(source(data), spool, out.stream)
    await until(() => spool.writableFinished)
    expect(open).toHaveBeenCalledTimes(2)
    out.resume()
    await done
    expect(out.received.equals(data)).toBe(true)
  })
})

describe('sharing free space between spools', () => {
  it("doesn't let parallel spools in one directory spend the same free space", async () => {
    fakeDisk({ total: 100 * MiB, free: 10 * MiB })
    const floor = 10 * MiB - 512 * KiB
    const spools = [0, 1, 2].map(() => new Spool({ dir: tmp.path, minFreeSpace: floor }))
    const outs = spools.map(() => sink())
    const data = spools.map(() => randomBytes(MiB))
    for (const out of outs) out.pause()
    const done = spools.map((spool, i) =>
      pipeline(source(data[i] as Buffer, [16 * KiB]), spool, (outs[i] as ReturnType<typeof sink>).stream),
    )

    await until(() => spools.reduce((n, s) => n + fileSize(s), 0) >= 512 * KiB - 48 * KiB)
    await sleep(300)
    expect(spools.reduce((n, s) => n + fileSize(s), 0)).toBeLessThanOrEqual(512 * KiB)

    for (const out of outs) out.resume()
    await Promise.all(done)
    outs.forEach((out, i) => expect(out.received.equals(data[i] as Buffer)).toBe(true))
  })
})

describe('high-water marks of 0', () => {
  it.each<SpoolOptions>([{ highWaterMark: 0 }, { readableHighWaterMark: 0 }])(
    'sizes reads by the default instead of failing with %o',
    async (options) => {
      const data = randomBytes(512 * KiB)
      const spool = new Spool({ dir: tmp.path, ...options })
      const out = sink()
      out.pause()
      const done = pipeline(source(data), spool, out.stream)
      await until(() => spool.writableFinished)
      out.resume()
      await done
      expect(out.received.equals(data)).toBe(true)
    },
  )

  it('still honours an explicit chunkSize', async () => {
    const data = randomBytes(64 * KiB)
    const spool = new Spool({ dir: tmp.path, highWaterMark: 0, chunkSize: 1000 })
    const out = sink()
    out.pause()
    const done = pipeline(source(data), spool, out.stream)
    await until(() => spool.writableFinished)
    out.resume()
    await done
    expect(out.received.equals(data)).toBe(true)
    expect(Math.max(...out.chunks.map((c) => c.length))).toBeLessThanOrEqual(1000)
  })
})

describe('stream options', () => {
  it('ignores Duplex options Spool does not document', async () => {
    const spool = new Spool({
      dir: tmp.path,
      readableObjectMode: true,
      autoDestroy: false,
      emitClose: false,
    } as SpoolOptions)
    expect(spool.readableObjectMode).toBe(false)
    const out = sink()
    await pipeline(Readable.from([Buffer.from('ab')]), spool, out.stream)
    await closed(spool)
    expect(spool.destroyed).toBe(true)
  })

  it('names the allowed onLimit values', () => {
    expect(() => new Spool({ onLimit: 'drop' as never })).toThrow(/onLimit must be 'backpressure' or 'error', got drop/)
  })
})

describe('jsonCodec in a spool', () => {
  it('fails on the write of a record JSON cannot carry, not on reading it back', async () => {
    const spool = new Spool({ objectMode: true, dir: tmp.path, codec: jsonCodec })
    spool.write({ ok: true })
    // No consumer, so `undefined` must be encoded to spill.
    const error = await new Promise<Error>((resolve) => {
      spool.on('error', resolve)
      spool.write(undefined)
    })
    expect(error).toBeInstanceOf(SpoolCodecError)
    expect(error.message).toMatch(/failed to encode/)
  })
})

describe('reclaiming disk space', () => {
  async function drain(out: ReturnType<typeof sink>, total: number) {
    out.resume()
    await until(() => out.received.length === total)
  }

  it('truncates the spool file once the backlog drains, then spills from the start again', async () => {
    const data = randomBytes(MiB + 256 * KiB)
    const spool = new Spool({ dir: tmp.path })
    const src = new Readable({ read() {} })
    const out = sink()
    out.pause()
    const done = pipeline(src, spool, out.stream)

    src.push(data.subarray(0, MiB))
    await until(() => fileSize(spool) >= MiB - 64 * KiB && spool.writableLength === 0)
    await drain(out, MiB)
    await until(() => fileSize(spool) === 0)

    out.pause()
    src.push(data.subarray(MiB))
    await until(() => src.readableLength === 0 && spool.writableLength === 0)
    expect(fileSize(spool)).toBeLessThanOrEqual(256 * KiB)
    src.push(null)
    out.resume()
    await done
    expect(out.received.equals(data)).toBe(true)
  })

  it('holds a spill that arrives mid-truncate until the truncate finishes', async () => {
    const truncate = FileHandleProto.truncate
    let truncating = false
    let truncates = 0
    vi.spyOn(FileHandleProto, 'truncate').mockImplementation(async function (this: FileHandle, length?: number) {
      truncates++
      truncating = true
      await sleep(50)
      truncating = false
      return truncate.call(this, length)
    })
    const K = 16 * KiB
    const spool = new Spool({ dir: tmp.path, chunkSize: K, highWaterMark: K, minFreeSpace: false })
    const errors: Error[] = []
    spool.on('error', (error) => errors.push(error))
    const first = randomBytes(8 * K)
    const second = randomBytes(4 * K)
    spool.write(first)
    await until(() => spool.writableLength === 0)

    // Read the backlog until its last chunk sits in the full readable buffer:
    // the file is drained and being truncated, and the spool doesn't want
    // data, so the next write must spill rather than pass straight through.
    const received: Buffer[] = []
    let total = 0
    while (total < 7 * K || spool.readableLength < K) {
      const chunk = spool.read() as Buffer | null
      if (chunk) {
        received.push(chunk)
        total += chunk.length
      } else {
        await sleep(1)
      }
    }
    await until(() => truncating)
    spool.write(second)
    spool.end()
    expect(truncating).toBe(true)
    // Read nothing more until the truncate is done, or a spill that raced it
    // could be read back before the file is cut.
    await until(() => !truncating)
    await sleep(10)

    for await (const chunk of spool) received.push(chunk as Buffer)
    expect(errors).toEqual([])
    expect(truncates).toBe(1)
    expect(Buffer.concat(received).equals(Buffer.concat([first, second]))).toBe(true)
  })
})

describe('write batching', () => {
  it('coalesces small chunks queued behind a spill into fewer, larger writes', async () => {
    const write = vi.spyOn(FileHandleProto, 'write')
    const data = randomBytes(MiB)
    // chunkSize defaults to the high-water mark, which is 16 KiB on Windows
    // and 64 KiB elsewhere, so pin it.
    const spool = new Spool({ dir: tmp.path, minFreeSpace: false, chunkSize: 64 * KiB })
    await spillWithoutConsumer(spool, data, 512)
    // 2048 chunks of 512 bytes; 64 KiB writes need about 16.
    expect(write.mock.calls.length).toBeLessThan(64)

    const out = sink()
    await pipeline(spool, out.stream)
    expect(out.received.equals(data)).toBe(true)
  })

  it('frames a batch of records into one write', async () => {
    const write = vi.spyOn(FileHandleProto, 'write')
    const records = Array.from({ length: 2000 }, (_, i) => ({ i, s: 'x'.repeat(i % 40) }))
    const spool = new Spool({ objectMode: true, dir: tmp.path, minFreeSpace: false })
    for (const record of records) spool.write(record)
    spool.end()
    await until(() => spool.writableFinished)
    expect(write.mock.calls.length).toBeLessThan(20)

    const received: unknown[] = []
    await pipeline(
      spool,
      new Writable({
        objectMode: true,
        write: (record, _encoding, done) => {
          received.push(record)
          done()
        },
      }),
    )
    expect(received).toEqual(records)
  })

  it("keeps each record's bytes when the codec reuses one output buffer", async () => {
    // A common codec optimization: encode into a scratch buffer and return a view of it.
    const scratch = new Uint8Array(1024)
    const text = new TextEncoder()
    const codec = {
      encode: (value: unknown) => scratch.subarray(0, text.encodeInto(JSON.stringify(value), scratch).written),
      decode: (bytes: Uint8Array) => JSON.parse(new TextDecoder().decode(bytes)),
    }
    const records = Array.from({ length: 2000 }, (_, i) => ({ i: 1000 + i, pad: 'x'.repeat(i % 7) }))
    const spool = new Spool({ objectMode: true, codec, dir: tmp.path, minFreeSpace: false })
    for (const record of records) spool.write(record)
    spool.end()
    await until(() => spool.writableFinished)
    expect(spool.filepath).not.toBeNull()

    const received: unknown[] = []
    await pipeline(
      spool,
      new Writable({
        objectMode: true,
        write: (record, _encoding, done) => {
          received.push(record)
          done()
        },
      }),
    )
    expect(received).toEqual(records)
  })

  it('writes a record larger than chunkSize whole, between batches', async () => {
    const records = [{ s: 'a' }, { s: 'b'.repeat(4 * KiB) }, { s: 'c' }, { s: 'd'.repeat(300) }, { s: 'e' }]
    const spool = new Spool({ objectMode: true, dir: tmp.path, chunkSize: 256, codec: jsonCodec, minFreeSpace: false })
    for (const record of records) spool.write(record)
    spool.end()
    await until(() => spool.writableFinished)

    const received: unknown[] = []
    await pipeline(
      spool,
      new Writable({
        objectMode: true,
        write: (record, _encoding, done) => {
          received.push(record)
          done()
        },
      }),
    )
    expect(received).toEqual(records)
  })
})

describe('cleanup', () => {
  it('still deletes the file when closing it fails', async () => {
    // `close` is an own property of each FileHandle, so patch the spool's.
    const actual = vi.mocked(open).getMockImplementation() as typeof open
    vi.mocked(open).mockImplementationOnce(async (...args) => {
      const handle = await actual(...args)
      const close = handle.close
      handle.close = async () => {
        await close()
        throw Object.assign(new Error('EIO: close failed'), { code: 'EIO' })
      }
      return handle
    })
    const spool = new Spool({ dir: tmp.path, minFreeSpace: false })
    spool.write(randomBytes(64 * KiB))
    await until(() => fileSize(spool) > 0)

    const errors: Error[] = []
    spool.on('error', (error) => errors.push(error))
    spool.destroy()
    await closed(spool)
    expect(errors.map((e) => e.message)).toEqual(['EIO: close failed'])
    expect(await readdir(tmp.path)).toEqual([])
  })

  it('removes the file if the process exits while it is being closed', () => {
    const result = runChild(`
      import fsp from 'node:fs/promises'
      import { statSync } from 'node:fs'
      import { syncBuiltinESMExports } from 'node:module'

      // Every handle the spool opens exits the process as it's closed,
      // before cleanup can delete the file.
      const open = fsp.open
      fsp.open = async (...args) => {
        const handle = await open(...args)
        handle.close = () => {
          console.log(args[0])
          process.exit(0)
        }
        return handle
      }
      syncBuiltinESMExports()
      const { Spool } = await import(${JSON.stringify(entry)})

      const spool = new Spool({ dir: ${JSON.stringify(tmp.path)}, minFreeSpace: false })
      spool.write(Buffer.alloc(64 * 1024))
      while (!spool.filepath || !(statSync(spool.filepath, { throwIfNoEntry: false })?.size > 0)) {
        await new Promise((resolve) => setTimeout(resolve, 5))
      }
      spool.destroy()
    `)
    expect(result.status).toBe(0)
    expect(result.stdout.trim()).toMatch(/syke-spool-/)
    expect(readdirSync(tmp.path)).toEqual([])
  })
})
