import { readdir } from 'node:fs/promises'
import { Readable, Writable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { ReadableStream } from 'node:stream/web'
import { describe, expect, it } from 'vitest'
import {
  type Codec,
  Spool,
  SpoolCodecError,
  SpoolLimitError,
  jsonCodec,
  v8Codec,
} from '../../../src/node.ts'
import { closed, fileSize, sleep, until, useTempDir, watchFileSize } from '../../nodeHelpers.ts'

const tmp = useTempDir()

/** An object-mode consumer that records what it gets and can be paused. */
function recordSink() {
  const records: unknown[] = []
  let release: (() => void) | null = null
  let paused: Promise<void> | null = null
  const stream = new Writable({
    objectMode: true,
    highWaterMark: 1,
    write(record, _encoding, done) {
      records.push(record)
      if (paused) paused.then(() => done())
      else done()
    },
  })
  return {
    stream,
    records,
    pause() {
      paused ??= new Promise((resolve) => {
        release = resolve
      })
    },
    resume() {
      paused = null
      release?.()
    },
  }
}

/** Wraps `codec`, counting its calls. */
function counting(codec: Codec = v8Codec) {
  const counts = { encode: 0, decode: 0 }
  const wrapped: Codec = {
    encode(value) {
      counts.encode++
      return codec.encode(value)
    },
    decode(bytes) {
      counts.decode++
      return codec.decode(bytes)
    },
  }
  return { codec: wrapped, counts }
}

const finished = (spool: Spool) => new Promise((resolve) => spool.on('finish', resolve))
/** Like a database cursor: yields records in batches, waiting for I/O between them. */
async function* cursor<T>(records: T[], batch = 100) {
  for (let i = 0; i < records.length; i += batch) {
    await new Promise((resolve) => setImmediate(resolve))
    yield* records.slice(i, i + batch)
  }
}

const numbered = (n: number) => Array.from({ length: n }, (_, i) => ({ i, pad: 'x'.repeat(i % 97) }))

const varied: unknown[] = [
  { id: 1, name: 'ada', tags: ['a', 'b'], nested: { deep: { deeper: true } } },
  new Date('2024-02-29T12:34:56.789Z'),
  new Map<unknown, unknown>([
    ['k', 1],
    [2, { v: 'two' }],
  ]),
  new Set([1, 'two', 3n]),
  new Uint8Array([0, 1, 254, 255]),
  new Float64Array([Math.PI, -0, Number.NaN]),
  Buffer.from('a buffer record'),
  'plain string',
  '',
  0,
  -1.5,
  12345678901234567890n,
  true,
  false,
  undefined,
  [null, undefined, [1, [2, [3]]]],
  { when: new Date(0), missing: undefined, re: /ab+c/gi },
  'é'.repeat(5000),
]

describe('Spool in object mode', () => {
  it('round-trips varied records that spill to disk while the consumer is stalled', async () => {
    const records = Array.from({ length: 20 }, () => varied).flat()
    const spool = new Spool({ objectMode: true, dir: tmp.path, chunkSize: 256 })
    const out = recordSink()
    out.pause()

    const done = pipeline(Readable.from(records), spool, out.stream)
    // The source is fully accepted while the consumer is stalled.
    await finished(spool)
    expect(spool.filepath).not.toBeNull()
    out.resume()
    await done

    expect(out.records).toEqual(records)
    await closed(spool)
    expect(await readdir(tmp.path)).toEqual([])
  })

  it('preserves order across pass-through, memory and disk', async () => {
    const records = numbered(20_000)
    const { codec, counts } = counting()
    const spool = new Spool({ objectMode: true, dir: tmp.path, codec, memoryThreshold: 50, chunkSize: 2000 })
    const out = recordSink()

    // Stall and release the consumer repeatedly so records move between
    // pass-through, memory and disk.
    let stalls = 0
    const timer = setInterval(() => {
      if (stalls++ % 2 === 0) out.pause()
      else out.resume()
    }, 2)
    try {
      await pipeline(Readable.from(records), spool, out.stream)
    } finally {
      clearInterval(timer)
    }

    expect(out.records).toEqual(records)
    // Some records spilled, but not all of them.
    expect(spool.filepath).not.toBeNull()
    expect(counts.encode).toBeGreaterThan(0)
    expect(counts.encode).toBeLessThan(records.length)
    expect(counts.decode).toBe(counts.encode)
  })

  it('never calls the codec while the consumer keeps up', async () => {
    const records = numbered(5000)
    const { codec, counts } = counting()
    const spool = new Spool({ objectMode: true, dir: tmp.path, codec })
    const out = recordSink()

    await pipeline(Readable.from(cursor(records)), spool, out.stream)

    expect(out.records).toEqual(records)
    expect(counts).toEqual({ encode: 0, decode: 0 })
    expect(spool.filepath).toBeNull()
  })

  it('passes records through as the same objects, not copies', async () => {
    const records = numbered(100)
    const out = recordSink()
    await pipeline(Readable.from(cursor(records)), new Spool({ objectMode: true, dir: tmp.path }), out.stream)
    out.records.forEach((record, i) => expect(record).toBe(records[i]))
  })

  it('holds up to memoryThreshold records in memory without the codec', async () => {
    const records = numbered(500)
    const { codec, counts } = counting()
    const spool = new Spool({ objectMode: true, dir: tmp.path, codec, memoryThreshold: 1000 })
    const out = recordSink()
    out.pause()

    const done = pipeline(Readable.from(records), spool, out.stream)
    await finished(spool)
    expect(spool.filepath).toBeNull()
    out.resume()
    await done

    expect(out.records).toEqual(records)
    expect(counts).toEqual({ encode: 0, decode: 0 })
  })

  it('counts memoryThreshold in records', async () => {
    const records = numbered(500)
    const { codec, counts } = counting()
    const spool = new Spool({ objectMode: true, dir: tmp.path, codec, memoryThreshold: 100 })
    const out = recordSink()
    out.pause()

    const done = pipeline(Readable.from(records), spool, out.stream)
    await finished(spool)
    out.resume()
    await done

    expect(out.records).toEqual(records)
    // Some went straight to the consumer's buffers, 100 waited in memory, and
    // everything else spilled.
    expect(counts.encode).toBeGreaterThan(300)
    expect(counts.encode).toBeLessThanOrEqual(400)
  })

  it('uses a custom codec for spilled records', async () => {
    // Fixed-width binary rows: 4-byte id, 8-byte score.
    type Row = { id: number; score: number }
    const rowCodec: Codec<Row> = {
      encode(row) {
        const bytes = new Uint8Array(12)
        const view = new DataView(bytes.buffer)
        view.setUint32(0, row.id)
        view.setFloat64(4, row.score)
        return bytes
      },
      decode(bytes) {
        const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
        return { id: view.getUint32(0), score: view.getFloat64(4) }
      },
    }
    const records: Row[] = Array.from({ length: 3000 }, (_, id) => ({ id, score: id / 7 }))
    const spool = new Spool({ objectMode: true, dir: tmp.path, codec: rowCodec, chunkSize: 1000 })
    const out = recordSink()
    out.pause()

    const done = pipeline(Readable.from(records), spool, out.stream)
    await finished(spool)
    expect(spool.filepath).not.toBeNull()
    out.resume()
    await done

    expect(out.records).toEqual(records)
  })

  it('accepts jsonCodec, which only changes the records that spill', async () => {
    const spool = new Spool({ objectMode: true, dir: tmp.path, codec: jsonCodec })
    const out = recordSink()
    out.pause()
    const records = numbered(200).map((r) => ({ ...r, at: new Date(r.i) }))

    const done = pipeline(Readable.from(cursor(records)), spool, out.stream)
    await finished(spool)
    out.resume()
    await done

    const first = out.records[0] as { at: unknown }
    const last = out.records.at(-1) as { at: unknown }
    // The first record passed straight through; the last one spilled as JSON.
    expect(first.at).toBeInstanceOf(Date)
    expect(last.at).toBe(new Date(199).toISOString())
  })

  it('reads back records many times larger than chunkSize', async () => {
    const records = [
      { small: 1 },
      { big: 'a'.repeat(100_000) },
      Buffer.alloc(250_000, 7),
      { small: 2 },
      { big: 'b'.repeat(50_001) },
      { small: 3 },
    ]
    const spool = new Spool({ objectMode: true, dir: tmp.path, chunkSize: 1024 })
    const out = recordSink()
    out.pause()

    let peakBuffered = 0
    spool.on('data', () => {
      peakBuffered = Math.max(peakBuffered, spool.readableLength)
    })

    const done = pipeline(Readable.from(records), spool, out.stream)
    await finished(spool)
    out.resume()
    await done

    expect(out.records).toEqual(records)
    expect(peakBuffered).toBeLessThanOrEqual(spool.readableHighWaterMark)
  })

  it('follows object-mode high-water marks and sizes reads in bytes', () => {
    const spool = new Spool({ objectMode: true })
    expect(spool.readableObjectMode).toBe(true)
    expect(spool.writableObjectMode).toBe(true)
    expect(spool.readableHighWaterMark).toBe(16)
    expect(spool.writableHighWaterMark).toBe(16)
    spool.destroy()
  })

  it('carries undefined', async () => {
    const records = [1, undefined, 2, undefined]
    const spool = new Spool({ objectMode: true, dir: tmp.path })
    const out = recordSink()
    out.pause()
    const done = pipeline(Readable.from(records), spool, out.stream)
    await finished(spool)
    out.resume()
    await done
    expect(out.records).toEqual(records)
  })

  it('ends an empty stream without touching disk', async () => {
    const spool = new Spool({ objectMode: true, dir: tmp.path })
    const out = recordSink()
    await pipeline(Readable.from([]), spool, out.stream)
    expect(out.records).toEqual([])
    expect(spool.filepath).toBeNull()
  })
})

describe('null in object mode', () => {
  it('rejects a null write instead of ending the stream', () => {
    const spool = new Spool({ objectMode: true })
    expect(() => spool.write(null)).toThrow(expect.objectContaining({ code: 'ERR_STREAM_NULL_VALUES' }))
    spool.destroy()
  })

  it('fails Spool.from() on a null record instead of dropping the rest', async () => {
    const out = recordSink()
    const spool = Spool.from([1, null, 2], { objectMode: true, dir: tmp.path })
    await expect(pipeline(spool, out.stream)).rejects.toMatchObject({ code: 'ERR_STREAM_NULL_VALUES' })
  })

  it('fails when a codec decodes a record to null', async () => {
    const nulling: Codec = { encode: v8Codec.encode, decode: () => null }
    const spool = new Spool({ objectMode: true, dir: tmp.path, codec: nulling })
    const out = recordSink()
    out.pause()

    // Reading the first spilled chunk fails, before the source even finishes.
    const error = await pipeline(Readable.from(numbered(100)), spool, out.stream).catch((e) => e)
    expect(error).toBeInstanceOf(SpoolCodecError)
    expect(error.message).toMatch(/null/)
    await closed(spool)
    expect(await readdir(tmp.path)).toEqual([])
  })
})

describe('object-mode disk limits', () => {
  it('pauses the source at maxDiskBytes, then resumes without reordering', async () => {
    const cap = 20_000
    const records = numbered(5000)
    const spool = new Spool({ objectMode: true, dir: tmp.path, maxDiskBytes: cap, minFreeSpace: false, chunkSize: 4000 })
    const out = recordSink()
    out.pause()
    let done_ = false
    spool.on('finish', () => {
      done_ = true
    })

    const done = pipeline(Readable.from(records), spool, out.stream)
    await until(() => fileSize(spool) === cap)
    await sleep(50)
    expect(fileSize(spool)).toBe(cap)
    expect(done_).toBe(false)

    const size = watchFileSize(spool)
    out.resume()
    await done

    expect(size.stop()).toBeLessThanOrEqual(cap)
    expect(out.records).toEqual(records)
  })

  it('wraps around a small cap with stop-start consumption, without reordering', async () => {
    const cap = 3000
    const records = numbered(20_000)
    const spool = new Spool({ objectMode: true, dir: tmp.path, maxDiskBytes: cap, minFreeSpace: false, chunkSize: 700 })
    const out = recordSink()
    const size = watchFileSize(spool)

    let stalls = 0
    const timer = setInterval(() => {
      if (stalls++ % 2 === 0) out.pause()
      else out.resume()
    }, 2)
    try {
      await pipeline(Readable.from(records), spool, out.stream)
    } finally {
      clearInterval(timer)
    }

    expect(size.stop()).toBeLessThanOrEqual(cap)
    expect(out.records).toEqual(records)
  })

  it('delivers records larger than maxDiskBytes', async () => {
    const records = [{ a: 1 }, 'x'.repeat(10_000), { b: 2 }, Buffer.alloc(7000, 1), { c: 3 }]
    const spool = new Spool({ objectMode: true, dir: tmp.path, maxDiskBytes: 1000, minFreeSpace: false, chunkSize: 300 })
    const out = recordSink()
    out.pause()

    const done = pipeline(Readable.from(records), spool, out.stream)
    await until(() => fileSize(spool) === 1000)
    const size = watchFileSize(spool)
    out.resume()
    await done

    expect(size.stop()).toBeLessThanOrEqual(1000)
    expect(out.records).toEqual(records)
  })

  it("fails with SpoolLimitError when onLimit is 'error'", async () => {
    const spool = new Spool({
      objectMode: true,
      dir: tmp.path,
      maxDiskBytes: 10_000,
      minFreeSpace: false,
      onLimit: 'error',
    })
    const out = recordSink()
    out.pause()

    const error = await pipeline(Readable.from(numbered(5000)), spool, out.stream).catch((e) => e)
    expect(error).toBeInstanceOf(SpoolLimitError)
    await closed(spool)
    expect(await readdir(tmp.path)).toEqual([])
  })
})

describe('object-mode cleanup', () => {
  it('removes the spool file when the source errors', async () => {
    const spool = new Spool({ objectMode: true, dir: tmp.path })
    const out = recordSink()
    out.pause()
    async function* failing() {
      yield* numbered(1000)
      await sleep(20)
      throw new Error('cursor timed out')
    }

    await expect(pipeline(Readable.from(failing()), spool, out.stream)).rejects.toThrow('cursor timed out')
    await closed(spool)
    expect(spool.filepath).not.toBeNull()
    expect(await readdir(tmp.path)).toEqual([])
  })

  it('removes the spool file when destroyed mid-stream', async () => {
    const spool = new Spool({ objectMode: true, dir: tmp.path })
    const out = recordSink()
    out.pause()
    const done = pipeline(Readable.from(numbered(2000)), spool, out.stream)
    await finished(spool)
    expect(await readdir(tmp.path)).toHaveLength(1)

    spool.destroy()
    await expect(done).rejects.toThrow()
    await closed(spool)
    expect(await readdir(tmp.path)).toEqual([])
  })

  it('fails with SpoolCodecError, with the cause, when encoding throws', async () => {
    const cause = new Error('cannot encode this one')
    const picky: Codec = {
      encode: (value) => {
        if ((value as { i: number }).i === 500) throw cause
        return v8Codec.encode(value)
      },
      decode: v8Codec.decode,
    }
    const spool = new Spool({ objectMode: true, dir: tmp.path, codec: picky })
    const out = recordSink()
    out.pause()

    const error = await pipeline(Readable.from(numbered(1000)), spool, out.stream).catch((e) => e)
    expect(error).toBeInstanceOf(SpoolCodecError)
    expect(error).toMatchObject({ code: 'ERR_SYKE_SPOOL_CODEC', cause })
    expect(error.message).toMatch(/^syke: .*cannot encode this one/)
    await closed(spool)
    expect(await readdir(tmp.path)).toEqual([])
  })

  it('fails with SpoolCodecError when v8 cannot serialize a record', async () => {
    const spool = new Spool({ objectMode: true, dir: tmp.path })
    const out = recordSink()
    out.pause()
    const records = [...numbered(100), { fn: () => {} }]

    const error = await pipeline(Readable.from(records), spool, out.stream).catch((e) => e)
    expect(error).toBeInstanceOf(SpoolCodecError)
  })

  it('fails with SpoolCodecError when encode returns something other than bytes', async () => {
    const wrong = { encode: (value: unknown) => JSON.stringify(value), decode: jsonCodec.decode }
    const spool = new Spool({ objectMode: true, dir: tmp.path, codec: wrong as never })
    const out = recordSink()
    out.pause()

    const error = await pipeline(Readable.from(numbered(100)), spool, out.stream).catch((e) => e)
    expect(error).toBeInstanceOf(SpoolCodecError)
    expect(error.message).toMatch(/Uint8Array, got string/)
  })

  it('fails with SpoolCodecError when decoding throws', async () => {
    const broken: Codec = {
      encode: v8Codec.encode,
      decode: () => {
        throw new Error('bad bytes')
      },
    }
    const spool = new Spool({ objectMode: true, dir: tmp.path, codec: broken })
    const out = recordSink()
    out.pause()

    // Reading the first spilled chunk fails, before the source even finishes.
    const error = await pipeline(Readable.from(numbered(100)), spool, out.stream).catch((e) => e)
    expect(error).toBeInstanceOf(SpoolCodecError)
    expect(error.cause).toMatchObject({ message: 'bad bytes' })
    await closed(spool)
    expect(await readdir(tmp.path)).toEqual([])
  })
})

describe('Spool.from() in object mode', () => {
  it('reads an async iterable of records without holding it back', async () => {
    const records = numbered(3000)
    async function* cursor() {
      yield* records
    }
    const spool = Spool.from(cursor(), { objectMode: true, dir: tmp.path })
    const out = recordSink()
    out.pause()
    const done = pipeline(spool, out.stream)

    await finished(spool)
    expect(spool.filepath).not.toBeNull()
    out.resume()
    await done
    expect(out.records).toEqual(records)
  })

  it('reads a sync iterable, an object-mode Node readable and a web ReadableStream', async () => {
    const records = [{ a: 1 }, 'two', [3], new Date(4)]
    const web = new ReadableStream({
      start(controller) {
        for (const record of records) controller.enqueue(record)
        controller.close()
      },
    })
    for (const source of [records, Readable.from(records), web]) {
      const out = recordSink()
      await pipeline(Spool.from(source, { objectMode: true, dir: tmp.path }), out.stream)
      expect(out.records).toEqual(records)
    }
  })
})

describe('object-mode option validation', () => {
  it('rejects a codec without objectMode', () => {
    expect(() => new Spool({ codec: jsonCodec })).toThrow(TypeError)
    expect(() => new Spool({ objectMode: false, codec: jsonCodec })).toThrow(TypeError)
  })

  it('rejects a codec without encode() and decode()', () => {
    expect(() => new Spool({ objectMode: true, codec: {} as never })).toThrow(TypeError)
    expect(() => new Spool({ objectMode: true, codec: null as never })).toThrow(TypeError)
  })

  it('rejects a non-boolean objectMode', () => {
    expect(() => new Spool({ objectMode: 'yes' as never })).toThrow(TypeError)
  })

  it('rejects invalid sizes as in byte mode', () => {
    expect(() => new Spool({ objectMode: true, chunkSize: 0 })).toThrow(RangeError)
    expect(() => new Spool({ objectMode: true, memoryThreshold: 1.5 })).toThrow(RangeError)
  })
})
