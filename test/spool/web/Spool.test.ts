import { randomBytes } from 'node:crypto'
import { describe, expect, it } from 'vitest'
import { chunk } from '../../harness.ts'
import { type FakeBrowser, freshPage, installFakeBrowser, useFakeBrowser, waitFor } from './fakeBrowser.ts'

const KiB = 1024
const MiB = 1024 * KiB

useFakeBrowser()

async function readAll<T>(readable: ReadableStream<T>): Promise<T[]> {
  const out: T[] = []
  for await (const value of readable as unknown as AsyncIterable<T>) out.push(value)
  return out
}

/** Writes every chunk without anyone reading: each write must still resolve. */
async function writeStalled<T>(writable: WritableStream<T>, chunks: Iterable<T>, close = true): Promise<void> {
  const writer = writable.getWriter()
  for (const piece of chunks) await writer.write(piece)
  if (close) await writer.close()
  writer.releaseLock()
}

async function expectNoLeftovers(browser: FakeBrowser) {
  expect(browser.opfsFiles()).toEqual([])
  expect(await browser.idbRecords()).toBe(0)
  expect(browser.workers.size).toBe(0)
}

describe.each(['opfs', 'indexeddb'] as const)('Spool in the browser (%s)', (storage) => {
  it('accepts the whole source while the consumer is stalled, then replays it in bounded chunks', async () => {
    const browser = installFakeBrowser()
    const { Spool } = await freshPage()
    const data = randomBytes(5 * MiB)
    const spool = new Spool({ storage, chunkSize: 32 * KiB })

    await writeStalled(spool.writable, chunk(data, 7000))
    expect(spool.storage).toBe(storage)

    const out = await readAll(spool.readable)
    expect(Buffer.concat(out).equals(data)).toBe(true)
    // The first chunk passed straight through; the rest came back in chunkSize pieces.
    expect(Math.max(...out.slice(1).map((piece) => piece.byteLength))).toBeLessThanOrEqual(32 * KiB)
    await spool.closed
    await expectNoLeftovers(browser)
  })

  it('keeps data in order when reads and writes interleave', async () => {
    installFakeBrowser()
    const { Spool } = await freshPage()
    const data = randomBytes(3 * MiB)
    const spool = new Spool({ storage, chunkSize: 10_000 })
    const writer = spool.writable.getWriter()
    const reader = spool.readable.getReader()
    const out: Uint8Array[] = []
    let offset = 0
    let sizes = 0
    while (offset < data.length) {
      const size = 1 + ((sizes++ * 7919) % 90_000)
      await writer.write(data.subarray(offset, offset + size))
      offset += size
      if (sizes % 3 === 0) {
        const { value } = await reader.read()
        if (value) out.push(value)
      }
    }
    await writer.close()
    for (;;) {
      const { value, done } = await reader.read()
      if (done) break
      out.push(value)
    }
    expect(Buffer.concat(out).equals(data)).toBe(true)
  })

  it('round-trips records in object mode, keeping them as objects when not spilled', async () => {
    const browser = installFakeBrowser()
    const { Spool } = await freshPage()
    const records = Array.from({ length: 5000 }, (_, i) => ({ i, text: 'x'.repeat(i % 300), nested: { ok: i % 2 === 0 } }))
    const spool = new Spool<(typeof records)[number]>({ storage, objectMode: true, chunkSize: 4096 })

    await writeStalled(spool.writable, records)
    const out = await readAll(spool.readable)
    expect(out).toEqual(records)
    // The first record passed straight through: the same object, never encoded.
    expect(out[0]).toBe(records[0])
    await spool.closed
    await expectNoLeftovers(browser)
  })

  it('takes a custom codec in object mode', async () => {
    installFakeBrowser()
    const { Spool } = await freshPage()
    const codec = {
      encode: (value: Date) => new TextEncoder().encode(value.toISOString()),
      decode: (bytes: Uint8Array) => new Date(new TextDecoder().decode(bytes)),
    }
    const dates = Array.from({ length: 100 }, (_, i) => new Date(Date.UTC(2020, 0, 1 + i)))
    const spool = new Spool<Date>({ storage, objectMode: true, codec })
    await writeStalled(spool.writable, dates)
    const out = await readAll(spool.readable)
    expect(out.map((date) => date.getTime())).toEqual(dates.map((date) => date.getTime()))
    expect(out[50]).toBeInstanceOf(Date)
  })

  it('deletes its storage when the writable side is aborted', async () => {
    const browser = installFakeBrowser()
    const { Spool } = await freshPage()
    const spool = new Spool({ storage })
    const writer = spool.writable.getWriter()
    for (const piece of chunk(randomBytes(2 * MiB), 64 * KiB)) await writer.write(piece)
    await waitFor(async () => browser.opfsFiles().length + (await browser.idbRecords()) > 0)

    await writer.abort(new Error('socket reset'))
    await expect(spool.readable.getReader().read()).rejects.toThrow('socket reset')
    await spool.closed
    await expectNoLeftovers(browser)
  })

  it('deletes its storage and errors the writable when the readable side is cancelled', async () => {
    const browser = installFakeBrowser()
    const { Spool } = await freshPage()
    const spool = new Spool({ storage })
    const writer = spool.writable.getWriter()
    for (const piece of chunk(randomBytes(2 * MiB), 64 * KiB)) await writer.write(piece)

    const reason = new Error('consumer gave up')
    await spool.readable.cancel(reason)
    await expect(writer.write(new Uint8Array(1))).rejects.toBe(reason)
    await spool.closed
    await expectNoLeftovers(browser)
  })

  it('caps storage with maxDiskBytes, pausing the source until the consumer frees space', async () => {
    const browser = installFakeBrowser()
    const { Spool } = await freshPage()
    const cap = 256 * KiB
    const data = randomBytes(4 * MiB)
    const spool = new Spool({ storage, maxDiskBytes: cap, chunkSize: 16 * KiB })
    const reader = spool.readable.getReader()

    let accepted = 0
    const writing = (async () => {
      const writer = spool.writable.getWriter()
      for (const piece of chunk(data, 16 * KiB)) {
        await writer.write(piece)
        accepted += piece.byteLength
      }
      await writer.close()
    })()
    // Stalls at the cap plus one batch in memory, well short of the whole source.
    await waitFor(() => accepted > cap)
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(accepted).toBeLessThan(cap + 1.5 * MiB)
    expect(accepted).toBeLessThan(data.length)

    const out: Uint8Array[] = []
    for (;;) {
      const { value, done } = await reader.read()
      if (done) break
      out.push(value)
    }
    await writing
    expect(Buffer.concat(out).equals(data)).toBe(true)
    if (storage === 'opfs') expect(browser.root.peakFileSize).toBeLessThanOrEqual(cap)
  })

  it("fails with SpoolLimitError at maxDiskBytes with onLimit: 'error'", async () => {
    const browser = installFakeBrowser()
    const { Spool, SpoolLimitError } = await freshPage()
    const spool = new Spool({ storage, maxDiskBytes: 64 * KiB, onLimit: 'error' })
    const writer = spool.writable.getWriter()
    const writes = (async () => {
      for (const piece of chunk(randomBytes(4 * MiB), 16 * KiB)) await writer.write(piece)
    })()
    const error = await writes.catch((e: unknown) => e)
    expect(error).toBeInstanceOf(SpoolLimitError)
    expect(error).toMatchObject({ code: 'ERR_SYKE_SPOOL_LIMIT', message: expect.stringMatching(/maxDiskBytes/) })
    await expect(spool.readable.getReader().read()).rejects.toBe(error)
    await spool.closed
    await expectNoLeftovers(browser)
  })
})
