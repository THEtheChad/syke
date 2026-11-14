import { randomBytes } from 'node:crypto'
import { beforeEach, describe, expect, it } from 'vitest'
import * as node from '../../src/node.ts'
import * as web from '../../src/web.ts'
import { chunk, runBytes, runSettled, runtimes } from '../harness.ts'
import { useTempDir } from '../nodeHelpers.ts'
import { installFakeBrowser, useFakeBrowser } from './web/fakeBrowser.ts'

// The behaviour both Spools share. Runtime-specific behaviour (disk files,
// browser storage backends) is tested in ./node and ./web.
const tmp = useTempDir()
useFakeBrowser()
beforeEach(() => {
  installFakeBrowser()
})

describe.each(runtimes)('Spool (%s)', (runtime) => {
  type Options = node.SpoolOptions & web.SpoolOptions
  const make = (options: Options = {}): unknown =>
    runtime === 'node' ? new node.Spool({ dir: tmp.path, ...options }) : new web.Spool(options)

  it('passes data through unchanged', async () => {
    const data = randomBytes(3 * 1024 * 1024)
    expect((await runBytes(make({ chunkSize: 16 * 1024 }), chunk(data, 7001))).equals(data)).toBe(true)
  })

  it('ends an empty stream', async () => {
    expect((await runBytes(make(), [])).length).toBe(0)
  })

  it('fails when the source fails', async () => {
    async function* failing() {
      yield randomBytes(1024)
      throw new Error('socket reset')
    }
    const { error } = await runSettled(make(), failing())
    expect(error).toMatchObject({ message: 'socket reset' })
  })

  it('fails with SpoolCodecError when the codec throws on a record that spills', async () => {
    const codec = {
      encode: () => {
        throw new Error('cannot encode')
      },
      decode: (bytes: Uint8Array) => bytes,
    }
    const spool = make({ objectMode: true, codec })
    const records = [{ id: 1 }, { id: 2 }]
    // Nobody reads, so the records must spill through the codec.
    let error: unknown
    if (runtime === 'node') {
      const stream = spool as node.Spool
      error = await new Promise((resolve) => {
        stream.once('error', resolve)
        for (const record of records) stream.write(record)
      })
    } else {
      const writer = (spool as web.Spool<unknown>).writable.getWriter()
      error = await Promise.all(records.map((record) => writer.write(record))).catch((e) => e)
    }
    expect(error).toBeInstanceOf(node.SpoolCodecError)
    expect(error).toMatchObject({ code: 'ERR_SYKE_SPOOL_CODEC', cause: { message: 'cannot encode' } })
  })

  it.each<[Options, RegExp]>([
    [{ chunkSize: 0 }, /chunkSize must be a positive integer/],
    [{ memoryThreshold: -1 }, /memoryThreshold must be a non-negative integer/],
    [{ maxDiskBytes: 1.5 }, /maxDiskBytes must be a positive integer/],
    [{ minFreeSpace: '150%' }, /minFreeSpace must be a percentage/],
    [{ onLimit: 'drop' as never }, /onLimit must be 'backpressure' or 'error'/],
  ])('rejects %o with a RangeError', (options, message) => {
    expect(() => make(options)).toThrow(RangeError)
    expect(() => make(options)).toThrow(message)
  })
})

it('shares the Spool error classes between syke/node and syke/web', () => {
  expect(web.SpoolLimitError).toBe(node.SpoolLimitError)
  expect(web.SpoolCodecError).toBe(node.SpoolCodecError)
})
