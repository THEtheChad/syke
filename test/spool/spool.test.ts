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

it('shares one SpoolLimitError class between syke/node and syke/web', () => {
  expect(web.SpoolLimitError).toBe(node.SpoolLimitError)
})
