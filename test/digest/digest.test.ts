import { createHash, randomBytes } from 'node:crypto'
import * as zlib from 'node:zlib'
import { describe, expect, it } from 'vitest'
import { Digest as NodeDigest, DigestError, type Hasher } from '../../src/node.ts'
import { Digest as WebDigest } from '../../src/web.ts'
import { crc32 } from '../../src/digest/crc32.ts'
import { chunk, pick, run, runBytes, runtimes } from '../harness.ts'

const data = randomBytes(100_003)
const sha256 = createHash('sha256').update(data)
const sha256Hex = sha256.copy().digest('hex')
const sha256Base64 = sha256.copy().digest('base64')
const crcHex = (zlib.crc32(data) >>> 0).toString(16).padStart(8, '0')

/** A hasher shaped like `@noble/hashes` (`digest()` returns bytes). */
const nobleLike = (): Hasher => createHash('sha256')
/** A hasher shaped like `hash-wasm` (async factory, `digest()` returns hex). */
async function hashWasmLike(): Promise<Hasher> {
  const hash = createHash('sha256')
  return { update: (d) => hash.update(d), digest: () => hash.digest('hex') }
}

describe('crc32', () => {
  it('matches the standard check value and zlib', () => {
    expect(crc32(new TextEncoder().encode('123456789')).toString(16)).toBe('cbf43926')
    for (const size of [0, 1, 7, 8, 9, 63, 1000]) {
      const slice = data.subarray(0, size)
      expect(crc32(slice)).toBe(zlib.crc32(slice) >>> 0)
    }
    // Incremental updates across odd boundaries.
    let value = 0
    for (const piece of chunk(data, 13)) value = crc32(piece, value)
    expect(value).toBe(zlib.crc32(data) >>> 0)
  })
})

describe.each(runtimes)('Digest (%s)', (runtime) => {
  const Digest = pick(runtime, { node: NodeDigest, web: WebDigest })

  it('passes data through and resolves the crc32 digest', async () => {
    const digest = new Digest({ algorithm: 'crc32' })
    expect((await runBytes(digest, chunk(data, 4096))).equals(data)).toBe(true)
    expect(await digest.digest).toBe(crcHex)
  })

  it('takes a synchronous hasher', async () => {
    const digest = new Digest({ hasher: nobleLike() })
    await run(digest, chunk(data, 4096))
    expect(await digest.digest).toBe(sha256Hex)
  })

  it('takes a promised hasher whose digest is hex', async () => {
    const digest = new Digest({ hasher: hashWasmLike(), encoding: 'base64' })
    await run(digest, chunk(data, 4096))
    expect(await digest.digest).toBe(sha256Base64)
  })

  it('verifies expected as hex (any case), base64 or bytes', async () => {
    await run(new Digest({ algorithm: 'crc32', expected: crcHex.toUpperCase() }), [data])
    await run(new Digest({ hasher: nobleLike(), expected: sha256Base64, encoding: 'base64' }), [data])
    await run(new Digest({ hasher: nobleLike(), expected: sha256.copy().digest() }), [data])
  })

  it('fails on a mismatch before the consumer sees the end, and rejects digest', async () => {
    const digest = new Digest({ algorithm: 'crc32', expected: '00000000' })
    const error = await run(digest, chunk(data, 4096)).catch((e) => e)
    expect(error).toBeInstanceOf(DigestError)
    expect(error).toMatchObject({ code: 'ERR_SYKE_DIGEST', expected: '00000000', actual: crcHex })
    await expect(digest.digest).rejects.toBe(error)
  })

  it('rejects digest when the stream fails, without an unhandled rejection', async () => {
    const digest = new Digest({ algorithm: 'crc32' })
    async function* failing() {
      yield data
      throw new Error('boom')
    }
    await expect(run(digest, failing())).rejects.toThrow('boom')
    await expect(digest.digest).rejects.toThrow(/closed before its digest/)
  })

  it('needs an algorithm or a hasher', () => {
    // @ts-expect-error neither given
    expect(() => new Digest({})).toThrow(TypeError)
  })

  it('takes an algorithm or a hasher, not both', () => {
    // @ts-expect-error both given
    expect(() => new Digest({ algorithm: 'crc32', hasher: nobleLike() })).toThrow(
      /^syke: Digest takes an algorithm or a hasher, not both$/,
    )
  })

  it('rejects an expected digest that is neither a string nor bytes', () => {
    // @ts-expect-error not a string or Uint8Array
    expect(() => new Digest({ algorithm: 'crc32', expected: 1234 })).toThrow(/^syke: expected must be a string or a Uint8Array/)
    // @ts-expect-error not a string or Uint8Array
    expect(() => new Digest({ algorithm: 'crc32', expected: new ArrayBuffer(4) })).toThrow(TypeError)
  })

  it('rejects a hasher without update() and digest()', () => {
    // @ts-expect-error not a hasher
    expect(() => new Digest({ hasher: { update() {} } })).toThrow(/^syke: hasher must have update\(\) and digest\(\)/)
  })

  it("fails with a promised hasher's own error, and rejects digest with it", async () => {
    const failure = new Error('wasm failed to load')
    const digest = new Digest({ hasher: Promise.reject(failure) })
    await expect(run(digest, [data])).rejects.toBe(failure)
    await expect(digest.digest).rejects.toBe(failure)
  })

  it.each([
    ['data', [data]],
    ['an empty stream', []],
  ])('fails the stream and rejects digest with the same Error when a promised hasher rejects with undefined (%s)', async (_, input) => {
    const digest = new Digest({ hasher: Promise.reject(undefined) })
    const error = await run(digest, input).catch((e) => e)
    expect(error).toBeInstanceOf(Error)
    expect(error).toMatchObject({ message: 'syke: stream failed with undefined', cause: undefined })
    await expect(digest.digest).rejects.toBe(error)
  })

  it('rejects digest with the same wrapped Error when update() throws a falsy value', async () => {
    const digest = new Digest({
      hasher: {
        update: () => {
          throw 0
        },
        digest: () => '00',
      },
    })
    const error = await run(digest, [data]).catch((e) => e)
    expect(error).toMatchObject({ message: 'syke: stream failed with 0', cause: 0 })
    await expect(digest.digest).rejects.toBe(error)
  })

  it("rejects digest with a promised hasher's error on an empty stream", async () => {
    const failure = new Error('wasm failed to load')
    const digest = new Digest({ hasher: Promise.reject(failure) })
    await expect(run(digest, [])).rejects.toBe(failure)
    await expect(digest.digest).rejects.toBe(failure)
  })

  it('leaves no unhandled rejection when a promised hasher fails and the stream never runs', async () => {
    const digest = new Digest({ hasher: Promise.reject(new Error('wasm failed to load')) })
    if (runtime === 'node') (digest as unknown as NodeDigest).destroy()
    else await (digest as unknown as TransformStream).readable.cancel()
    await new Promise((resolve) => setImmediate(resolve))
    await expect(digest.digest).rejects.toThrow(/closed before its digest/)
  })

  it("rejects digest with the hasher's own error when digest() throws", async () => {
    const failure = new Error('digest broke')
    const digest = new Digest({ hasher: { update: () => {}, digest: () => { throw failure } } })
    await expect(run(digest, [data])).rejects.toBe(failure)
    await expect(digest.digest).rejects.toBe(failure)
  })

  it("rejects digest with the hasher's own error when update() throws", async () => {
    const failure = new Error('update broke')
    const digest = new Digest({ hasher: { update: () => { throw failure }, digest: () => '00' } })
    await expect(run(digest, [data])).rejects.toBe(failure)
    await expect(digest.digest).rejects.toBe(failure)
  })

  it('rejects digest when a hex digest() returns something that is not hex', async () => {
    const digest = new Digest({ hasher: { update: () => {}, digest: () => 'xyz' } })
    await expect(run(digest, [data])).rejects.toThrow(/^syke: not a hex string/)
    await expect(digest.digest).rejects.toThrow(/^syke: not a hex string/)
  })
})

describe('Digest (node)', () => {
  it('hashes strings as their UTF-8 bytes', async () => {
    const digest = new NodeDigest({ algorithm: 'sha256' })
    await run(digest, ['héllo'])
    expect(await digest.digest).toBe(createHash('sha256').update('héllo', 'utf8').digest('hex'))
  })

  it('forwards only highWaterMark and signal to the Node stream', () => {
    const digest = new NodeDigest({ algorithm: 'crc32', highWaterMark: 9, objectMode: true } as never)
    expect(digest.writableObjectMode).toBe(false)
    expect(digest.writableHighWaterMark).toBe(9)
  })

  it('supports any crypto.createHash algorithm', async () => {
    const digest = new NodeDigest({ algorithm: 'md5' })
    await run(digest, [data])
    expect(await digest.digest).toBe(createHash('md5').update(data).digest('hex'))
  })
})

describe('Digest (web)', () => {
  it('fails with a TypeError on a non-byte chunk rather than hashing it', async () => {
    const digest = new WebDigest({ algorithm: 'crc32' })
    await expect(run(digest, ['abc'])).rejects.toThrow(/^syke: Digest takes Uint8Array chunks, got string$/)
    await expect(digest.digest).rejects.toThrow(TypeError)
  })

  it('rejects algorithms it would need WebCrypto streaming for', () => {
    // @ts-expect-error only 'crc32' is built in
    expect(() => new WebDigest({ algorithm: 'sha256' })).toThrow(/pass a hasher/)
  })
})
