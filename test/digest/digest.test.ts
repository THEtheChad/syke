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
})

describe('Digest (node)', () => {
  it('supports any crypto.createHash algorithm', async () => {
    const digest = new NodeDigest({ algorithm: 'md5' })
    await run(digest, [data])
    expect(await digest.digest).toBe(createHash('md5').update(data).digest('hex'))
  })
})

describe('Digest (web)', () => {
  it('rejects algorithms it would need WebCrypto streaming for', () => {
    // @ts-expect-error only 'crc32' is built in
    expect(() => new WebDigest({ algorithm: 'sha256' })).toThrow(/pass a hasher/)
  })
})
