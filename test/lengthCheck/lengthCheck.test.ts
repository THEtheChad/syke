import { randomBytes } from 'node:crypto'
import { runInNewContext } from 'node:vm'
import { describe, expect, it } from 'vitest'
import { LengthCheck as NodeLengthCheck, LengthCheckError } from '../../src/node.ts'
import { LengthCheckError as WebLengthCheckError, LengthCheck as WebLengthCheck } from '../../src/web.ts'
import { chunk, pick, run, runBytes, runSettled, runtimes } from '../harness.ts'

describe.each(runtimes)('LengthCheck (%s)', (runtime) => {
  const LengthCheck = pick(runtime, { node: NodeLengthCheck, web: WebLengthCheck })
  const data = randomBytes(10_000)

  it('passes a stream of the expected length through unchanged', async () => {
    const check = new LengthCheck({ expected: data.length })
    expect((await runBytes(check, chunk(data, 999))).equals(data)).toBe(true)
    expect(check.bytes).toBe(data.length)
  })

  it('fails a short stream before the consumer sees the end', async () => {
    const error = await run(new LengthCheck({ expected: data.length + 1 }), chunk(data, 999)).catch((e) => e)
    expect(error).toBeInstanceOf(LengthCheckError)
    expect(error).toMatchObject({ code: 'ERR_SYKE_LENGTH', bytes: data.length })
    expect(error.message).toMatch(/short of the expected 10001 bytes/)
  })

  it('fails a long stream without passing on the chunk that overruns', async () => {
    const { output, error } = await runSettled<Uint8Array>(new LengthCheck({ expected: 2500 }), chunk(data, 1000))
    expect(error).toMatchObject({ code: 'ERR_SYKE_LENGTH', bytes: 3000 })
    expect(error.message).toMatch(/longer than the expected 2500 bytes/)
    // At most the two chunks within the limit reach the consumer.
    expect(Buffer.concat(output).length).toBeLessThanOrEqual(2000)
  })

  it('enforces min and max bounds', async () => {
    await expect(run(new LengthCheck({ max: 100 }), [data.subarray(0, 101)])).rejects.toThrow(/maximum of 100/)
    await expect(run(new LengthCheck({ min: 100 }), [data.subarray(0, 99)])).rejects.toThrow(/minimum of 100/)
    expect((await runBytes(new LengthCheck({ min: 10, max: 20 }), [data.subarray(0, 15)])).length).toBe(15)
  })

  it('accepts an empty stream when zero bytes are allowed', async () => {
    expect((await runBytes(new LengthCheck({ expected: 0 }), [])).length).toBe(0)
  })

  it('rejects invalid options', () => {
    expect(() => new LengthCheck({})).toThrow(TypeError)
    expect(() => new LengthCheck({ expected: -1 })).toThrow(RangeError)
    expect(() => new LengthCheck({ max: 1.5 })).toThrow(RangeError)
    expect(() => new LengthCheck({ min: 10, max: 5 })).toThrow(/allows no length/)
    expect(() => new LengthCheck({ expected: 10, max: 5 })).toThrow(/allows no length/)
    // Past 2^53 integers stop being exact, so byte counts couldn't be compared.
    expect(() => new LengthCheck({ max: 2 ** 53 })).toThrow(RangeError)
    expect(() => new LengthCheck({ max: Number.MAX_SAFE_INTEGER })).not.toThrow()
  })

  it('accepts a Uint8Array from another realm', async () => {
    const foreign = runInNewContext('new Uint8Array([1, 2, 3])') as Uint8Array
    expect(foreign instanceof Uint8Array).toBe(false)
    const check = new LengthCheck({ expected: 3 })
    expect((await runBytes(check, [foreign])).length).toBe(3)
  })
})

describe('LengthCheck (web)', () => {
  it.each([
    ['a string', 'abc', /got string$/],
    ['a number', 42, /got number$/],
    ['an ArrayBuffer', new ArrayBuffer(4), /got ArrayBuffer$/],
    ['a DataView', new DataView(new ArrayBuffer(4)), /got DataView$/],
    ['a Uint16Array', new Uint16Array(2), /got Uint16Array$/],
    ['a plain object', { byteLength: 3 }, /got Object$/],
  ])('fails with a TypeError on %s rather than miscounting', async (_, bad, message) => {
    const check = new WebLengthCheck({ max: 100 })
    const { error } = await runSettled(check, [new Uint8Array(2), bad])
    expect(error).toBeInstanceOf(TypeError)
    expect(error.message).toMatch(/^syke: LengthCheck takes Uint8Array chunks, got /)
    expect(error.message).toMatch(message)
    expect(check.bytes).toBe(2)
  })
})

describe('LengthCheck (node)', () => {
  it('accepts strings, counted as UTF-8 bytes', async () => {
    const check = new NodeLengthCheck({ expected: 4 })
    expect((await runBytes(check, ['ab' as unknown as Uint8Array, 'é' as unknown as Uint8Array])).length).toBe(4)
    expect(check.bytes).toBe(4)
  })

  it('forwards only highWaterMark and signal to the Node stream', async () => {
    const options = { max: 10, highWaterMark: 7, objectMode: true, autoDestroy: false } as never
    const check = new NodeLengthCheck(options)
    expect(check.writableObjectMode).toBe(false)
    expect(check.readableObjectMode).toBe(false)
    expect(check.writableHighWaterMark).toBe(7)
    await runBytes(check, [new Uint8Array(3)])
    // autoDestroy stays on: the stream cleans itself up once finished.
    expect(check.destroyed).toBe(true)
  })
})

it('shares one LengthCheckError class between syke/node and syke/web', () => {
  expect(WebLengthCheckError).toBe(LengthCheckError)
})
