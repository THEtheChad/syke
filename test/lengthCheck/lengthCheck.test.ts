import { randomBytes } from 'node:crypto'
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
  })
})

it('shares one LengthCheckError class between syke/node and syke/web', () => {
  expect(WebLengthCheckError).toBe(LengthCheckError)
})
