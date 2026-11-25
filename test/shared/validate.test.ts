import { randomBytes } from 'node:crypto'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { describe, expect, it } from 'vitest'
import { fork, Spool } from '../../src/node.ts'
import { nonNegativeInt, positiveInt } from '../../src/shared/validate.ts'
import { useTempDir } from '../nodeHelpers.ts'

describe.each([
  ['positiveInt', positiveInt, 1],
  ['nonNegativeInt', nonNegativeInt, 0],
] as const)('%s', (_, validate, least) => {
  it('returns an integer within range unchanged', () => {
    expect(validate('n', least)).toBe(least)
    expect(validate('n', 1234)).toBe(1234)
    expect(validate('n', Number.MAX_SAFE_INTEGER)).toBe(Number.MAX_SAFE_INTEGER)
  })

  // 0.1.0 took these as "effectively unlimited"; they mustn't start throwing.
  it.each([2 ** 53, 2 ** 60, 1e21, Number.MAX_VALUE])('clamps %d to Number.MAX_SAFE_INTEGER', (value) => {
    expect(validate('n', value)).toBe(Number.MAX_SAFE_INTEGER)
  })

  it.each([
    least - 1,
    -(2 ** 60),
    1.5,
    Number.NaN,
    // Not an integer, and rejected by 0.1.0 too.
    Number.POSITIVE_INFINITY,
    Number.NEGATIVE_INFINITY,
    '5' as unknown as number,
  ])('rejects %s', (value) => {
    expect(() => validate('n', value)).toThrow(RangeError)
  })
})

describe('large limits in utilities that share the validator', () => {
  const tmp = useTempDir()

  it('Spool takes maxDiskBytes and memoryThreshold past 2^53 as unlimited', async () => {
    const data = randomBytes(100_000)
    const spool = new Spool({ dir: tmp.path, maxDiskBytes: 2 ** 60, memoryThreshold: Number.MAX_VALUE })
    const out: Buffer[] = []
    await pipeline(Readable.from([data]), spool, async (source: AsyncIterable<Buffer>) => {
      for await (const chunk of source) out.push(chunk)
    })
    expect(Buffer.concat(out).equals(data)).toBe(true)
  })

  it('Spool still rejects Infinity and fractions for maxDiskBytes', () => {
    expect(() => new Spool({ dir: tmp.path, maxDiskBytes: Number.POSITIVE_INFINITY })).toThrow(
      /maxDiskBytes must be a positive integer, got Infinity/,
    )
    expect(() => new Spool({ dir: tmp.path, maxDiskBytes: 1.5 })).toThrow(RangeError)
  })

  it('fork validates count with the same rules', () => {
    expect(() => fork([1], { count: Number.POSITIVE_INFINITY })).toThrow(RangeError)
    expect(() => fork([1], { count: 1.5 })).toThrow(/count must be a positive integer, got 1.5/)
  })
})
