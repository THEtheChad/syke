import { describe, expect, it } from 'vitest'
import { parseMinFreeSpace, reservedBytes } from '../../../src/spool/core/limits.ts'

describe('parseMinFreeSpace()', () => {
  it.each([
    [false, null],
    [0, { bytes: 0 }],
    [1024, { bytes: 1024 }],
    [Number.MAX_SAFE_INTEGER, { bytes: Number.MAX_SAFE_INTEGER }],
    // Accepted in 0.1.0; past 2^53 it can never be met, so it clamps.
    [2 ** 53, { bytes: Number.MAX_SAFE_INTEGER }],
    [2 ** 60, { bytes: Number.MAX_SAFE_INTEGER }],
    [Number.MAX_VALUE, { bytes: Number.MAX_SAFE_INTEGER }],
    ['0%', { percent: 0 }],
    ['10%', { percent: 10 }],
    ['12.5%', { percent: 12.5 }],
    ['100%', { percent: 100 }],
  ] as const)('parses %o', (value, expected) => {
    expect(parseMinFreeSpace(value)).toEqual(expected)
  })

  it.each([
    -1,
    1.5,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    '150%',
    '-1%',
    '10',
    'lots',
    '%',
    ' 10%',
  ])('rejects %o with a RangeError', (value) => {
    expect(() => parseMinFreeSpace(value as never)).toThrow(RangeError)
    expect(() => parseMinFreeSpace(value as never)).toThrow(/^syke: minFreeSpace must be/)
  })

  it.each([true, null, undefined, {}, [], 10n])('rejects a non-string, non-number %o with a RangeError', (value) => {
    expect(() => parseMinFreeSpace(value as never)).toThrow(RangeError)
    expect(() => parseMinFreeSpace(value as never)).toThrow(/^syke: minFreeSpace must be/)
  })

  it('does not coerce objects that stringify to a percentage', () => {
    expect(() => parseMinFreeSpace({ toString: () => '10%' } as never)).toThrow(RangeError)
  })
})

describe('reservedBytes()', () => {
  it('reserves a fixed number of bytes or a share of the total', () => {
    expect(reservedBytes({ bytes: 500 }, 1000)).toBe(500)
    expect(reservedBytes({ percent: 10 }, 1000)).toBe(100)
    expect(reservedBytes({ percent: 0 }, 1000)).toBe(0)
  })
})
