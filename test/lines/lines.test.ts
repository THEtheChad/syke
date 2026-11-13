import { describe, expect, it } from 'vitest'
import { LineTooLongError, Lines as NodeLines } from '../../src/node.ts'
import { LineTooLongError as WebLineTooLongError, Lines as WebLines } from '../../src/web.ts'
import { chunk, pick, run, runSettled, runtimes } from '../harness.ts'

const encode = (text: string) => new TextEncoder().encode(text)

describe.each(runtimes)('Lines (%s)', (runtime) => {
  const Lines = pick(runtime, { node: NodeLines, web: WebLines })

  it('splits on \\n and strips a trailing \\r', async () => {
    expect(await run(new Lines(), [encode('a\nb\r\nc\n')])).toEqual(['a', 'b', 'c'])
  })

  it('emits a final line without a newline, but no empty line after a final newline', async () => {
    expect(await run(new Lines(), [encode('a\nb')])).toEqual(['a', 'b'])
    expect(await run(new Lines(), [encode('a\n')])).toEqual(['a'])
  })

  it('keeps empty lines in the middle', async () => {
    expect(await run(new Lines(), [encode('a\n\n\r\nb\n')])).toEqual(['a', '', '', 'b'])
  })

  it('emits nothing for empty input', async () => {
    expect(await run(new Lines(), [])).toEqual([])
    expect(await run(new Lines(), [new Uint8Array(0)])).toEqual([])
  })

  it('handles every chunk boundary, including \\r\\n and multi-byte characters split across chunks', async () => {
    const text = 'héllo wörld\r\n日本語\n🦀 crab\r\n\nlast 😀'
    const expected = ['héllo wörld', '日本語', '🦀 crab', '', 'last 😀']
    const bytes = encode(text)
    for (const size of [1, 2, 3, 5, 7]) {
      expect(await run(new Lines(), chunk(bytes, size))).toEqual(expected)
    }
  })

  it('joins a line that spans many chunks', async () => {
    const long = 'x'.repeat(100_000)
    expect(await run(new Lines(), chunk(encode(`${long}\n${long}`), 10))).toEqual([long, long])
  })

  it('decodes other encodings', async () => {
    const latin1 = new Uint8Array([0x63, 0x61, 0x66, 0xe9, 0x0a, 0x6e, 0xe9])
    expect(await run(new Lines({ encoding: 'latin1' }), [latin1])).toEqual(['café', 'né'])
    // UTF-16LE: "hi\nyo", split mid-code-unit.
    const utf16 = new Uint8Array([0x68, 0, 0x69, 0, 0x0a, 0, 0x79, 0, 0x6f, 0])
    expect(await run(new Lines({ encoding: 'utf-16le' }), chunk(utf16, 3))).toEqual(['hi', 'yo'])
  })

  it('strips a UTF-8 byte order mark', async () => {
    expect(await run(new Lines(), [new Uint8Array([0xef, 0xbb, 0xbf, 0x61, 0x0a])])).toEqual(['a'])
  })

  it('fails a line longer than maxLineLength, even without a newline', async () => {
    const { output, error } = await runSettled(new Lines({ maxLineLength: 5 }), chunk(encode('abc\n123456789'), 2))
    expect(error).toBeInstanceOf(LineTooLongError)
    expect(error).toMatchObject({ code: 'ERR_SYKE_LINE_TOO_LONG', line: 2, maxLineLength: 5 })
    expect(error.message).toMatch(/^syke: line 2 is longer than the maximum of 5/)
    // Node's pipeline may drop output still buffered when the error lands.
    expect(['abc']).toEqual(expect.arrayContaining(output))
  })

  it('checks complete lines and the final line against maxLineLength', async () => {
    await expect(run(new Lines({ maxLineLength: 3 }), [encode('abcd\n')])).rejects.toThrow(LineTooLongError)
    await expect(run(new Lines({ maxLineLength: 3 }), [encode('ab\nabcd')])).rejects.toMatchObject({ line: 2 })
  })

  it('does not count the line ending against maxLineLength', async () => {
    expect(await run(new Lines({ maxLineLength: 3 }), chunk(encode('abc\r\nabc\r'), 4))).toEqual(['abc', 'abc'])
  })

  it('accepts Infinity for maxLineLength', async () => {
    expect(await run(new Lines({ maxLineLength: Number.POSITIVE_INFINITY }), [encode('a\n')])).toEqual(['a'])
  })

  it('rejects invalid options', () => {
    expect(() => new Lines({ maxLineLength: 0 })).toThrow(RangeError)
    expect(() => new Lines({ maxLineLength: 1.5 })).toThrow(RangeError)
    expect(() => new Lines({ maxLineLength: Number.NaN })).toThrow(RangeError)
    expect(() => new Lines({ encoding: 'no-such-encoding' })).toThrow(/^syke: unsupported encoding/)
  })
})

describe('Lines (node)', () => {
  it('accepts strings', async () => {
    expect(await run(new NodeLines(), ['ab', 'c\nd\r', '\n', 'é'])).toEqual(['abc', 'd', 'é'])
  })

  it('ends a character split before a string chunk with U+FFFD', async () => {
    const euro = encode('€')
    expect(await run(new NodeLines(), [euro.subarray(0, 2), 'x\n'])).toEqual(['�x'])
  })
})

it('shares one LineTooLongError class between syke/node and syke/web', () => {
  expect(WebLineTooLongError).toBe(LineTooLongError)
})
