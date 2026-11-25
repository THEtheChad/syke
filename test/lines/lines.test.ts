import { ReadableStream as NodeReadableStream } from 'node:stream/web'
import { runInNewContext } from 'node:vm'
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

describe.each(runtimes)('Lines (%s) with text input', (runtime) => {
  const Lines = pick(runtime, { node: NodeLines, web: WebLines })

  it('accepts strings', async () => {
    expect(await run(new Lines(), ['ab', 'c\nd\r', '\n', 'é'])).toEqual(['abc', 'd', 'é'])
  })

  it('ends a character split before a string chunk with U+FFFD', async () => {
    const euro = encode('€')
    expect(await run(new Lines(), [euro.subarray(0, 2), 'x\n'])).toEqual(['�x'])
  })

  it('strips a byte order mark from a leading string, as from leading bytes', async () => {
    expect(await run(new Lines(), ['\uFEFFa\nb'])).toEqual(['a', 'b'])
    expect(await run(new Lines(), ['', '\uFEFFa\n'])).toEqual(['a'])
  })

  it('strips a byte order mark split across byte chunks', async () => {
    expect(await run(new Lines(), [new Uint8Array([0xef]), new Uint8Array([0xbb, 0xbf, 0x61, 0x0a])])).toEqual(['a'])
  })

  it('strips a UTF-16 byte order mark', async () => {
    const utf16 = new Uint8Array([0xff, 0xfe, 0x68, 0, 0x69, 0])
    expect(await run(new Lines({ encoding: 'utf-16le' }), [utf16])).toEqual(['hi'])
  })

  it('keeps U+FEFF past the start of the stream, whatever the chunk type', async () => {
    const bom = '\uFEFF'
    expect(await run(new Lines(), [encode('a\n'), encode(`${bom}b\n`)])).toEqual(['a', `${bom}b`])
    expect(await run(new Lines(), ['a\n', `${bom}b\n`])).toEqual(['a', `${bom}b`])
    // A string chunk flushes the decoder; that mustn't make it strip a BOM again.
    expect(await run(new Lines(), ['a\n', encode(`${bom}b\n`)])).toEqual(['a', `${bom}b`])
  })

  it('fails a line far past maxLineLength that completes in one chunk', async () => {
    const { error } = await runSettled(new Lines({ maxLineLength: 4 }), ['ab', 'cd', `${'x'.repeat(1000)}\n`])
    expect(error).toMatchObject({ code: 'ERR_SYKE_LINE_TOO_LONG', line: 1 })
  })

  it('lets a line of exactly maxLineLength end in \\r\\n split across chunks', async () => {
    expect(await run(new Lines({ maxLineLength: 4 }), ['ab', 'cd\r', '\nef'])).toEqual(['abcd', 'ef'])
    await expect(run(new Lines({ maxLineLength: 4 }), ['ab', 'cde\r', '\n'])).rejects.toThrow(LineTooLongError)
  })
})

describe.each(runtimes)('Lines (%s) with other byte views', (runtime) => {
  const Lines = pick(runtime, { node: NodeLines, web: WebLines })

  // A byte-mode Node stream turns any view into a Buffer first; the web one decodes it as is.
  it('decodes the bytes of a DataView or Uint16Array', async () => {
    const bytes = encode('ab\ncd\n')
    const input = [new DataView(bytes.buffer, 0, 3), new Uint16Array(bytes.slice(3, 5).buffer), bytes.subarray(5)]
    expect(await run(new Lines(), input)).toEqual(['ab', 'cd'])
  })
})

describe('Lines (web)', () => {
  it('decodes ArrayBuffer and SharedArrayBuffer chunks, from any realm', async () => {
    const shared = new SharedArrayBuffer(3)
    new Uint8Array(shared).set(encode('cd\n'))
    const foreign = runInNewContext('new Uint8Array([101, 102, 10]).buffer') as ArrayBuffer
    expect(foreign instanceof ArrayBuffer).toBe(false)
    expect(await run(new WebLines(), [encode('ab\n').buffer, shared, foreign])).toEqual(['ab', 'cd', 'ef'])
  })

  it('types its input to accept a TextDecoderStream (lib.dom)', () => {
    // Compile-time check: this only type-checks if the input side takes strings.
    const pipe = (text: ReadableStream<string>) => text.pipeThrough(new WebLines())
    expect(typeof pipe).toBe('function')
  })

  it("takes a TextDecoderStream's output", async () => {
    const source = NodeReadableStream.from([encode('héllo\nwö'), encode('rld\n')])
    const lines: unknown[] = []
    const text = source.pipeThrough(new TextDecoderStream() as never) as NodeReadableStream<string>
    for await (const line of text.pipeThrough(new WebLines() as never)) lines.push(line)
    expect(lines).toEqual(['héllo', 'wörld'])
  })

  it.each([
    ['a number', 42],
    ['an object', { length: 1 }],
    ['an object posing as an ArrayBuffer', { byteLength: 1, [Symbol.toStringTag]: 'ArrayBuffer' }],
    ['null', null],
  ])('fails with a TypeError on %s', async (_, bad) => {
    const { error } = await runSettled(new WebLines(), [encode('a\n'), bad])
    expect(error).toBeInstanceOf(TypeError)
    expect(error.message).toMatch(/^syke: expected Uint8Array or string chunks, got /)
  })
})

describe('Lines (node)', () => {
  it('forwards only highWaterMark and signal to the Node stream', () => {
    const lines = new NodeLines({ highWaterMark: 5, objectMode: true, decodeStrings: true } as never)
    expect(lines.writableObjectMode).toBe(false)
    expect(lines.readableObjectMode).toBe(true)
    expect(lines.writableHighWaterMark).toBe(5)
    // Strings stay strings: decodeStrings: true would have re-encoded them as UTF-8 bytes.
    expect((lines as unknown as { _writableState: { decodeStrings: boolean } })._writableState.decodeStrings).toBe(false)
  })
})

it('shares one LineTooLongError class between syke/node and syke/web', () => {
  expect(WebLineTooLongError).toBe(LineTooLongError)
})
