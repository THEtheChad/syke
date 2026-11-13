import { describe, expect, it } from 'vitest'
import {
  LineTooLongError,
  NdjsonParse as NodeNdjsonParse,
  NdjsonParseError,
  NdjsonStringify as NodeNdjsonStringify,
  NdjsonStringifyError,
} from '../../src/node.ts'
import {
  NdjsonParse as WebNdjsonParse,
  NdjsonParseError as WebNdjsonParseError,
  NdjsonStringify as WebNdjsonStringify,
  NdjsonStringifyError as WebNdjsonStringifyError,
} from '../../src/web.ts'
import { chunk, pick, run, runSettled, runtimes } from '../harness.ts'

const encode = (text: string) => new TextEncoder().encode(text)
/** Runs values through a stringifier and joins the bytes it emits. */
const runBytes = async (stream: unknown, input: unknown[]) => Buffer.concat(await run<Uint8Array>(stream, input))

const records = [
  { id: 1, name: 'Zoë', tags: ['a', 'b'] },
  { id: 2, name: '日本', nested: { ok: true, n: null } },
  [1, 2, 3],
  'a string',
  42,
  false,
  { emoji: '🦀' },
]

describe.each(runtimes)('NdjsonParse (%s)', (runtime) => {
  const NdjsonParse = pick(runtime, { node: NodeNdjsonParse, web: WebNdjsonParse })

  it('parses one value per line, across every chunk boundary', async () => {
    const bytes = encode(`${records.map((r) => JSON.stringify(r)).join('\r\n')}\n`)
    for (const size of [1, 3, 7, bytes.length]) {
      expect(await run(new NdjsonParse(), chunk(bytes, size))).toEqual(records)
    }
  })

  it('parses a final line without a newline', async () => {
    expect(await run(new NdjsonParse(), [encode('{"a":1}\n{"a":2}')])).toEqual([{ a: 1 }, { a: 2 }])
  })

  it('skips blank and whitespace-only lines', async () => {
    expect(await run(new NdjsonParse(), [encode('\n{"a":1}\n\n  \t\r\n{"a":2}\n\n')])).toEqual([{ a: 1 }, { a: 2 }])
  })

  it('emits nothing for empty input', async () => {
    expect(await run(new NdjsonParse(), [])).toEqual([])
    expect(await run(new NdjsonParse(), [encode('\n\n')])).toEqual([])
  })

  it('fails on invalid JSON with the 1-based line number, counting blank lines', async () => {
    const { output, error } = await runSettled(new NdjsonParse(), [encode('{"a":1}\n\n{"a":\n{"a":3}\n')])
    expect(error).toBeInstanceOf(NdjsonParseError)
    expect(error).toMatchObject({ code: 'ERR_SYKE_NDJSON', line: 3 })
    expect(error.message).toMatch(/^syke: invalid JSON on line 3: /)
    expect(error.cause).toBeInstanceOf(SyntaxError)
    expect([{ a: 1 }]).toEqual(expect.arrayContaining(output))
  })

  it('fails on a top-level null line', async () => {
    const error = await run(new NdjsonParse(), [encode('1\nnull\n2\n')]).catch((e) => e)
    expect(error).toBeInstanceOf(NdjsonParseError)
    expect(error).toMatchObject({ code: 'ERR_SYKE_NDJSON', line: 2 })
    expect(error.message).toMatch(/null/)
  })

  it('passes a reviver to JSON.parse, which can map null or drop lines', async () => {
    const NULL = Symbol('null')
    const reviver = (key: string, value: unknown) => {
      if (key === '') return value === null ? NULL : value === 'skip' ? undefined : value
      return typeof value === 'number' ? value * 10 : value
    }
    expect(await run(new NdjsonParse({ reviver }), [encode('{"n":1}\nnull\n"skip"\n[2]\n')])).toEqual([
      { n: 10 },
      NULL,
      [20],
    ])
  })

  it('fails a line longer than maxLineLength with a LineTooLongError', async () => {
    const error = await run(new NdjsonParse({ maxLineLength: 10 }), chunk(encode('{"a":1}\n"0123456789'), 4)).catch(
      (e) => e,
    )
    expect(error).toBeInstanceOf(LineTooLongError)
    expect(error).toMatchObject({ code: 'ERR_SYKE_LINE_TOO_LONG', line: 2 })
  })

  it('rejects invalid options', () => {
    expect(() => new NdjsonParse({ maxLineLength: -1 })).toThrow(RangeError)
    // @ts-expect-error not a function
    expect(() => new NdjsonParse({ reviver: 'nope' })).toThrow(TypeError)
  })
})

describe.each(runtimes)('NdjsonStringify (%s)', (runtime) => {
  const NdjsonStringify = pick(runtime, { node: NodeNdjsonStringify, web: WebNdjsonStringify })
  const NdjsonParse = pick(runtime, { node: NodeNdjsonParse, web: WebNdjsonParse })

  it('writes JSON.stringify(value) + "\\n" per value as UTF-8', async () => {
    const out = await runBytes(new NdjsonStringify(), records)
    expect(out.toString('utf8')).toBe(records.map((r) => `${JSON.stringify(r)}\n`).join(''))
  })

  it('round-trips through NdjsonParse', async () => {
    const bytes = await runBytes(new NdjsonStringify(), records)
    expect(await run(new NdjsonParse(), chunk(bytes, 5))).toEqual(records)
  })

  it('emits nothing for empty input', async () => {
    expect((await runBytes(new NdjsonStringify(), [])).length).toBe(0)
  })

  it('passes a replacer to JSON.stringify', async () => {
    const replacer = (key: string, value: unknown) => (key === 'secret' ? undefined : value)
    const out = await runBytes(new NdjsonStringify({ replacer }), [{ id: 1, secret: 'x' }])
    expect(out.toString()).toBe('{"id":1}\n')
  })

  it('serialises BigInt through a replacer', async () => {
    const replacer = (_key: string, value: unknown) => (typeof value === 'bigint' ? value.toString() : value)
    expect((await runBytes(new NdjsonStringify({ replacer }), [{ n: 10n }])).toString()).toBe('{"n":"10"}\n')
  })

  it('fails on a circular value, with the cause', async () => {
    const circular: Record<string, unknown> = { id: 1 }
    circular.self = circular
    const error = await run(new NdjsonStringify(), [{ ok: 1 }, circular]).catch((e) => e)
    expect(error).toBeInstanceOf(NdjsonStringifyError)
    expect(error).toMatchObject({ code: 'ERR_SYKE_NDJSON_STRINGIFY', line: 2 })
    expect(error.message).toMatch(/^syke: value 2 can't be serialised as JSON/)
    expect(error.cause).toBeInstanceOf(TypeError)
  })

  it('fails on a BigInt', async () => {
    await expect(run(new NdjsonStringify(), [{ n: 1n }])).rejects.toBeInstanceOf(NdjsonStringifyError)
  })

  it('fails on a value with no JSON form', async () => {
    for (const value of [undefined, () => {}, Symbol('s')]) {
      const error = await run(new NdjsonStringify(), [1, value]).catch((e) => e)
      expect(error).toBeInstanceOf(NdjsonStringifyError)
      expect(error).toMatchObject({ line: 2 })
      expect(error.message).toMatch(/has no JSON form/)
    }
  })

  it('rejects invalid options', () => {
    // @ts-expect-error not a function
    expect(() => new NdjsonStringify({ replacer: ['id'] })).toThrow(TypeError)
  })
})

describe('NdjsonParse (node)', () => {
  it('accepts strings', async () => {
    expect(await run(new NodeNdjsonParse(), ['{"a":', '1}\n[', '2]'])).toEqual([{ a: 1 }, [2]])
  })
})

describe('NdjsonStringify (web)', () => {
  it('serialises a null value, which only web streams can carry', async () => {
    expect((await runBytes(new WebNdjsonStringify(), [null])).toString()).toBe('null\n')
  })
})

it('shares error classes between syke/node and syke/web', () => {
  expect(WebNdjsonParseError).toBe(NdjsonParseError)
  expect(WebNdjsonStringifyError).toBe(NdjsonStringifyError)
})
