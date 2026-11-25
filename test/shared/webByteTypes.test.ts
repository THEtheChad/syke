import { describe, expect, expectTypeOf, it } from 'vitest'
import type {
  ByteChunk,
  Digest as DigestClass,
  Lines as LinesClass,
  NdjsonParse as NdjsonParseClass,
  Rechunk as RechunkClass,
} from '../../src/web.ts'

// Typechecked only (pnpm typecheck): nothing here runs.

// Lines and NdjsonParse decode any byte chunk, as well as strings.
const decoders = (
  Lines: typeof LinesClass,
  NdjsonParse: typeof NdjsonParseClass,
  buffers: ReadableStream<ArrayBuffer>,
  views: ReadableStream<DataView>,
  words: ReadableStream<Uint16Array>,
  mixed: ReadableStream<ByteChunk | string>,
) => {
  expectTypeOf(buffers.pipeThrough(new Lines())).toEqualTypeOf<ReadableStream<string>>()
  expectTypeOf(views.pipeThrough(new Lines())).toEqualTypeOf<ReadableStream<string>>()
  expectTypeOf(words.pipeThrough(new Lines())).toEqualTypeOf<ReadableStream<string>>()
  expectTypeOf(mixed.pipeThrough(new Lines())).toEqualTypeOf<ReadableStream<string>>()
  expectTypeOf(buffers.pipeThrough(new NdjsonParse<{ a: number }>())).toEqualTypeOf<ReadableStream<{ a: number }>>()
  expectTypeOf(views.pipeThrough(new NdjsonParse())).toEqualTypeOf<ReadableStream<unknown>>()
}

// Every call below compiled against 0.1.0 and must keep compiling.
const compat = (
  Lines: typeof LinesClass,
  NdjsonParse: typeof NdjsonParseClass,
  bytes: ReadableStream<Uint8Array>,
  text: ReadableStream<string>,
) => {
  expectTypeOf(bytes.pipeThrough(new Lines())).toEqualTypeOf<ReadableStream<string>>()
  expectTypeOf(text.pipeThrough(new Lines())).toEqualTypeOf<ReadableStream<string>>()
  expectTypeOf(bytes.pipeThrough(new NdjsonParse<{ a: number }>())).toEqualTypeOf<ReadableStream<{ a: number }>>()
  expectTypeOf(text.pipeThrough(new NdjsonParse())).toEqualTypeOf<ReadableStream<unknown>>()
  const lines: TransformStream<Uint8Array | string, string> = new Lines()
  const records: TransformStream<Uint8Array | string, { a: number }> = new NdjsonParse<{ a: number }>()
  const writable: WritableStream<Uint8Array | string> = new Lines().writable
  return [lines, records, writable]
}

// Rechunk and Digest index and slice their chunks, so they stay Uint8Array-only.
const strict = (
  Rechunk: typeof RechunkClass,
  Digest: typeof DigestClass,
  bytes: ReadableStream<Uint8Array>,
  buffers: ReadableStream<ArrayBuffer>,
  views: ReadableStream<DataView>,
  words: ReadableStream<Uint16Array>,
) => {
  expectTypeOf(bytes.pipeThrough(new Rechunk({ size: 4 }))).toEqualTypeOf<ReadableStream<Uint8Array>>()
  expectTypeOf(bytes.pipeThrough(new Digest({ algorithm: 'crc32' }))).toEqualTypeOf<ReadableStream<Uint8Array>>()
  // @ts-expect-error an ArrayBuffer stream
  buffers.pipeThrough(new Rechunk({ size: 4 }))
  // @ts-expect-error a DataView stream
  views.pipeThrough(new Rechunk({ size: 4 }))
  // @ts-expect-error a Uint16Array stream
  words.pipeThrough(new Rechunk({ size: 4 }))
  // @ts-expect-error an ArrayBuffer stream
  buffers.pipeThrough(new Digest({ algorithm: 'crc32' }))
  // @ts-expect-error a DataView stream
  views.pipeThrough(new Digest({ algorithm: 'crc32' }))
  // @ts-expect-error a Uint16Array stream
  words.pipeThrough(new Digest({ algorithm: 'crc32' }))
}

describe('web byte chunk types', () => {
  it('types each web byte utility for the chunks it takes', () => {
    expect([decoders, compat, strict].every((fn) => typeof fn === 'function')).toBe(true)
  })
})
