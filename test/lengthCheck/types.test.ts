import { describe, expect, expectTypeOf, it } from 'vitest'
import type {
  ByteChunk,
  Digest as DigestClass,
  LengthCheck as LengthCheckClass,
  LengthCheckOptions,
  LengthCheckOutput,
  Rechunk as RechunkClass,
} from '../../src/web.ts'

// Typechecked only (pnpm typecheck): nothing here runs.

// Byte chunks other than Uint8Arrays go through and come out with the type they went in with.
const anyChunk = (
  LengthCheck: typeof LengthCheckClass,
  buffers: ReadableStream<ArrayBuffer>,
  shared: ReadableStream<SharedArrayBuffer>,
  views: ReadableStream<DataView>,
  words: ReadableStream<Uint16Array>,
  mixed: ReadableStream<ByteChunk>,
  someBytes: ReadableStream<Uint8Array<ArrayBuffer> | DataView>,
) => {
  expectTypeOf(buffers.pipeThrough(new LengthCheck({ expected: 4 }))).toEqualTypeOf<ReadableStream<ArrayBuffer>>()
  expectTypeOf(shared.pipeThrough(new LengthCheck({ expected: 4 }))).toEqualTypeOf<ReadableStream<SharedArrayBuffer>>()
  expectTypeOf(views.pipeThrough(new LengthCheck({ max: 4 }))).toEqualTypeOf<ReadableStream<DataView>>()
  expectTypeOf(words.pipeThrough(new LengthCheck({ min: 4 }))).toEqualTypeOf<ReadableStream<Uint16Array>>()
  expectTypeOf(mixed.pipeThrough(new LengthCheck({ expected: 4 }))).toEqualTypeOf<ReadableStream<ByteChunk>>()
  // Each member of a union on its own: the Uint8Array comes out plain.
  expectTypeOf(someBytes.pipeThrough(new LengthCheck({ max: 4 }))).toEqualTypeOf<
    ReadableStream<Uint8Array | DataView>
  >()

  // An explicit type argument, for a check built before it's piped.
  const check = new LengthCheck<ArrayBuffer>({ expected: 4 })
  expectTypeOf(check).toExtend<TransformStream<ArrayBuffer, ArrayBuffer>>()
  expectTypeOf(buffers.pipeThrough(check)).toEqualTypeOf<ReadableStream<ArrayBuffer>>()
  expectTypeOf(check.bytes).toEqualTypeOf<number>()
}

// Any Uint8Array comes out as a plain Uint8Array, as in 0.1.0.
interface Packet extends Uint8Array {
  tag: string
}
const uint8 = (
  LengthCheck: typeof LengthCheckClass,
  body: ReadableStream<Uint8Array<ArrayBuffer>>,
  packets: ReadableStream<Packet>,
  plain: ReadableStream<Uint8Array>,
) => {
  expectTypeOf<LengthCheckOutput<Uint8Array<ArrayBuffer>>>().toEqualTypeOf<Uint8Array>()
  expectTypeOf<LengthCheckOutput<Packet>>().toEqualTypeOf<Uint8Array>()
  expectTypeOf<LengthCheckOutput<any>>().toEqualTypeOf<Uint8Array>()

  // A Response body (Uint8Array<ArrayBuffer>), reassigned and chained.
  let cur = body.pipeThrough(new LengthCheck({ max: 1024 }))
  expectTypeOf(cur).toEqualTypeOf<ReadableStream<Uint8Array>>()
  cur = plain
  cur = cur.pipeThrough(new LengthCheck({ max: 1 }))
  const streams = [body.pipeThrough(new LengthCheck({ max: 1024 }))]
  streams.push(plain)

  let fromPackets = packets.pipeThrough(new LengthCheck({ max: 1024 }))
  expectTypeOf(fromPackets).toEqualTypeOf<ReadableStream<Uint8Array>>()
  fromPackets = plain

  // An untyped stream comes out typed.
  const untyped = new ReadableStream({ start: (controller) => controller.close() })
  expectTypeOf(untyped.pipeThrough(new LengthCheck({ max: 4 }))).toEqualTypeOf<ReadableStream<Uint8Array>>()
  return [cur, streams, fromPackets]
}

// Every call below compiled against 0.1.0 and must keep compiling.
const compat = (
  LengthCheck: typeof LengthCheckClass,
  Rechunk: typeof RechunkClass,
  Digest: typeof DigestClass,
  bytes: ReadableStream<Uint8Array>,
  sink: WritableStream<Uint8Array>,
) => {
  // No type argument and no stream to infer one from: Uint8Array, as in 0.1.0.
  const check = new LengthCheck({ expected: 4 })
  expectTypeOf(check).toEqualTypeOf<LengthCheckClass<Uint8Array>>()
  expectTypeOf<LengthCheckClass>().toEqualTypeOf<LengthCheckClass<Uint8Array>>()
  expectTypeOf(check.readable).toEqualTypeOf<ReadableStream<Uint8Array>>()
  expectTypeOf(check.writable).toEqualTypeOf<WritableStream<Uint8Array>>()
  expectTypeOf<InstanceType<typeof LengthCheckClass>>().toEqualTypeOf<LengthCheckClass>()
  const pair: TransformStream<Uint8Array, Uint8Array> = check
  const annotated: LengthCheckClass = new LengthCheck({ min: 1, max: 4 })
  const ctor: new (options: LengthCheckOptions) => TransformStream<Uint8Array, Uint8Array> = LengthCheck

  // Piped straight through from a Uint8Array stream, and on into others.
  expectTypeOf(bytes.pipeThrough(new LengthCheck({ expected: 4 }))).toEqualTypeOf<ReadableStream<Uint8Array>>()
  expectTypeOf(bytes.pipeThrough(check)).toEqualTypeOf<ReadableStream<Uint8Array>>()
  const piped: ReadableStream<Uint8Array> = bytes.pipeThrough(new LengthCheck({ expected: 4 }))
  const done: Promise<void> = bytes.pipeThrough(check).pipeTo(sink)
  const chained = bytes
    .pipeThrough(new LengthCheck({ max: 4 }))
    .pipeThrough(new Rechunk({ size: 2 }))
    .pipeThrough(new Digest({ algorithm: 'crc32' }))
  expectTypeOf(chained).toEqualTypeOf<ReadableStream<Uint8Array>>()

  // Over a generic Uint8Array stream.
  const generic = <T extends Uint8Array>(source: ReadableStream<T>): ReadableStream<Uint8Array> =>
    source.pipeThrough(new LengthCheck({ max: 4 }))

  // Narrowed by instanceof.
  const value: unknown = check
  if (value instanceof LengthCheck) expectTypeOf(value.bytes).toEqualTypeOf<number>()
  return [pair, annotated, ctor, piped, done, generic]
}

// Subclasses, and a subclass where `typeof LengthCheck` is expected.
const subclass = (LengthCheck: typeof LengthCheckClass, bytes: ReadableStream<Uint8Array>) => {
  class CustomCheck extends LengthCheck {
    readonly label = 'custom'
  }
  class FixedCheck extends LengthCheck {
    constructor() {
      super({ expected: 4 })
    }
    override get bytes(): number {
      return super.bytes
    }
  }
  class BufferCheck extends LengthCheck<ArrayBuffer> {}
  const C: typeof LengthCheck = CustomCheck
  const custom: LengthCheckClass = new CustomCheck({ max: 4 })
  expectTypeOf(bytes.pipeThrough(new FixedCheck())).toEqualTypeOf<ReadableStream<Uint8Array>>()
  expectTypeOf(new BufferCheck({ max: 4 }).readable).toEqualTypeOf<ReadableStream<ArrayBuffer>>()
  return [C, custom]
}

// Still only byte chunks.
const narrow = (LengthCheck: typeof LengthCheckClass, strings: ReadableStream<string>) => {
  // @ts-expect-error a string stream
  strings.pipeThrough(new LengthCheck({ expected: 4 }))
  // @ts-expect-error a type argument that isn't bytes
  new LengthCheck<string>({ expected: 4 })
}

describe('LengthCheck types', () => {
  it('types the web LengthCheck for any byte chunk', () => {
    expectTypeOf<ByteChunk>().toEqualTypeOf<ArrayBufferLike | ArrayBufferView>()
    expect([anyChunk, uint8, compat, subclass, narrow].every((fn) => typeof fn === 'function')).toBe(true)
  })
})
