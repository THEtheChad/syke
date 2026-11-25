// Compiled against the built declarations (dist/), as a consumer would:
// pnpm typecheck:dist, after pnpm build. Nothing here runs.
//
// Any byte chunk through the web LengthCheck, Lines and NdjsonParse.
import { type ByteChunk, LengthCheck, Lines, NdjsonParse } from '@thethechad/syke/web'

type Equals<X, Y> = (<T>() => T extends X ? 1 : 2) extends <T>() => T extends Y ? 1 : 2 ? true : false
declare function exact<A, B>(ok: Equals<A, B>): void
declare function typeOf<A>(value: A): { is<B>(ok: Equals<A, B>): void }

declare const res: Response
declare const buffers: ReadableStream<ArrayBuffer>
declare const shared: ReadableStream<SharedArrayBuffer>
declare const views: ReadableStream<DataView>
declare const words: ReadableStream<Uint16Array>
declare const mixed: ReadableStream<ByteChunk>
declare const bytes: ReadableStream<Uint8Array>
declare const strings: ReadableStream<string>

typeOf(buffers.pipeThrough(new LengthCheck({ max: 4 }))).is<ReadableStream<ArrayBuffer>>(true)
typeOf(shared.pipeThrough(new LengthCheck({ max: 4 }))).is<ReadableStream<SharedArrayBuffer>>(true)
typeOf(views.pipeThrough(new LengthCheck({ max: 4 }))).is<ReadableStream<DataView>>(true)
typeOf(words.pipeThrough(new LengthCheck({ max: 4 }))).is<ReadableStream<Uint16Array>>(true)
typeOf(mixed.pipeThrough(new LengthCheck({ max: 4 }))).is<ReadableStream<ByteChunk>>(true)
typeOf(bytes.pipeThrough(new LengthCheck({ max: 4 }))).is<ReadableStream<Uint8Array>>(true)
typeOf(res.body!.pipeThrough(new LengthCheck({ max: 4 }))).is<ReadableStream<Uint8Array>>(true)
typeOf(new LengthCheck<ArrayBuffer>({ max: 4 }).readable).is<ReadableStream<ArrayBuffer>>(true)
typeOf(new LengthCheck({ max: 4 })).is<LengthCheck<Uint8Array>>(true)
// Uint8Array<ArrayBuffer> on TypeScript 5.7+, as fetch hands it out.
type BodyChunk = NonNullable<Response['body']> extends ReadableStream<infer C> ? C : never
declare const someBytes: ReadableStream<BodyChunk | DataView>
typeOf(someBytes.pipeThrough(new LengthCheck({ max: 4 }))).is<ReadableStream<Uint8Array | DataView>>(true)
exact<ByteChunk, ArrayBufferLike | ArrayBufferView>(true)

typeOf(buffers.pipeThrough(new LengthCheck({ max: 4 })).pipeThrough(new Lines())).is<ReadableStream<string>>(true)
typeOf(views.pipeThrough(new NdjsonParse<{ a: number }>())).is<ReadableStream<{ a: number }>>(true)
typeOf(words.pipeThrough(new Lines())).is<ReadableStream<string>>(true)

// @ts-expect-error a string stream
strings.pipeThrough(new LengthCheck({ max: 4 }))
// @ts-expect-error not bytes
new LengthCheck<string>({ max: 4 })

// A subclass for another chunk type.
export class BufferCheck extends LengthCheck<ArrayBuffer> {}
typeOf(buffers.pipeThrough(new BufferCheck({ max: 4 }))).is<ReadableStream<ArrayBuffer>>(true)
