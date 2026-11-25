# LengthCheck

`syke/node` · `syke/web`

Passes bytes through unchanged and fails the stream if it carries more or fewer
bytes than allowed. Use it to catch truncated downloads (a connection that
closes early looks like a normal end to most code) or to refuse oversized
uploads.

```js
import { pipeline } from 'node:stream/promises'
import { LengthCheck } from '@thethechad/syke/node'

const res = await fetch(url)
await pipeline(
  res.body,
  new LengthCheck({ expected: Number(res.headers.get('content-length')) }),
  createWriteStream('download.bin'),
)
```

```js
import { LengthCheck } from '@thethechad/syke/web'

await res.body
  .pipeThrough(new LengthCheck({ max: 50 * 1024 ** 2 }))
  .pipeTo(writable)
```

## Options

At least one is required.

| Option | Description |
| ------ | ----------- |
| `expected` | Exact number of bytes the stream must carry. |
| `min` | Fewest bytes allowed. |
| `max` | Most bytes allowed. |

Each is a non-negative integer. One of 2^53 or more can't be reached, so it
works as no limit; `Infinity`, a fraction or a negative number throws a
`RangeError`.

`syke/node` also takes `highWaterMark` and `signal`, passed to `stream.Transform`.

`check.bytes` holds the number of bytes that have passed so far.

## Chunk types

`LengthCheck` only reads each chunk's `byteLength`, so it counts bytes in any
form and passes every chunk on untouched:

- **`syke/web`:** a `Uint8Array`, any other view (`DataView`, `Uint16Array`,
  ...), an `ArrayBuffer` or a `SharedArrayBuffer`, including one from another
  realm (an iframe, a `vm` context). A bare `SharedArrayBuffer` is only
  recognised where this realm has a `SharedArrayBuffer` global (in browsers, a
  cross-origin isolated page); views of one always are.
- **`syke/node`:** the byte-mode stream turns views and strings (as UTF-8)
  into `Buffer`s before `LengthCheck` sees them. Node rejects a bare
  `ArrayBuffer` itself.

Anything else fails the stream with a `TypeError` rather than being
miscounted.

### Types

In `syke/web`, `LengthCheck<T extends ByteChunk = Uint8Array>` is a
`TransformStream` whose writable side takes `T`. `ByteChunk`
(`ArrayBufferLike | ArrayBufferView`) is exported from `syke/web`. Passed
straight to `pipeThrough()`, `T` is inferred from the stream; built on its own
first, it defaults to `Uint8Array` unless you name it.

The readable side is typed by `LengthCheckOutput<T>`, also exported:

- **Any `Uint8Array`** (`Uint8Array<ArrayBuffer>`, as `fetch()` types a body,
  a subtype such as `Buffer`, or an untyped `any` stream) comes out as a plain
  `Uint8Array`, as in 0.1.0. The output can be reassigned to, or mixed with,
  any other `ReadableStream<Uint8Array>`.
- **Other byte chunks** come out as they went in: an `ArrayBuffer`,
  `SharedArrayBuffer`, `DataView`, `Uint16Array` or `ByteChunk` stream stays
  one. In a union each member is mapped on its own, so
  `Uint8Array<ArrayBuffer> | DataView` comes out as `Uint8Array | DataView`.

```ts
import { type ByteChunk, LengthCheck } from '@thethechad/syke/web'

declare const res: Response
const body = res.body!.pipeThrough(new LengthCheck({ expected: 1024 }))
//    ^? ReadableStream<Uint8Array>

declare const buffers: ReadableStream<ArrayBuffer>
const checked = buffers.pipeThrough(new LengthCheck({ expected: 1024 }))
//    ^? ReadableStream<ArrayBuffer>

const check = new LengthCheck<DataView>({ max: 1024 })
//    ^? LengthCheck<DataView>, a TransformStream<DataView, DataView>

function checkAny(source: ReadableStream<ByteChunk>): ReadableStream<ByteChunk> {
  return source.pipeThrough(new LengthCheck({ max: 1024 }))
}
```

`LengthCheck` can be subclassed (`class MyCheck extends LengthCheck {}`, or
`extends LengthCheck<ArrayBuffer>`), and a subclass can be used where a
`typeof LengthCheck` is expected.

The `syke/node` `LengthCheck` is a `stream.Transform`, so its chunks aren't
typed.

## Errors

A failed check errors the stream with a `LengthCheckError`
(`code: 'ERR_SYKE_LENGTH'`), whose `bytes` property is the count when it
failed:

- **Too long:** fails as soon as a chunk takes the total past `max` or
  `expected`. That chunk is not passed on.
- **Too short:** fails when the source ends short of `min` or `expected`,
  instead of ending the output. A consumer that waits for the end before
  committing (renaming a temp file, finishing a transaction) never commits
  truncated data.
