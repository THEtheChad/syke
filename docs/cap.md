# Cap

`syke/node` · `syke/web`

Passes on at most `maxBytes` bytes (or `maxItems` records), then either fails
the stream or cuts it short. Its main job is to bound how much data an
untrusted source can make you handle. A compressed upload of a few kilobytes
can inflate to gigabytes (a *decompression bomb*), and putting `Cap` after the
decompressor stops it:

```js
import { Cap } from '@thethechad/syke/web'

const text = await new Response(
  req.body
    .pipeThrough(new DecompressionStream('gzip'))
    .pipeThrough(new Cap({ maxBytes: 10 * 1024 ** 2 })),
).text()
```

```js
import { pipeline } from 'node:stream/promises'
import { createGunzip } from 'node:zlib'
import { Cap } from '@thethechad/syke/node'

await pipeline(req, createGunzip(), new Cap({ maxBytes: 10 * 1024 ** 2 }), createWriteStream(path))
```

To keep only the start of a stream, such as a preview of a large file, use
`onLimit: 'truncate'`:

```js
const head = res.body.pipeThrough(new Cap({ maxBytes: 64 * 1024, onLimit: 'truncate' }))
```

## Cap or LengthCheck?

[`LengthCheck`](length-check.md) with `max` *verifies* a stream: it is for data
whose size you know or can bound, and anything else is an error. `Cap`
*limits* a stream: it can fail the same way, but it can also cut the stream
short and carry on, and it can count records instead of bytes. With
`onLimit: 'error'` and `maxBytes`, the two behave alike; pick `LengthCheck`
when you also need a minimum or an exact length.

## Options

Exactly one of `maxBytes` and `maxItems` is required.

| Option | Description |
| ------ | ----------- |
| `maxBytes` | Most bytes to pass on. |
| `maxItems` | Most chunks to pass on. In `syke/node` this puts the stream in object mode and counts records; in `syke/web` it counts chunks of any type. |
| `onLimit` | `'error'` (default) or `'truncate'`. |

`maxBytes` and `maxItems` are non-negative integers. One of 2^53 or more
can't be reached, so it works as no limit; `Infinity`, a fraction or a
negative number throws a `RangeError`.

`syke/node` also takes `highWaterMark` and `signal`, passed to `stream.Transform`.

`cap.bytes` and `cap.items` count what has been passed on, and
`cap.truncated` becomes `true` once `'truncate'` has discarded input. A stream
that ends exactly at the cap is not truncated: `Cap` waits for a chunk past
the cap before it decides.

Types: `Cap<T = Uint8Array>`, so record streams can be typed as
`new Cap<Row>({ maxItems: 1000 })`.

With `maxBytes`, chunks are counted by `byteLength`. In `syke/web` that's a
`Uint8Array`, any other view (`DataView`, `Uint16Array`, ...), an
`ArrayBuffer` or a `SharedArrayBuffer`; in `syke/node` the byte-mode stream
turns views and strings into `Buffer`s first. A chunk that fits is passed on
untouched. Anything else fails the stream with a `TypeError`.

## When the cap is reached

- **`'error'`:** fails the stream with a `CapError` before passing on any of
  the chunk that overruns, so the consumer never sees more than the cap.
- **`'truncate'`:** passes on the part of that chunk that fits (a `subarray`
  view, not a copy) and ends the output normally. The cut is in bytes, so the
  part of a chunk that isn't a `Uint8Array` (an `ArrayBuffer`, `DataView`,
  `Uint16Array`, ...) comes out as a `Uint8Array` view of its first bytes.
  `Cap<T>`'s output is typed as `T`, so for such chunks name both types:
  `new Cap<ArrayBuffer | Uint8Array>({ maxBytes, onLimit: 'truncate' })`.

After truncating, the two runtimes treat the rest of the source differently:

- **`syke/web`** terminates the `TransformStream`, which cancels a piped
  source. A decompressor upstream stops working right away.
- **`syke/node`** keeps reading the source and discarding it, so `pipeline()`
  only settles when the source ends. Node streams have no way for a
  transform to stop its source without `pipeline()` rejecting with
  `ERR_STREAM_PREMATURE_CLOSE`. Memory stays bounded, but a decompression bomb
  is still fully decompressed (into nothing) and an endless source never
  finishes. When the source may be unbounded or hostile, use the default
  `onLimit: 'error'`, which tears the pipeline down immediately.

## Errors

`CapError` (`code: 'ERR_SYKE_CAP'`) with `limit` (the cap) and `unit`
(`'bytes'` or `'items'`).
