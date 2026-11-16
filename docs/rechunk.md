# Rechunk

`syke/node` · `syke/web`

Re-cuts a byte stream into chunks of exactly `size` bytes. Only the last
chunk may be shorter. Sources hand out chunks of whatever size suits them (a
socket's read size, a decompressor's window), and some consumers need fixed
sizes:

- **Multipart uploads.** S3 parts must be at least 5 MiB (except the last).
- **Block writes.** Devices, encryption modes and archive formats that work in
  fixed-size blocks.
- **Fewer, larger chunks.** Every Web Streams chunk costs a promise and some
  bookkeeping, so a stream of many tiny chunks is slow. Coalescing them into
  64 KiB chunks before a long chain of transforms cuts that overhead.

```js
import { Rechunk } from '@thethechad/syke/node'

let partNumber = 0
for await (const part of file.pipe(new Rechunk({ size: 8 * 1024 ** 2 }))) {
  await uploadPart(++partNumber, part)
}
```

```js
import { Rechunk } from '@thethechad/syke/web'

const blocks = res.body.pipeThrough(new Rechunk({ size: 64 * 1024 }))
```

## Options

| Option | Description |
| ------ | ----------- |
| `size` | Bytes per output chunk, a positive integer. Required. |

`syke/node` also takes `highWaterMark` and `signal`, passed to `stream.Transform`.

## Copying

`Rechunk` copies as little as it can:

- When an input chunk holds one or more whole output chunks, they are passed
  on as `subarray` views of it, with no copy.
- Bytes are copied only to join pieces of different input chunks. The
  leftover end of an input chunk is copied into a fresh `size`-byte buffer
  straight away, so a producer that reuses its buffer can't change data that
  `Rechunk` is holding.
- A buffer is never written to after it has been passed on. Each joined chunk
  gets a buffer of its own.

So memory use is at most one partial chunk (`size` bytes) beyond what the
streams around it buffer.

## Node.js

A byte-mode Node stream joins buffered chunks together when it is read with
`read()` or `for await`, which would undo the re-cutting. So the readable side
of the Node version is in object mode: every chunk keeps its size however it is
read. Chunks are `Buffer`s. `highWaterMark` stays in bytes (default 64 KiB):
the writable side buffers that many bytes, and the readable side buffers
`highWaterMark / size` chunks, and at least one.

## Errors

`Rechunk` raises no errors of its own. A source error fails the stream, and
the partial chunk it was holding is dropped.
