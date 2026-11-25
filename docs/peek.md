# peek

`syke/node` · `syke/web`

Reads the first bytes of a stream without consuming them. You get the head,
to sniff magic bytes or a header, and a stream that replays it followed by the
rest of the source, read on demand rather than into memory.

```js
import { pipeline } from 'node:stream/promises'
import { createGunzip } from 'node:zlib'
import { peek } from '@thethechad/syke/node'

const { head, stream } = await peek(res, 2)
const gzipped = head[0] === 0x1f && head[1] === 0x8b
await pipeline(stream, ...(gzipped ? [createGunzip()] : []), createWriteStream('data.json'))
```

```js
import { peek } from '@thethechad/syke/web'

const { head, stream } = await peek(res.body, 2)
const body = head[0] === 0x1f && head[1] === 0x8b ? stream.pipeThrough(new DecompressionStream('gzip')) : stream
```

## Arguments

`peek(source, bytes)` returns a promise of `{ head, stream }`:

- `head`: a `Uint8Array` of the first `bytes` bytes, or fewer if the source
  ends sooner. It's a copy, safe to keep and modify.
- `stream`: the whole source, head included: a Node `Readable` from
  `syke/node`, a `ReadableStream` from `syke/web`. Chunks pass through as they
  came, so the head arrives in the source's own chunks.

`source` is a Node `Readable` (`syke/node`), a `ReadableStream`, or an iterable
or async iterable of `Uint8Array` chunks. Only the chunks covering the head are
held in memory; they're released as `stream` replays them. From a Node
`Readable` in byte mode, `stream` is byte-mode too, with the source's
`highWaterMark`; otherwise it's in object mode.

`bytes` is a non-negative integer; `peek(source, 0)` reads nothing and gives
an empty head.

If you don't use `stream`, cancel or destroy it so the source is released.

## Errors

- A source error while peeking rejects the promise with that error, and the
  source is cancelled (web) or destroyed (Node).
- A chunk within the head that isn't a `Uint8Array` (a Node `Buffer` is one)
  rejects with a `TypeError`. Chunks after the head aren't checked.
- A source error after peeking errors `stream`.
- Cancelling or destroying `stream` cancels or destroys the source. See
  [What a stopped source gets](#what-a-stopped-source-gets).
- An invalid `bytes` or `source` throws a `TypeError` or `RangeError` from
  `peek()` itself, not through the promise.

### What a stopped source gets

When `peek` stops a source early, what the source sees depends on its kind:

| Source | What it gets |
| ------ | ------------ |
| Node `Readable` (`syke/node`) | `destroy()` with no error, whatever the reason it was stopped for. It emits `'close'` but no `'error'`, so nothing else wired to it (a `pipeline()` into it, the request behind an HTTP response, another combinator reading it) sees a failure it didn't cause. A stream already destroyed, or whose readable side has already ended, is left alone: a half-open `Duplex` keeps its writable side, and a stream with `autoDestroy: false` stays open. Before 0.1.1, one that had ended was destroyed too. (Some Node releases, 22 and 26 as of writing, destroy a half-open `Duplex` themselves once a reader reaches its end, whoever is reading it.) |
| `ReadableStream` | `cancel(reason)`, whatever the reason. |
| Iterable or async iterable | `return()`, without the reason (`throw()` is never called). |

In `syke/web`, a Node `Readable` is read as the async iterable it is, so it gets
`return()`: once it has been read from, Node's iterator then destroys it (with
an `AbortError`, not the reason); one never read from is left open.
