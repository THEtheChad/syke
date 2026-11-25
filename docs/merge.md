# merge

`syke/node` · `syke/web`

Interleaves several sources into one stream, passing on chunks as they arrive
from any of them, and ends when every source has ended. Sources are read only
as fast as the output is consumed.

```js
import { pipeline } from 'node:stream/promises'
import { merge } from '@thethechad/syke/node'

await pipeline(merge([queueA, queueB, queueC]), processor)
```

```js
import { merge } from '@thethechad/syke/web'

const events = merge([socketEvents, pollEvents])
for await (const event of events) handle(event)
```

## Arguments

`merge(sources)` returns a Node `Readable` (in object mode) from `syke/node`,
or a `ReadableStream` from `syke/web`.

`sources` is an iterable or async iterable of sources, each a Node `Readable`
(`syke/node`), a `ReadableStream`, or an iterable or async iterable of chunks.
A string or byte array on its own is refused as a source (it's iterable, but
almost certainly meant as one chunk): wrap it in an array.

- Sources from a plain iterable (such as an array) are all opened when
  `merge()` is called, so an error in any of them fails the output at once.
- An async iterable of sources is read alongside them as the output is
  consumed: each source joins the merge when it arrives, and the output ends
  once the iterable and every source it gave have ended.

Chunks from one source keep their order; the order across sources is the
order they arrive in. No chunk is copied or combined.

## Backpressure and memory

Each source has at most one read in flight, and reads are only issued while
the consumer is waiting for data, so beyond the output's own buffer at most
one chunk per source is held. A slow source doesn't hold back the others.

## Errors and cancellation

- An error in any source (or in the async iterable of sources) errors the
  output, and every other source is cancelled (web) or destroyed (Node). An
  async iterable of sources is stopped with `return()`.
- Something that isn't a source, from an async iterable of sources, errors the
  output with a `TypeError`.
- Cancelling (web) or destroying (Node) the output cancels or destroys every
  source. See
  [What a stopped source gets](#what-a-stopped-source-gets).
- In `syke/node`, a source yielding `null` errors the output with a `TypeError`
  whose `code` is `'ERR_STREAM_NULL_VALUES'` (Node streams treat `null` as
  end-of-stream). Before 0.1.1, the output silently ended there instead.
- A non-iterable `sources`, or an invalid source in a plain iterable
  (including a locked `ReadableStream`), throws a `TypeError` from `merge()`.
  When `merge()` throws, it leaves every source as it found it: none is read,
  cancelled, destroyed or left locked.

### What a stopped source gets

When `merge` stops a source early, what the source sees depends on its kind:

| Source | What it gets |
| ------ | ------------ |
| Node `Readable` (`syke/node`) | `destroy()` with no error, whatever the reason it was stopped for. It emits `'close'` but no `'error'`, so nothing else wired to it (a `pipeline()` into it, the request behind an HTTP response, another combinator reading it) sees a failure it didn't cause. A stream already destroyed, or whose readable side has already ended, is left alone: a half-open `Duplex` keeps its writable side, and a stream with `autoDestroy: false` stays open. Before 0.1.1, one that had ended was destroyed too. (Some Node releases, 22 and 26 as of writing, destroy a half-open `Duplex` themselves once a reader reaches its end, whoever is reading it.) |
| `ReadableStream` | `cancel(reason)`, whatever the reason. |
| Iterable or async iterable | `return()`, without the reason (`throw()` is never called). |

In `syke/web`, a Node `Readable` is read as the async iterable it is, so it gets
`return()`: once it has been read from, Node's iterator then destroys it (with
an `AbortError`, not the reason); one never read from is left open.
