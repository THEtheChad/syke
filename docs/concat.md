# concat

`syke/node` · `syke/web`

Emits several sources one after another as a single stream. Sources can be
given as functions, so later ones (a file handle, an HTTP request) aren't
opened until the ones before them have ended.

```js
import { pipeline } from 'node:stream/promises'
import { concat } from '@thethechad/syke/node'

const parts = ['part-1', 'part-2', 'part-3'].map((name) => () => createReadStream(name))
await pipeline(concat(parts), createWriteStream('joined.bin'))
```

```js
import { concat } from '@thethechad/syke/web'

const body = concat(urls.map((url) => async () => (await fetch(url)).body))
await body.pipeTo(writable)
```

## Arguments

`concat(sources)` returns a Node `Readable` (in object mode) from `syke/node`,
or a `ReadableStream` from `syke/web`.

`sources` is an array, iterable or async iterable whose items are each:

- a source: a Node `Readable` (`syke/node`), a `ReadableStream`, or an
  iterable or async iterable of chunks; or
- a function returning a source (or a promise of one), called only when its
  turn comes.

A string or byte array on its own is refused as a source (it's iterable, but
almost certainly meant as one chunk): wrap it in an array.

Sources given directly in an **array** are opened when `concat()` is called:
they're watched for errors and can be cancelled with the rest, but they aren't
read until their turn. Items of any other iterable are taken one at a time, as
they're reached, so a generator can produce sources lazily.

Each source is read only as fast as the output is consumed.

## Errors and cancellation

- An error in any open source errors the output, including a source in an array
  that fails while waiting its turn (it fails the output at once). Every other
  open source is then cancelled (web) or destroyed (Node), and factories not
  yet called never are.
- A factory that throws, rejects or returns something that isn't a source
  errors the output the same way.
- Cancelling (web) or destroying (Node) the output cancels the current source,
  along with any sources from the array that are still waiting, and stops a
  generator of sources (`return()`). See
  [What a stopped source gets](#what-a-stopped-source-gets).
- In `syke/node`, a source yielding `null` errors the output with a `TypeError`
  whose `code` is `'ERR_STREAM_NULL_VALUES'` (Node streams treat `null` as
  end-of-stream). Before 0.1.1, the output silently ended there instead.
- A non-iterable `sources`, or an invalid source directly in an array
  (including a locked `ReadableStream`), throws a `TypeError` from `concat()`.
  When `concat()` throws, it leaves every source as it found it: none is read,
  cancelled, destroyed or left locked.

### What a stopped source gets

When `concat` stops a source early, what the source sees depends on its kind:

| Source | What it gets |
| ------ | ------------ |
| Node `Readable` (`syke/node`) | `destroy()` with no error, whatever the reason it was stopped for. It emits `'close'` but no `'error'`, so nothing else wired to it (a `pipeline()` into it, the request behind an HTTP response, another combinator reading it) sees a failure it didn't cause. A stream already destroyed, or whose readable side has already ended, is left alone: a half-open `Duplex` keeps its writable side, and a stream with `autoDestroy: false` stays open. Before 0.1.1, one that had ended was destroyed too. (Some Node releases, 22 and 26 as of writing, destroy a half-open `Duplex` themselves once a reader reaches its end, whoever is reading it.) |
| `ReadableStream` | `cancel(reason)`, whatever the reason. |
| Iterable or async iterable | `return()`, without the reason (`throw()` is never called). |

In `syke/web`, a Node `Readable` is read as the async iterable it is, so it gets
`return()`: once it has been read from, Node's iterator then destroys it (with
an `AbortError`, not the reason); one never read from is left open.
