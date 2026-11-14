# concat

`syke/node` · `syke/web`

Emits several sources one after another as a single stream. Sources can be
given as functions, so later ones (a file handle, an HTTP request) aren't
opened until the ones before them have ended.

```js
import { pipeline } from 'node:stream/promises'
import { concat } from 'syke/node'

const parts = ['part-1', 'part-2', 'part-3'].map((name) => () => createReadStream(name))
await pipeline(concat(parts), createWriteStream('joined.bin'))
```

```js
import { concat } from 'syke/web'

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
  generator of sources (`return()`).
- A non-iterable `sources`, or an invalid source directly in an array, throws a
  `TypeError` from `concat()`.
