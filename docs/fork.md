# fork

`syke/node` · `syke/web`

Splits one source into several branches that each get every chunk, with
bounded memory. The web's `ReadableStream#tee()` buffers without limit for the
slower branch, so memory grows with the speed gap between consumers. `fork`
instead advances the source at the pace of the slowest branch, or, with a
`buffer`, lets a slow branch fall behind into a buffer of your choosing (such as
a `Spool`, which spills to disk).

```js
import { pipeline } from 'node:stream/promises'
import { fork, Digest } from '@thethechad/syke/node'

const [toDisk, toHash] = fork(res)
const digest = new Digest({ algorithm: 'sha256' })
await Promise.all([
  pipeline(toDisk, createWriteStream('download.bin')),
  pipeline(toHash, digest, new Writable({ write: (_chunk, _enc, done) => done() })),
])
```

```js
import { fork } from '@thethechad/syke/web'

const [a, b] = fork(res.body)
await Promise.all([a.pipeTo(upload), b.pipeTo(cache)])
```

A slow branch that shouldn't hold back the others (Node):

```js
import { fork, Spool } from '@thethechad/syke/node'

const [live, archive] = fork(res, { buffer: () => new Spool() })
```

## Arguments

`fork(source, options?)` returns an array of `count` branches: Node `Readable`s
from `syke/node`, `ReadableStream`s from `syke/web`.

`source` is a Node `Readable` (`syke/node`), a `ReadableStream`, or an iterable
or async iterable of chunks. A string or byte array on its own is refused (it's
iterable, but almost certainly meant as one chunk): wrap it in an array.

| Option | Description |
| ------ | ----------- |
| `count` | Number of branches. Defaults to `2`. |
| `buffer` | A function returning a new stream to pipe each branch through: a duplex in Node (`() => new Spool()`), a `{ writable, readable }` pair on the web (`() => new TransformStream({}, { highWaterMark: 1000 })`). Called once per branch, before the source is opened. |

From a Node `Readable` in byte mode, the branches are byte-mode too, with the
source's `highWaterMark`; otherwise they're in object mode.

## Pacing and memory

The source is read only when every live branch has asked for more, so it moves
at the pace of the slowest branch, and each branch holds at most its own
stream buffer. With `buffer`, each branch is drained into its buffer, so a slow
branch grows its buffer rather than holding back the source.

Read every branch concurrently (`Promise.all`, not one after another): without
a `buffer`, a branch nobody reads holds back all the others. A Node branch
doesn't ask for data until something reads it, so until then it counts as
stalled.

**Branches share chunk objects.** Each chunk is passed to every branch as the
same object (the same `Uint8Array`, the same record), so no consumer may
mutate one.

## Errors and cancellation

- A source error errors every branch with that error.
- Cancelling (web) or destroying (Node) one branch detaches only that branch;
  the others carry on, no longer held back by it.
- Once every branch is gone, the source is cancelled (web) or destroyed (Node).
- In `syke/node`, a source yielding `null` errors every branch with a `TypeError`
  whose `code` is `'ERR_STREAM_NULL_VALUES'` (Node streams treat `null` as
  end-of-stream). Before 0.1.1, every branch silently ended there instead.
- Invalid options, or an invalid source (including a locked `ReadableStream`),
  throw a `TypeError` or `RangeError` from `fork()`, before any `buffer` is
  made.
