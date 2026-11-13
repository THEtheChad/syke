# ParallelMap

`syke/node` · `syke/web`

Maps each record through an async function with several calls in flight at
once, such as looking each row up in another service, geocoding addresses or
writing records one by one to an API. With `concurrency: 8` it runs eight
calls at a time instead of one, while still applying backpressure: it never
pulls more input than it has room for, so memory stays bounded however long
the stream is.

Node has `readable.map(fn, { concurrency })`, but it's still experimental, and
Web Streams have nothing like it.

```js
import { pipeline } from 'node:stream/promises'
import { NdjsonParse, NdjsonStringify, ParallelMap } from 'syke/node'

await pipeline(
  createReadStream('orders.ndjson'),
  new NdjsonParse(),
  new ParallelMap(
    async (order, { signal }) => {
      const res = await fetch(`https://tax.example/rate?zip=${order.zip}`, { signal })
      return { ...order, taxRate: (await res.json()).rate }
    },
    { concurrency: 8 },
  ),
  new NdjsonStringify(),
  createWriteStream('orders-with-tax.ndjson'),
)
```

```js
import { ParallelMap } from 'syke/web'

const thumbnails = images.pipeThrough(
  new ParallelMap((image, { signal }) => makeThumbnail(image, signal), { concurrency: 4, ordered: false }),
)
```

## The function

`fn(item, { signal })` may return a value or a promise of one.

- **Filtering:** a result of `null` or `undefined` is dropped, so one
  ParallelMap can map and filter. That also keeps `null` out of Node
  object-mode streams, which treat it as the end.
- **`signal`** is an `AbortSignal`, aborted when the stream fails (another
  call threw), is destroyed or cancelled, or its consumer stops early. Pass it
  on to `fetch`, your database driver and so on, so abandoned work stops
  instead of running on in the background. One signal is shared by every call
  on a stream.

## Options

| Option | Default | Description |
| ------ | ------- | ----------- |
| `concurrency` | required | Most calls to `fn` in flight at once, a positive integer. |
| `ordered` | `true` | Emit results in input order. With `false`, each result is emitted as soon as it's ready. |

`syke/node` also takes `highWaterMark` (counted in records) and `signal`,
passed to `stream.Transform`. Both sides are in object mode.

## Ordering and memory

Each item takes a *slot* from the moment `fn` is called. While all
`concurrency` slots are taken, ParallelMap takes no more input, and the source
is held back.

- **`ordered: false`:** a slot frees as soon as its call finishes and its
  result is emitted. A slow item holds up only its own slot.
- **`ordered: true`:** a result can only be emitted once every earlier one
  has, so a finished result waits behind a slow one *and keeps its slot*
  until it's emitted. That bounds the results held back to `concurrency - 1`,
  whatever the latencies, but one very slow item stalls the stream until it
  finishes (head-of-line blocking). If order doesn't matter, as with most
  inserts and lookups whose result carries its own key, use
  `ordered: false` for steadier throughput.

Emitted results go to the output buffer, which normal stream backpressure
bounds: while nobody reads it, no new input is accepted, so a stalled consumer
leaves at most about `concurrency` results plus the output's high-water mark
in memory.

When the input ends, the output ends only after every call in flight has
finished and its result has been emitted.

## Errors

- **`fn` throws or rejects:** the stream fails with that error as soon as it
  happens, without waiting for the other calls. The other calls' `signal` is
  aborted with the error as its `reason`, no more calls start, and results not
  yet emitted are discarded (in order, including results that finished after
  an earlier item that's still running).
- **The stream is destroyed or cancelled**, or its consumer stops early: the
  calls in flight are aborted (`signal.reason` is an `AbortError`), and their
  results are discarded.

A missing `fn` or `concurrency`, or an `ordered` that isn't a boolean, throws
a `TypeError` from the constructor, and a `concurrency` that isn't a positive
integer throws a `RangeError`.
