# Batch

`syke/node` · `syke/web`

Groups records into arrays of up to `size`. Its main use is database bulk
inserts: one `INSERT` of 500 rows is far faster than 500 single-row inserts,
and with `maxWait` a slow trickle of records still gets written promptly
instead of sitting in a half-full batch.

```js
import { pipeline } from 'node:stream/promises'
import { Batch, NdjsonParse } from 'syke/node'

await pipeline(
  createReadStream('users.ndjson'),
  new NdjsonParse(),
  new Batch({ size: 500, maxWait: 1000 }),
  async (batches) => {
    for await (const rows of batches) await db.insert('users', rows)
  },
)
```

Because the consumer awaits each insert, backpressure reaches all the way back
to the file: at most a few batches are in memory at once, however large the
input.

```js
import { Batch } from 'syke/web'

await events
  .pipeThrough(new Batch({ size: 100, maxWait: 5000 }))
  .pipeTo(new WritableStream({ write: (batch) => fetch('/ingest', { method: 'POST', body: JSON.stringify(batch) }) }))
```

## Options

| Option | Default | Description |
| ------ | ------- | ----------- |
| `size` | required | Most records per batch, a positive integer. A batch is emitted as soon as it's full. |
| `maxWait` | none | Milliseconds. A partial batch is emitted once this long has passed since its first record arrived. Without it, a partial batch waits for more records or the end of the input. |

`syke/node` also takes `highWaterMark` (counted in batches on the output) and
`signal`, passed to `stream.Transform`. Both sides are in object mode.

## Behaviour

- Batches are new arrays; records are passed by reference, never copied.
- When the input ends, the last partial batch is emitted. An empty batch is
  never emitted.
- The `maxWait` clock starts at each batch's first record, not at the last
  emission, so an idle stream sets no timers. The timer is cleared when the
  batch fills, when the input ends, and when the stream fails or is destroyed
  or cancelled, so it never keeps a process alive after the stream is done.
- A batch emitted by the `maxWait` timer is queued even if the consumer is
  busy. That adds at most one partial batch to the output buffer; the input
  is still held back by normal backpressure.

## Errors

Batch raises no errors of its own; a source or consumer error passes through.
A missing `size` throws a `TypeError`, and a `size` that isn't a positive
integer or a `maxWait` that isn't a positive number throws a `RangeError`,
from the constructor.
