# Watchdog

`syke/node` · `syke/web`

Passes data through unchanged and fails the stream if the source goes quiet.
A connection that stalls without closing (a dead peer behind a NAT, a server
that hangs mid-response) otherwise leaves a stream waiting forever, holding
its resources. Socket and fetch timeouts often cover only connecting or
the headers, not a body that stops halfway.

```js
import { pipeline } from 'node:stream/promises'
import { Watchdog } from '@thethechad/syke/node'

await pipeline(res, new Watchdog({ timeout: 30_000 }), createWriteStream('download.bin'))
```

```js
import { Watchdog } from '@thethechad/syke/web'

await res.body
  .pipeThrough(new Watchdog({ timeout: 30_000, firstChunkTimeout: 120_000 }))
  .pipeTo(writable)
```

## Options

| Option | Description |
| ------ | ----------- |
| `timeout` | Milliseconds without a chunk before the stream fails. Required. |
| `firstChunkTimeout` | Milliseconds to wait for the first chunk. Defaults to `timeout`. |
| `objectMode` | In `syke/node`, puts the stream in object mode so it can watch record streams. `syke/web` streams take any chunk anyway. |

`syke/node` also takes `highWaterMark` and `signal`, passed to `stream.Transform`.

Types: `Watchdog<T = Uint8Array>`.

## When the clock runs

- **It starts at construction.** A transform can't tell when its consumer
  first asks for data, so create the `Watchdog` where you build the pipeline,
  not ahead of time. In Node, a `Watchdog` that times out with nobody
  listening for `'error'` crashes the process like any other unhandled stream
  error.
- **The first chunk gets `firstChunkTimeout`.** Many sources take longer to
  start than to continue: a server that thinks before it answers, or a
  generated response. Set a generous `firstChunkTimeout` and a tight
  `timeout` for the gaps after that.
- **Every chunk restarts it.** Empty chunks count.
- **A slow consumer doesn't count.** While downstream is applying
  backpressure, the source can't deliver, so the clock doesn't treat it as
  stalled. When the timer finds downstream full, it restarts the clock and
  checks again every `timeout / 4`. So once the consumer catches up, a source
  that then stalls fails between `timeout` and `1.25 × timeout` later.
- **It stops when the stream ends, fails, is destroyed or is cancelled.** The
  timer is cleared, so a finished stream never keeps the process alive or
  fails later. While the stream is open and waiting, the timer does keep a
  Node process alive: the stream is still in use.

In `syke/web`, the `Watchdog`'s readable side has a high-water mark of one
chunk (a `TransformStream` defaults to zero). With zero it couldn't tell a
consumer that is waiting for data from one that isn't reading. So it buffers
at most one chunk.

## Errors

`WatchdogError` (`code: 'ERR_SYKE_TIMEOUT'`) with `timeout` (the timeout that
expired, in milliseconds) and `firstChunk` (`true` if no data had arrived
yet). The stream is failed from a timer, so the error reaches the consumer
and, through `pipeline()` or `pipeTo()`, the source.
