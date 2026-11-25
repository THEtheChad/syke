# Throttle

`syke/node` · `syke/web`

Limits a stream to an average rate, in bytes or records per second. Use it to
keep a background upload or download from saturating a link, to stay under
an API's rate limit, or to simulate a slow network in tests.

```js
import { pipeline } from 'node:stream/promises'
import { Throttle } from '@thethechad/syke/node'

// About 1 MB/s.
await pipeline(createReadStream(path), new Throttle({ bytesPerSecond: 1e6 }), upload)
```

```js
import { Throttle } from '@thethechad/syke/web'

// At most 10 records a second, one at a time.
await records.pipeThrough(new Throttle({ itemsPerSecond: 10, burst: 1 })).pipeTo(api)
```

`Throttle` holds back its source rather than buffering: while a chunk waits
its turn, the stream stops taking new ones, and backpressure reaches the
source.

## Options

Exactly one of `bytesPerSecond` and `itemsPerSecond` is required.

| Option | Description |
| ------ | ----------- |
| `bytesPerSecond` | Average bytes per second. |
| `itemsPerSecond` | Average chunks per second. In `syke/node` this puts the stream in object mode and counts records; in `syke/web` it counts chunks of any type. Fractions are allowed: `0.5` is one every two seconds. |
| `burst` | Bucket size, in bytes or items: how far the stream may run ahead of the rate after a pause. Defaults to one second's worth of the rate. |

`syke/node` also takes `highWaterMark` and `signal`, passed to `stream.Transform`.

Types: `Throttle<T = Uint8Array>`.

With `bytesPerSecond`, each chunk costs its `byteLength` and is passed on
untouched. In `syke/web` that's a `Uint8Array`, any other view (`DataView`,
`Uint16Array`, ...), an `ArrayBuffer` or a `SharedArrayBuffer`. In `syke/node`
the byte-mode stream turns views and strings into `Buffer`s first. Anything
else fails the stream with a `TypeError` rather than passing for free.

## How it works

`Throttle` is a token bucket that allows debt. The bucket holds up to `burst`
tokens and refills at the rate. It starts full, so the first `burst` bytes (or
items) pass at once. Then:

- A chunk waits until the bucket holds its cost, then takes it out.
- A chunk bigger than the bucket waits only for a full bucket, then takes its
  whole cost out, leaving the bucket in debt. The chunks after it wait the
  debt off. Chunks are never split, and the average rate still holds.
- A smaller `burst` gives a smoother rate; a larger one lets the stream catch
  up after a pause. For steady pacing of records, use `burst: 1`.

Waiting happens in the transform: it returns a promise that settles when the
chunk may pass. The chunk being delayed is the only thing `Throttle` holds.

## Ending early

When the stream is destroyed (Node) or its readable side is cancelled (web),
the pending delay's timer is cleared at once, so it never keeps the process
alive. One caveat on the web: when the source fails or the writable side is
aborted while a chunk is waiting, the Web Streams spec lets the in-flight
write finish first. So that chunk is still passed on when its delay ends, and
the error follows. The wait is no longer than the chunk's own delay.

## Errors

`Throttle` raises no errors of its own.
