# Meter

`syke/node` · `syke/web`

Passes data through unchanged while counting it. Use it for upload and
download progress bars, or to log how fast a pipeline runs.

```js
import { pipeline } from 'node:stream/promises'
import { Meter } from 'syke/node'

const total = Number(res.headers['content-length'])
await pipeline(
  res,
  new Meter({ onProgress: ({ bytes }) => bar.update(bytes / total) }),
  createWriteStream('download.bin'),
)
```

```js
import { Meter } from 'syke/web'

const meter = new Meter()
await res.body.pipeThrough(meter).pipeTo(writable)
console.log(`${meter.bytes} bytes at ${(meter.bytesPerSecond / 1e6).toFixed(1)} MB/s`)
```

## Options

All optional.

| Option | Description |
| ------ | ----------- |
| `onProgress` | Called with a `MeterStats` snapshot. See below for when. |
| `interval` | Minimum milliseconds between `onProgress` calls. Default 1000; `0` calls it for every chunk. |
| `objectMode` | Count records. In `syke/node` this puts the stream in object mode; `syke/web` streams take any chunk anyway. |

`syke/node` also takes `highWaterMark` and `signal`, passed to `stream.Transform`.

## Counters

`meter.bytes`, `meter.chunks`, `meter.elapsedMs`, `meter.bytesPerSecond` and
`meter.chunksPerSecond` can be read at any time. `meter.stats` returns all five
as one `MeterStats` object, the same shape `onProgress` gets.

- `bytes` counts byte chunks (`Uint8Array` and other `ArrayBuffer` views, and
  `ArrayBuffer`s) and strings, as their UTF-8 length. Other records count
  towards `chunks` only.
- `elapsedMs` runs from the first chunk, not from construction, so time spent
  waiting for a slow server to respond doesn't drag the average down. It stops
  when the stream ends, errors or is destroyed, so the rates stay put
  afterwards.
- The rates are averages over the whole of `elapsedMs`, not a moving window,
  and are `0` until time has passed.

## Progress callbacks

`onProgress` is called on the first chunk, then on any chunk that arrives at
least `interval` ms after the previous call, and once more when the stream
ends successfully (even an empty stream). It isn't called on a timer, so a
stalled stream reports nothing until data moves again; pair `Meter` with
[`Watchdog`](watchdog.md) to fail a stall. A stream that fails gets no final
call.

The callback runs inline, before the next chunk. If it throws, the stream
fails with that error.

## Errors

`Meter` raises no errors of its own.
