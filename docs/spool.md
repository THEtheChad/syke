# Spool

`syke/node`

A pass-through stream that doesn't apply backpressure to its source. Whatever the
consumer isn't ready for is spooled to a temporary file and read back, a bounded
chunk at a time, as the consumer catches up.

Reach for it when:

1. **A network source would time out under backpressure.** You're piping an HTTP
   response, socket or upload into something slow (a database, a rate-limited API,
   a busy disk), and pausing the source long enough gets the connection dropped.
2. **You have many slow pipelines in parallel** and don't want each one's backlog
   held in memory.
3. **A database cursor would time out** behind a slow consumer. In
   [object mode](#object-mode) the spool carries records rather than bytes.

Memory stays bounded by the stream's high-water mark plus `chunkSize` (and
`memoryThreshold`, if you set one), however far the consumer falls behind. Disk
usage tracks the backlog, not the stream: space the consumer has already read is
reused before the spool file grows. By default it also stops spooling before the
disk drops below 10% free (see [Disk limits](#disk-limits)), and the file is
removed even if the process exits mid-stream.

## Usage

```js
import { pipeline } from 'node:stream/promises'
import { Spool } from 'syke/node'

const res = await fetch('https://example.com/large-dataset.ndjson')

await pipeline(res.body, new Spool(), slowDatabaseWriter)
```

CommonJS works too:

```js
const { Spool } = require('syke/node')
```

Use `pipeline` rather than chained `.pipe()` calls so that an error anywhere in
the chain destroys every stream and the temporary file is removed.

## `Spool.from()` and `Spool.toWeb()`

Following Node's `Readable.from()` convention, `Spool.from(source, options?)`
reads a source through a new spool. It accepts a Node readable, a web
`ReadableStream` (such as `fetch().body`), or an iterable or async iterable of
chunks. Errors and destruction propagate both ways, as with `pipeline`.

```js
const res = await fetch('https://example.com/large-dataset.ndjson')
Spool.from(res.body).pipe(slowDatabaseWriter)
```

`Spool.toWeb(spool)` (inherited from `Duplex`) turns a spool into a
`{ readable, writable }` pair for Web Streams code:

```js
await res.body
  .pipeThrough(Spool.toWeb(new Spool()))
  .pipeTo(slowWebWritable)
```

## Options

`new Spool(options?)`

| Option                  | Default                     | Description                                                                                                  |
| ----------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `objectMode`            | `false`                     | Carry records instead of bytes. See [Object mode](#object-mode).                                             |
| `codec`                 | `v8Codec`                   | Object mode only: how records are written to and read back from disk.                                        |
| `dir`                   | `os.tmpdir()`               | Directory for the spool file.                                                                                |
| `chunkSize`             | readable high-water mark (64 KiB in object mode) | Largest chunk, in bytes, read back from disk and pushed downstream.                     |
| `memoryThreshold`       | `0`                         | Bytes (records, in object mode) to hold in memory before spooling to disk. Small bursts under this size never touch disk. |
| `minFreeSpace`          | `'10%'`                     | Free space to leave on `dir`'s filesystem: a percentage (`'10%'`), bytes, or `false` for no floor.           |
| `maxDiskBytes`          | no cap                      | Largest size the spool file may reach, in bytes.                                                             |
| `onLimit`               | `'backpressure'`            | What happens at a disk limit: `'backpressure'` pauses the source, `'error'` fails the stream.               |
| `highWaterMark`, `readableHighWaterMark`, `writableHighWaterMark`, `signal` | Node defaults | Passed through to `stream.Duplex`. Aborting `signal` destroys the stream and removes the file. |

By default the stream is byte-oriented: strings are written as UTF-8 buffers.
Pass `objectMode: true` to carry records instead.

`spool.filepath` holds the spool file's path once the stream first spills to disk
(it's `null` until then).

## Object mode

`new Spool({ objectMode: true })` carries records (any JavaScript value except
`null`) instead of bytes, for sources such as database cursors, which can time
out behind a slow consumer just like a network connection:

```js
import { pipeline } from 'node:stream/promises'
import { Spool } from 'syke/node'

await pipeline(db.collection('events').find().stream(), new Spool({ objectMode: true }), slowWriter)

// Or wrap the cursor directly; any iterable or async iterable of records works.
Spool.from(cursor, { objectMode: true }).pipe(slowWriter)
```

Records only pay for serialization when they actually spill:

- A record the consumer is ready for is handed through as the same object.
- A record held in memory (under `memoryThreshold`) also stays as it is.
- Only a record that spills to disk is encoded by the `codec`, written with a
  length prefix, and decoded when it's read back. What comes out is a copy, so
  mutating a record after writing it affects it only if it didn't spill.

Order is preserved across all three paths, exactly as in byte mode.

### Codecs

A codec is any `{ encode(value): Uint8Array, decode(bytes): value }`. It must
round-trip every record the stream carries. Otherwise a record comes out
differently depending on whether it happened to spill.

- **`v8Codec`** (the default) uses `v8.serialize()`, the structured-clone format.
  It keeps Dates, Maps, Sets, typed arrays, Buffers, `RegExp`s, BigInts,
  `undefined` and circular references. Class instances come back as plain
  objects, and functions and symbols can't be encoded at all.
- **`jsonCodec`** is JSON over UTF-8. It's smaller for plain data, but Dates
  come back as strings, and `undefined` and functions are lost.
- Bring your own for compact or schema-based formats:

```js
import { pack, unpack } from 'msgpackr'
import { Spool, jsonCodec } from 'syke/node'

new Spool({ objectMode: true, codec: jsonCodec })
new Spool({ objectMode: true, codec: { encode: pack, decode: unpack } })
```

If a codec throws, `encode()` doesn't return a `Uint8Array`, or `decode()`
returns `null`, the stream fails with a `SpoolCodecError`
(`code: 'ERR_SYKE_SPOOL_CODEC'`), with the codec's own error as `cause`.
An encode error surfaces when the record spills, which may be long after the
codec would have been happy with every earlier record.

### Options in object mode

- **`highWaterMark`** follows Node's object-mode semantics: it counts records,
  and defaults to 16.
- **`memoryThreshold`** counts records, not bytes. Measuring bytes would mean
  encoding every record, which is the cost object mode avoids.
- **`chunkSize`** is still in bytes (default 64 KiB). It sizes reads from disk,
  and records that queue up while a spill is in flight are encoded and written
  together in batches of about this size. A record larger than `chunkSize` works.
  It's read back over several reads and reassembled before it's decoded.
- **`maxDiskBytes`** and **`minFreeSpace`** are in bytes of encoded data,
  including a 4-byte length prefix per record. A record larger than
  `maxDiskBytes` still gets through: it's written in pieces as the consumer
  reads the earlier ones.

Memory stays bounded by the high-water marks and `memoryThreshold` (in records),
plus about one `chunkSize` of records being encoded for disk and one being
decoded from it, plus the largest single record when one is bigger than
`chunkSize`.

### `null`

Node object-mode streams use `null` to mean "end of stream", so a spool can't
carry it. Writing `null` throws `ERR_STREAM_NULL_VALUES` (from Node itself), and
a `Spool.from()` source that yields `null` fails the stream with the same code.
Data is never silently dropped. Wrap nullable values (`{ value: null }`) or map
`null` to `undefined` if you need to carry them.

### Fast sources

The zero-serialization path depends on the consumer keeping up. A record can
only skip the disk when nothing older is still on it, so once a backlog has
spilled, newer records follow it through disk until it drains. A source that
produces records synchronously and faster than the disk can absorb them, such as
`Spool.from(hugeArray)`, can keep the spool on the disk path for the whole
stream. Real cursors deliver records in batches with I/O between them, which
gives the backlog room to drain.

## Disk limits

Two limits keep the spool file from filling the disk:

- **`minFreeSpace`** (default `'10%'`) checks the filesystem's real free space
  whenever the spool file is about to grow, and won't grow it past the floor.
  Because it reads live free space, every stream (and every other process) shares
  the same floor, so many parallel spools can't oversubscribe the disk. If free
  space can't be read on a platform, the floor isn't enforced.
- **`maxDiskBytes`** caps the size of one stream's spool file.

At a limit, new data goes into any space the consumer has already read, so the
source resumes as soon as the consumer frees some room rather than after the
whole backlog drains.

If there's no room at all, `onLimit` decides:

- `'backpressure'` (default): the source is paused, as with an ordinary stream,
  until the consumer frees space or free space recovers. Data is never dropped.
- `'error'`: the stream fails with a `SpoolLimitError`
  (`code: 'ERR_SYKE_SPOOL_LIMIT'`), for when a stalled source is worse than a
  failed one.

```js
import { Spool, SpoolLimitError } from 'syke/node'

new Spool({ maxDiskBytes: 2 * 1024 ** 3, minFreeSpace: '20%', onLimit: 'error' })
```

### Memory-backed temp directories

On some Linux systems `/tmp` is a `tmpfs`, which lives in RAM, so spooling "to
disk" there still uses memory. The first time a spool writes into a tmpfs or
ramfs directory, it emits a process warning (`code: 'SYKE_SPOOL_TMPFS'`). Pass a
`dir` on a real disk to fix it.

## Cleanup on exit

The spool file is deleted when the stream ends, errors or is destroyed, and also
if the process exits mid-stream (`process.exit()` or the event loop emptying),
through a single shared `exit` listener. A spool that's dropped without being
ended or destroyed has its file closed and deleted once it's garbage-collected,
so abandoned streams don't accumulate files in a long-running process.

Node doesn't emit `exit` when the process is killed by a signal it doesn't
handle, such as `SIGINT` or `SIGTERM`. If your app doesn't already handle those,
add a handler that exits normally so the cleanup runs:

```js
for (const signal of ['SIGINT', 'SIGTERM', 'SIGHUP']) {
  process.once(signal, () => process.exit(1))
}
```

syke doesn't install signal handlers itself, since that would change how your app
shuts down. Nothing can clean up after `SIGKILL` or a crash of the OS; files left
that way are named `syke-spool-*` in the spool directory.

## How it works

- If the consumer is waiting and nothing is queued, a chunk is handed straight
  through without touching disk (or, in object mode, the codec).
- Otherwise it goes to memory (up to `memoryThreshold`) or is appended to the
  spool file. Order is always preserved.
- The spool file is created lazily with mode `0600`, so a stream that never falls
  behind never creates one.
- The file holds unread data as a short list of regions. Reads take from the
  oldest region; writes extend the newest one or start a new region in space the
  consumer has already read (at least `chunkSize` bytes of it), and the file only
  grows when no such space exists.
- The file is closed and deleted when the stream ends, errors, is destroyed or
  the process exits.

## Prior art

[`fs-capacitor`](https://github.com/mike-marcacci/fs-capacitor) also buffers to a
temporary file, and supports multiple readers of the same buffered data. Use it if
you need to replay the stream. `Spool` is a single pass-through: it skips the disk
while the consumer keeps up, reuses space as data is read, and can cap disk use,
but it can't replay data that's already been read.
