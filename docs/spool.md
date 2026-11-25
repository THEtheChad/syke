# Spool

`syke/node` · `syke/web`

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
reused before the spool file grows, and the file is truncated back to zero once
the backlog drains. By default it also stops spooling before the
disk drops below 10% free (see [Disk limits](#disk-limits)), and the file is
removed even if the process exits mid-stream.

The sections up to [Prior art](#prior-art) describe `syke/node`. The browser
version, which spills to the origin's storage (OPFS or IndexedDB) instead of a
temp file, is described in [In the browser](#in-the-browser-sykeweb).

## Usage

```js
import { pipeline } from 'node:stream/promises'
import { Spool } from '@thethechad/syke/node'

const res = await fetch('https://example.com/large-dataset.ndjson')

await pipeline(res.body, new Spool(), slowDatabaseWriter)
```

CommonJS works too:

```js
const { Spool } = require('@thethechad/syke/node')
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

By default the stream is byte-oriented: strings are written as UTF-8 buffers,
and empty (0-byte) chunks are ignored. Pass `objectMode: true` to carry records
instead.

`spool.filepath` holds the spool file's path once the stream first spills to disk
(it's `null` until then).

## Object mode

`new Spool({ objectMode: true })` carries records (any JavaScript value except
`null`) instead of bytes, for sources such as database cursors, which can time
out behind a slow consumer just like a network connection:

```js
import { pipeline } from 'node:stream/promises'
import { Spool } from '@thethechad/syke/node'

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
  come back as strings, nested `undefined`/function properties are dropped, and
  top-level `undefined`, functions or symbols fail with `SpoolCodecError`.
- Bring your own for compact or schema-based formats:

```js
import { pack, unpack } from 'msgpackr'
import { Spool, jsonCodec } from '@thethechad/syke/node'

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

- **`minFreeSpace`** (default `'10%'`) checks the filesystem's free space
  whenever the spool file is about to grow, and won't grow it past the floor.
  Readings are cached briefly (100 ms) per directory, with in-process growth
  counted in between, so many parallel spools share the same floor without
  oversubscribing the disk. If free space can't be read on a platform, the floor
  isn't enforced.
- **`maxDiskBytes`** caps the size of one stream's spool file.
- **`ENOSPC` / `EDQUOT`** from the OS is also treated as hitting a disk limit.

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
import { Spool, SpoolLimitError } from '@thethechad/syke/node'

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
  grows when no such space exists. Once the backlog drains completely, the file
  is truncated back to zero.
- The file is closed and deleted when the stream ends, errors, is destroyed or
  the process exits.

## In the browser (`syke/web`)

`syke/web` has a `Spool` with the same job and, where they make sense, the same
options. Whatever the consumer isn't ready for is spilled to the page's origin
storage, in [OPFS](https://developer.mozilla.org/docs/Web/API/File_System_API/Origin_private_file_system)
or, as a fallback, IndexedDB, and read back as the consumer catches up.

```js
import { Spool } from '@thethechad/syke/web'

const res = await fetch('https://example.com/large-dataset.ndjson')
await res.body.pipeThrough(new Spool()).pipeTo(slowWritable)

// Or, like Node's Spool.from():
Spool.from(res.body).readable.pipeTo(slowWritable)
```

A `Spool` is a `{ readable, writable }` pair, so it works with `pipeThrough()`.
It isn't a `TransformStream`, because a `TransformStream` applies backpressure
to its writable side whenever its readable side is full, which is exactly what
a spool exists to avoid. Writes resolve as soon as a chunk is queued for
storage, however far behind the consumer is, until a [limit](#limits) is
reached.

`Spool.from(source, options?)` reads a `ReadableStream` or an iterable or async
iterable through a new spool and returns the spool. An invalid or already-locked
source throws synchronously. Errors and cancellation propagate both ways, as
with `pipeTo()`.

### Options

`new Spool(options?)`

| Option            | Default          | Description |
| ----------------- | ---------------- | ----------- |
| `chunkSize`       | `65536`          | Largest chunk, in bytes, read back from storage at once (and, for bytes, pushed downstream). |
| `memoryThreshold` | `0`              | How much to hold in memory before spilling: bytes, or records in object mode. |
| `minFreeSpace`    | `'10%'`          | Free storage to leave for the origin: a percentage of its quota (`'10%'`), bytes, or `false` for no floor. See [Limits](#limits). |
| `maxDiskBytes`    | no cap           | Most bytes this spool may keep in storage. |
| `onLimit`         | `'backpressure'` | What happens at a limit: `'backpressure'` pauses the source, `'error'` fails the stream with a `SpoolLimitError`. |
| `storage`         | `'auto'`         | `'auto'`, `'opfs'` or `'indexeddb'`. See [Storage](#storage). |
| `objectMode`      | `false`          | Carry any values instead of `Uint8Array` chunks. See [Object mode](#object-mode-in-the-browser). |
| `codec`           | `jsonCodec`      | In object mode, how records are turned into bytes when spilled: `{ encode(value): Uint8Array, decode(bytes) }`. |

The cap keeps Node's name, `maxDiskBytes`, so options can be shared between
the two versions: OPFS files and IndexedDB records live on disk too, in the
browser profile. In byte mode, empty (0-byte) chunks are ignored. To abort a
spool, pass a `signal` to `pipeThrough()` or `pipeTo()`; there's no `signal`
option.

Properties:

- `spool.storage` is where the backlog went: `'opfs'`, `'indexeddb'`, or
  `'memory'` if no storage was available. It's `null` until the spool first
  spills, since a spool whose consumer keeps up never opens storage.
- `spool.closed` settles once the spool's storage has been deleted, after the
  stream ends, errors or is cancelled. It rejects only if deleting failed.

### Storage

With `storage: 'auto'` (the default), a spool tries, in order:

1. **OPFS**, through a sync access handle (`createSyncAccessHandle()`) in a
   dedicated worker. Sync access handles only exist in workers, and they're by
   far the fastest browser storage: hundreds of MB/s for 64 KiB writes and over
   1 GB/s for larger ones. syke can't ship a separate worker file, so the worker
   is created from a Blob URL holding its source (a string, so bundlers and
   minifiers leave it alone). One worker serves every spool on the page and is
   terminated when the last one closes. Each spool gets one file, space the
   consumer has read is reused before the file grows, and the file is truncated
   back to zero once the backlog drains, as in Node.
2. **IndexedDB**, if the worker can't start (see [CSP](#content-security-policy))
   or the browser has no sync access handles. Records go in one shared database,
   `syke-spool`, keyed by spool and sequence number, in transactions of up to
   1 MiB with relaxed durability, since a transaction can cost ~2.5 ms. It
   managed roughly 100-300 MB/s in our tests.
3. **Nothing.** If neither is available (Node, Deno, Bun, a sandboxed iframe),
   the spool has no room at all, and `onLimit` decides: `'backpressure'` turns it
   into an ordinary pass-through that pauses the source (memory stays bounded),
   `'error'` fails the stream with a `SpoolLimitError`. In Node, use
   `syke/node`'s `Spool`.

`storage: 'opfs'` or `'indexeddb'` uses only that backend, and fails the stream
with a `SpoolStorageError` (`code: 'ERR_SYKE_SPOOL_STORAGE'`) if it's
unavailable. Storage is opened the first time a spool spills, and if the source
closes before that open settles, the stream waits for it so a missing backend
still surfaces `SpoolStorageError`.

### Content Security Policy

The OPFS worker runs from a `blob:` URL. A CSP whose `worker-src` (or, without
one, `child-src` or `script-src`) doesn't allow `blob:` blocks it, and spools
fall back to IndexedDB. To keep OPFS, allow it:

```
Content-Security-Policy: worker-src 'self' blob:
```

### Limits

- **`minFreeSpace`** (default `'10%'`) stops a spool from growing storage once
  the origin's free storage, its quota minus its usage from
  `navigator.storage.estimate()`, would drop below the floor. A percentage is
  of the quota, not the disk. Estimates are shared by every spool on the page
  and refreshed at most once a second, with each spool's growth counted in
  between, so parallel spools can't all spend the same space. Space a spool
  frees counts only once the browser has reclaimed it: an emptied OPFS file
  once it's truncated, deleted IndexedDB records (which browsers reclaim
  lazily) at the next estimate. If the quota can't be read, there's no floor.
- **`maxDiskBytes`** caps how much one spool keeps in storage.
- **The browser's quota** is always a hard limit: a write the browser refuses
  (`QuotaExceededError`) is kept and treated like any other limit (pausing new
  growth for a second even when `minFreeSpace: false`, or failing with
  `SpoolLimitError` under `onLimit: 'error'`).

At a limit, `onLimit` applies as in Node. With `'backpressure'` the source is
paused until the consumer frees space, and no data is dropped.

Memory stays bounded however far the consumer falls behind: up to 1 MiB of
writes waiting for storage (up to 2 MiB if a refused write is held back), one
1 MiB write in flight, one read (`chunkSize` for OPFS, up to 1 MiB for
IndexedDB), `memoryThreshold`, one chunk in the readable side's queue, and in
object mode the largest single record when one is bigger than `chunkSize`.

### Object mode in the browser

With `objectMode: true` a spool carries any values (including `null`, unlike
Node). Records that pass straight through or are held under `memoryThreshold`
stay the same objects; only records that are spilled are encoded, with `codec`
(default `jsonCodec`, JSON over UTF-8, which turns `Date`s into strings, drops
nested `undefined`/function properties and class identity, and fails with
`SpoolCodecError` on top-level `undefined`, functions or symbols).

Records are encoded with the codec in IndexedDB too, rather than stored by
structured clone, so a stream behaves the same whichever backend `'auto'`
picks, and so `maxDiskBytes` and the quota floor can count bytes. Pass a codec
if your records need more than JSON.

A codec that throws, or whose `encode()` doesn't return a `Uint8Array`, fails
the stream with a `SpoolCodecError` (`code: 'ERR_SYKE_SPOOL_CODEC'`, the
original error as `cause`), as in Node.

### Chunk ownership

Chunks that are spilled are copied into the spool's own buffers before the
write resolves, and those copies are what get transferred to the worker. A
chunk you write is never detached, so it's safe to share chunks with other
streams (as `fork` does). Chunks that pass straight through or are held in
memory are passed on as they are, as with any Web Stream, so don't modify a
chunk after writing it.

### Cleanup in the browser

A spool deletes its OPFS file or IndexedDB records when the stream ends, errors
or is cancelled, and when it's garbage-collected without ending.

A tab can also close (or crash) mid-stream. Each spool holds a
[Web Lock](https://developer.mozilla.org/docs/Web/API/Web_Locks_API) named
`syke-spool:<id>` while it has storage, which the browser releases when the page
goes away. The first time a page spills, it sweeps storage in the background:
it deletes the `syke-spool-*` OPFS files and `syke-spool` records of every
spool whose lock is free. A live spool, in this tab or another, always holds its
lock, so it's never swept. Without Web Locks (an insecure context), nothing is
swept.

### Browser support

Tested with `pnpm test:browser` in headless Edge 157, Chrome 157 and Firefox 160
on Windows 11: bytes and records round-trip intact on both backends, `'auto'`
picks OPFS and falls back to IndexedDB under a CSP that blocks `blob:` workers,
storage is deleted after the stream ends, errors or is cancelled, the sweep
removes a closed tab's spool and leaves live ones alone, and the main thread
never stalls for more than ~10 ms while 64 MiB is spilled and drained.

Safari and mobile browsers are untested. Safari has had sync access handles in
workers since 15.2 and Web Locks since 15.4, so it should take the OPFS path,
but that hasn't been verified.

## Prior art

[`fs-capacitor`](https://github.com/mike-marcacci/fs-capacitor) also buffers to a
temporary file, and supports multiple readers of the same buffered data. Use it if
you need to replay the stream. `Spool` is a single pass-through: it skips the disk
while the consumer keeps up, reuses space as data is read, and can cap disk use,
but it can't replay data that's already been read.
