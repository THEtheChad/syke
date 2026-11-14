# Changelog

## syke 0.1.0 (unreleased)

First release of `syke`: stream utilities for memory-bound work, in two
flavours with the same names and options. `syke/node` builds on Node.js streams
and `syke/web` on Web Streams (browsers, Deno, Bun and Node). `Spool` is the
successor to the `stream-fs-proxy` package's `FSProxy`.

### Changes from stream-fs-proxy 1.x

- The package is `syke` and the class is `Spool`, a named export:
  `import { Spool } from 'syke/node'` or `const { Spool } = require('syke/node')`. In
  stream-fs-proxy 1.x the CommonJS build only exposed `FSProxy` as `.default`, so
  the documented `require()` usage threw.
- Requires Node.js 22 or later.
- Object mode now works (it never did in 1.x); see `objectMode` below.

### Fixed compared with stream-fs-proxy 1.x

- Memory is bounded. 1.x read the entire on-disk backlog into one buffer when the
  consumer resumed, so a 200 MB backlog became a single 200 MB chunk in memory.
- The final chunk could be lost or fail with `stream.push() after EOF` when the
  writable side finished while the reader was waiting for data.
- The spool file is closed and deleted when the stream errors, is destroyed or
  is aborted. 1.x left it in the temp directory, and never closed the file
  descriptor even on success.
- Write errors are passed to the stream instead of corrupting the write position.
- Short reads no longer push zero-filled bytes.
- File I/O no longer blocks the event loop.
- The spool file is created with mode `0600`.

### Added: Spool

- Options: `dir`, `chunkSize`, `memoryThreshold`, and the `signal` and
  high-water-mark options from `stream.Duplex`.
- Disk limits: `minFreeSpace` (default `'10%'`, checked against the filesystem's
  live free space), `maxDiskBytes`, and `onLimit` (`'backpressure'` or `'error'`,
  which throws `SpoolLimitError`).
- A one-time process warning when the spool directory is tmpfs or ramfs.
- Chunks pass straight through without touching disk when the consumer is keeping up.
- The spool file is created only when the stream first spills, and space the
  consumer has read is reused before the file grows, so disk use tracks the
  backlog rather than the whole stream.
- The spool file is deleted if the process exits mid-stream, or once a spool
  that was dropped without being ended or destroyed is garbage-collected.
- `Spool.from(source, options)` reads a Node readable, web `ReadableStream` or
  (async) iterable through a new spool. It overrides the inherited
  `Duplex.from()`, which returned a plain `Duplex` with no disk spooling.
- Object mode: `new Spool({ objectMode: true, codec? })` spools records, so a
  database cursor isn't held back by a slow consumer. Records that pass
  through or wait in memory are never serialized; only records that spill go
  through the codec. `v8Codec` (the default, based on `v8.serialize`),
  `jsonCodec`, the `Codec` type and `SpoolCodecError`
  (`ERR_SYKE_SPOOL_CODEC`) are exported. In object mode `memoryThreshold`
  counts records; `chunkSize`, `maxDiskBytes` and `minFreeSpace` stay in bytes.
  `Spool.from(source, { objectMode: true })` accepts sources of records.

### Added: utilities for `syke/node` and `syke/web`

Integrity and limits:

- `LengthCheck`: fails a stream that's longer or shorter than `expected`,
  `min` or `max` (`LengthCheckError`, `ERR_SYKE_LENGTH`). An overrun fails
  before the offending chunk is passed on; a short stream fails in place of
  its end.
- `Digest`: hashes data as it passes and can verify an `expected` digest
  (`DigestError`, `ERR_SYKE_DIGEST`). `syke/node` supports `'crc32'` and every
  `crypto.createHash()` algorithm. `syke/web` has a built-in CRC-32 and stays
  dependency-free; stronger hashes come from a `hasher` such as
  `@noble/hashes` or `hash-wasm`.
- `Cap`: limits a stream to its first N bytes or records. Past the cap it fails
  with `CapError` (`ERR_SYKE_CAP`), or with `onLimit: 'truncate'` passes on
  what fits and ends.

Flow control:

- `Throttle`: limits a stream to an average rate in bytes or items per second,
  using a token bucket that never splits chunks.
- `Watchdog`: fails a stalled stream with `WatchdogError` (`ERR_SYKE_TIMEOUT`),
  with a separate `firstChunkTimeout`, ignoring time spent waiting on a slow
  consumer.
- `Meter`: counts bytes and chunks as they pass, with elapsed time, average
  rates and a throttled `onProgress` callback.

Reshaping:

- `Rechunk`: re-cuts a byte stream into chunks of exactly `size` bytes,
  copying only to join pieces of different input chunks.
- `Lines`: decodes bytes and emits one string per line, handling `\r\n`,
  multi-byte characters split across chunks and any `TextDecoder` encoding,
  with a `maxLineLength` guard (`LineTooLongError`, `ERR_SYKE_LINE_TOO_LONG`).
- `NdjsonParse` and `NdjsonStringify`: read and write newline-delimited JSON,
  with `reviver` and `replacer`. Failures raise `NdjsonParseError`
  (`ERR_SYKE_NDJSON`, with line number and cause) or `NdjsonStringifyError`
  (`ERR_SYKE_NDJSON_STRINGIFY`).
- `Batch`: groups records into arrays of up to `size` for bulk inserts, with an
  optional `maxWait` that emits partial batches.
- `ParallelMap`: maps records through a sync or async function with bounded
  `concurrency`, ordered or unordered. It drops `null`/`undefined` results,
  applies real backpressure, and aborts in-flight calls through an
  `AbortSignal` on error or cancellation.

Combining:

- `fork(source, { count, buffer })`: splits a stream into branches that each
  get every chunk. Unlike `ReadableStream#tee()`, memory stays bounded: the
  source advances at the slowest branch's pace, or a `buffer` factory such as
  `() => new Spool()` lets a slow branch fall behind on disk instead.
- `peek(source, bytes)`: resolves to the first bytes of a stream and a stream
  that replays them followed by the rest, for sniffing magic bytes.
- `concat(sources)`: emits sources one after another, opening sources given as
  functions only when they're reached.
- `merge(sources)`: interleaves sources as their chunks arrive, honouring
  backpressure. An error in any source errors the output and cancels the rest.

Every deliberate error extends `SykeError` and has a stable `ERR_SYKE_*` code.
ESM and CommonJS builds with TypeScript types and no runtime dependencies.

## stream-fs-proxy 1.0.5

Published to npm from code identical to 1.0.4; not tagged in git.
