# syke

Stream utilities for memory-bound work, for Node.js streams and Web Streams.

A *syke* (rhymes with *bike*) is a small stream, in northern English and Scots
dialect.

## Install

```sh
pnpm add syke
# or: npm install syke
```

Ships ESM, CommonJS and TypeScript types, with no runtime dependencies.

## Node streams or Web Streams

syke comes in two flavours with the same utility names and options:

- **`syke/node`** builds on Node.js streams (`stream.Transform` and friends).
  Use it in Node.js 22 or later: for small chunks Node streams are several
  times faster than Web Streams.
- **`syke/web`** builds on Web Streams (`TransformStream` and friends), so it
  runs in browsers, Deno, Bun, Cloudflare Workers and Node.js. It never touches
  a Node.js API.

```js
import { LengthCheck } from 'syke/node'
await pipeline(res, new LengthCheck({ expected: size }), file)

import { LengthCheck } from 'syke/web'
await res.body.pipeThrough(new LengthCheck({ expected: size })).pipeTo(file)
```

Both share one implementation of each utility's logic, so they behave the same.

## Utilities

### Flow control

Keep a source moving, or deliberately slow it down.

| Utility | What it does | `syke/node` | `syke/web` |
| ------- | ------------ | :---------: | :--------: |
| [`Spool`](docs/spool.md) | A pass-through that spills bytes or records to disk (OPFS or IndexedDB in the browser) instead of applying backpressure to its source | ✓ | ✓ |
| [`Throttle`](docs/throttle.md) | Limits a stream to an average rate in bytes or records per second | ✓ | ✓ |
| [`Watchdog`](docs/watchdog.md) | Fails the stream if no data arrives for a while, ignoring time spent on backpressure | ✓ | ✓ |
| [`Meter`](docs/meter.md) | Counts bytes and chunks as they pass, with throttled progress callbacks | ✓ | ✓ |

### Integrity and limits

Catch truncated, oversized or corrupted streams before you commit them.

| Utility | What it does | `syke/node` | `syke/web` |
| ------- | ------------ | :---------: | :--------: |
| [`LengthCheck`](docs/length-check.md) | Fails the stream if it's longer or shorter than expected | ✓ | ✓ |
| [`Digest`](docs/digest.md) | Hashes data as it passes, optionally verifying an expected checksum | ✓ | ✓ |
| [`Cap`](docs/cap.md) | Limits a stream to its first N bytes or records, failing or truncating past that | ✓ | ✓ |

### Reshaping

Change what a chunk is: fixed-size blocks, lines, records, batches.

| Utility | What it does | `syke/node` | `syke/web` |
| ------- | ------------ | :---------: | :--------: |
| [`Rechunk`](docs/rechunk.md) | Re-cuts a byte stream into chunks of exactly N bytes | ✓ | ✓ |
| [`Lines`](docs/lines.md) | Decodes bytes and emits one string per line | ✓ | ✓ |
| [`NdjsonParse`](docs/ndjson.md) | Parses newline-delimited JSON into values | ✓ | ✓ |
| [`NdjsonStringify`](docs/ndjson.md#ndjsonstringify) | Serialises values as newline-delimited JSON | ✓ | ✓ |
| [`Batch`](docs/batch.md) | Groups records into arrays for bulk inserts, with an optional time limit | ✓ | ✓ |
| [`ParallelMap`](docs/parallel-map.md) | Maps records through an async function with bounded concurrency | ✓ | ✓ |

### Combining

Functions that split, join or inspect whole streams.

| Utility | What it does | `syke/node` | `syke/web` |
| ------- | ------------ | :---------: | :--------: |
| [`fork`](docs/fork.md) | Splits a stream into branches that each get every chunk, with bounded memory | ✓ | ✓ |
| [`peek`](docs/peek.md) | Reads the first bytes of a stream, then replays the whole stream | ✓ | ✓ |
| [`concat`](docs/concat.md) | Emits several streams one after another, opening each when it's reached | ✓ | ✓ |
| [`merge`](docs/merge.md) | Interleaves several streams as their chunks arrive | ✓ | ✓ |

## Errors

Every error syke raises on purpose extends `SykeError` and has a stable
`code`, such as `ERR_SYKE_LENGTH`, so you can branch on `error.code` without
importing the class. Each utility's page lists its codes. Invalid options
throw a plain `TypeError` or `RangeError` from the constructor. Messages start
with `syke:`.

## Migrating from stream-fs-proxy

`Spool` is the successor to the `stream-fs-proxy` package's `FSProxy`:

| stream-fs-proxy 1.x | syke |
| ------------------- | ---- |
| `require('stream-fs-proxy').default` | `const { Spool } = require('syke/node')` |
| `new FSProxy()` | `new Spool()` |

See the [changelog](CHANGELOG.md) for the bugs fixed along the way.

## License

MIT
