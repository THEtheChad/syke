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

| Utility | What it does | `syke/node` | `syke/web` |
| ------- | ------------ | :---------: | :--------: |
| [`Spool`](docs/spool.md) | A pass-through that spills to disk instead of applying backpressure to its source | ✓ | |
| [`LengthCheck`](docs/length-check.md) | Fails the stream if it's longer or shorter than expected | ✓ | ✓ |
| [`Digest`](docs/digest.md) | Hashes data as it passes, optionally verifying an expected checksum | ✓ | ✓ |
| [`fork`](docs/fork.md) | Splits a stream into branches that each get every chunk, with bounded memory | ✓ | ✓ |
| [`peek`](docs/peek.md) | Reads the first bytes of a stream, then replays the whole stream | ✓ | ✓ |

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
