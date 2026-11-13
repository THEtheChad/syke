# Digest

`syke/node` · `syke/web`

Passes bytes through unchanged while hashing them, so you can checksum a
download or upload without a second pass. With `expected`, it also verifies
the result and fails the stream on a mismatch.

```js
import { pipeline } from 'node:stream/promises'
import { Digest } from 'syke/node'

const digest = new Digest({ algorithm: 'sha256' })
await pipeline(source, digest, destination)
console.log(await digest.digest) // 'e3b0c442...'
```

```js
// Verify while downloading: fails before the destination sees the end.
await pipeline(res.body, new Digest({ algorithm: 'sha256', expected: published }), file)
```

## Algorithms

- **`syke/node`:** `'crc32'` or any algorithm `crypto.createHash()` supports
  (`'sha256'`, `'sha512'`, `'md5'`, ...).
- **`syke/web`:** `'crc32'` built in. Browsers can't hash a stream
  incrementally (WebCrypto only digests a whole buffer), so for anything else
  pass a `hasher`. syke stays dependency-free; add the hashing library you
  want:

```js
import { Digest } from 'syke/web'
import { sha256 } from '@noble/hashes/sha2.js'   // audited, pure JS
import { createSHA256 } from 'hash-wasm'           // WebAssembly, faster

new Digest({ hasher: sha256.create() })
new Digest({ hasher: createSHA256() })            // a promise is fine
```

A `hasher` is any object with `update(bytes)` and `digest()`, where `digest()`
returns bytes or a hex string. Node's `crypto.createHash()` result fits too.

## Options

| Option | Default | Description |
| ------ | ------- | ----------- |
| `algorithm` | | Algorithm name (see above). Give this or `hasher`. |
| `hasher` | | An incremental hash, or a promise of one. Give this or `algorithm`. |
| `expected` | | The digest the stream must have: a string in `encoding`, or raw bytes. |
| `encoding` | `'hex'` | `'hex'` or `'base64'`: how `digest` is encoded and a string `expected` is read. Hex comparison ignores case. |

`syke/node` also takes `highWaterMark` and `signal`, passed to `stream.Transform`.

## Result and errors

`digest.digest` is a promise of the encoded digest. It resolves when the stream
ends, and rejects if the stream fails or is destroyed first. You don't have to
await it: an unawaited rejection won't crash the process.

On a mismatch the stream fails with a `DigestError`
(`code: 'ERR_SYKE_DIGEST'`, with `expected` and `actual`) in place of its end,
so a consumer that waits for the end before committing never commits
corrupted data. Bytes are passed on as they arrive, so the consumer will
already have seen them; pair `Digest` with a temporary destination you
discard on error.
