# LengthCheck

`syke/node` · `syke/web`

Passes bytes through unchanged and fails the stream if it carries more or fewer
bytes than allowed. Use it to catch truncated downloads (a connection that
closes early looks like a normal end to most code) or to refuse oversized
uploads.

```js
import { pipeline } from 'node:stream/promises'
import { LengthCheck } from '@thethechad/syke/node'

const res = await fetch(url)
await pipeline(
  res.body,
  new LengthCheck({ expected: Number(res.headers.get('content-length')) }),
  createWriteStream('download.bin'),
)
```

```js
import { LengthCheck } from '@thethechad/syke/web'

await res.body
  .pipeThrough(new LengthCheck({ max: 50 * 1024 ** 2 }))
  .pipeTo(writable)
```

## Options

At least one is required.

| Option | Description |
| ------ | ----------- |
| `expected` | Exact number of bytes the stream must carry. |
| `min` | Fewest bytes allowed. |
| `max` | Most bytes allowed. |

Each is a non-negative integer. One of 2^53 or more can't be reached, so it
works as no limit; `Infinity`, a fraction or a negative number throws a
`RangeError`.

`syke/node` also takes `highWaterMark` and `signal`, passed to `stream.Transform`.

`check.bytes` holds the number of bytes that have passed so far.

## Errors

A failed check errors the stream with a `LengthCheckError`
(`code: 'ERR_SYKE_LENGTH'`), whose `bytes` property is the count when it
failed:

- **Too long:** fails as soon as a chunk takes the total past `max` or
  `expected`. That chunk is not passed on.
- **Too short:** fails when the source ends short of `min` or `expected`,
  instead of ending the output. A consumer that waits for the end before
  committing (renaming a temp file, finishing a transaction) never commits
  truncated data.
