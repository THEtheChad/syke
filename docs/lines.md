# Lines

`syke/node` · `syke/web`

Decodes a byte stream and emits one string per line. Use it to read logs,
CSV-ish exports or any line-oriented text without loading the whole file, and
as the first stage before parsing each record.

```js
import { createReadStream } from 'node:fs'
import { pipeline } from 'node:stream/promises'
import { Lines } from '@thethechad/syke/node'

await pipeline(createReadStream('access.log'), new Lines(), async (lines) => {
  for await (const line of lines) if (line.includes(' 500 ')) console.log(line)
})
```

```js
import { Lines } from '@thethechad/syke/web'

const res = await fetch('/export.txt')
for await (const line of res.body.pipeThrough(new Lines())) render(line)
```

## Behaviour

- **Splitting:** on `\n`; a `\r` before it is stripped, so `\r\n` works too.
  Lines are emitted without their ending.
- **Last line:** a final line without a newline is emitted. A final newline
  doesn't produce an empty line after it. Empty lines in the middle are
  emitted as `''`.
- **Decoding:** a streaming `TextDecoder`, so a multi-byte character split
  across chunks decodes correctly. A UTF-8 (or UTF-16) byte order mark is
  dropped. Invalid bytes become `U+FFFD`, as `TextDecoder` does.
- **Strings (`syke/node` only):** a string chunk is taken as text that's
  already decoded. The output side is in object mode, one string per chunk.
- **Long lines** are joined once from their pieces, so a line spanning many
  chunks costs linear time.

## Options

| Option | Default | Description |
| ------ | ------- | ----------- |
| `encoding` | `'utf-8'` | Any encoding `TextDecoder` supports (`'latin1'`, `'utf-16le'`, ...). |
| `maxLineLength` | `16 * 1024 * 1024` | Longest line allowed, in characters (UTF-16 code units, `string.length`), not counting its ending. `Infinity` turns the check off. |

`syke/node` also takes `highWaterMark` and `signal`, passed to
`stream.Transform`. `highWaterMark` applies to both sides: bytes on the input,
lines on the output.

### Why a default `maxLineLength`

Lines buffers the current line until its newline arrives. Feed it a file with
no newlines (a binary file, a minified JSON blob, the wrong URL) and that
buffer grows until the process runs out of memory. 16 Mi characters is far
longer than any reasonable record yet stops a runaway input at a few tens of
megabytes. Raise it, or pass `Infinity`, if your lines really are that long.

## Errors

A line longer than `maxLineLength` fails the stream with a `LineTooLongError`
(`code: 'ERR_SYKE_LINE_TOO_LONG'`) as soon as the buffered part of it passes
the limit, without waiting for its newline. `error.line` is its 1-based line
number; `error.maxLineLength` is the limit.

An unsupported `encoding` or an invalid `maxLineLength` throws a `RangeError`
from the constructor.
