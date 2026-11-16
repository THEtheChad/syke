# NdjsonParse and NdjsonStringify

`syke/node` · `syke/web`

Read and write [newline-delimited JSON](https://github.com/ndjson/ndjson-spec)
(also called JSON Lines): one JSON value per line. It's the usual format for
database exports, log pipelines and bulk APIs because it streams: each record
can be handled as soon as its line arrives, in constant memory.

`NdjsonParse` takes UTF-8 bytes and emits parsed values. `NdjsonStringify`
takes values and emits UTF-8 bytes.

```js
import { createReadStream, createWriteStream } from 'node:fs'
import { pipeline } from 'node:stream/promises'
import { NdjsonParse, NdjsonStringify } from '@thethechad/syke/node'

// Export a table.
await pipeline(db.query('SELECT * FROM users').stream(), new NdjsonStringify(), createWriteStream('users.ndjson'))

// Import it again.
await pipeline(createReadStream('users.ndjson'), new NdjsonParse(), async (users) => {
  for await (const user of users) await upsert(user)
})
```

```js
import { NdjsonParse } from '@thethechad/syke/web'

const res = await fetch('/api/events?format=ndjson')
for await (const event of res.body.pipeThrough(new NdjsonParse())) render(event)
```

## NdjsonParse

Built on [`Lines`](lines.md): lines split on `\n` with a trailing `\r`
stripped, a final line without a newline is parsed, and a multi-byte character
split across chunks decodes correctly. Blank lines (empty, or only spaces,
tabs and `\r`) are skipped. In `syke/node` it also accepts strings, and the
output side is in object mode.

| Option | Default | Description |
| ------ | ------- | ----------- |
| `maxLineLength` | `16 * 1024 * 1024` | Longest line allowed, in characters. Guards memory against an input with no newlines. `Infinity` turns the check off. See [`Lines`](lines.md#why-a-default-maxlinelength). |
| `reviver` | none | Passed to `JSON.parse`. Returning `undefined` for the whole value (key `''`) drops that line. |

### `null` lines

A Node object-mode stream treats `null` as the end of the stream, so it can't
carry a JSON `null`. Rather than end early or silently drop a record,
`NdjsonParse` fails with an `NdjsonParseError` on a line that parses to `null`.
It does so in `syke/web` too, so the same data behaves the same in both
runtimes.

A `null` line in a record stream is almost always a bug upstream. If yours are
deliberate, map them with a reviver:

```js
const NULL = Symbol('null')
new NdjsonParse({ reviver: (key, value) => (key === '' && value === null ? NULL : value) })
// or drop them:
new NdjsonParse({ reviver: (key, value) => (key === '' && value === null ? undefined : value) })
```

`null` nested inside an object or array is untouched.

## NdjsonStringify

Writes `JSON.stringify(value) + '\n'` for each value, as one UTF-8 chunk. In
`syke/node` the input side is in object mode (so it can't be handed `null`;
`syke/web` writes it as `null\n`).

| Option | Default | Description |
| ------ | ------- | ----------- |
| `replacer` | none | A function passed to `JSON.stringify`. Use it to drop secret fields or turn `BigInt`s into strings. |

`syke/node` also takes `highWaterMark` and `signal` for both classes, passed to
`stream.Transform`. `highWaterMark` applies to both sides (bytes on one, values
on the other).

## Errors

- **Invalid JSON** fails `NdjsonParse` with an `NdjsonParseError`
  (`code: 'ERR_SYKE_NDJSON'`). `error.line` is the 1-based line number,
  counting blank lines, and `error.cause` is the `SyntaxError` from
  `JSON.parse`. The line itself isn't included in the message, since it may be
  huge or hold private data. A top-level `null` fails the same way (no
  `cause`).
- **A line longer than `maxLineLength`** fails `NdjsonParse` with a
  `LineTooLongError` (`code: 'ERR_SYKE_LINE_TOO_LONG'`).
- **A value that can't be serialised** fails `NdjsonStringify` with an
  `NdjsonStringifyError` (`code: 'ERR_SYKE_NDJSON_STRINGIFY'`). `error.line`
  is the 1-based number of the value. This covers values `JSON.stringify`
  throws on (circular references, `BigInt`; `error.cause` is its `TypeError`)
  and values with no JSON form at all: `undefined`, a function or a symbol,
  or a replacer returning `undefined` for the whole value. Skipping those
  silently would lose records, so they fail instead; filter them out
  upstream.
- A `reviver` or `replacer` that isn't a function throws a `TypeError` from
  the constructor.
