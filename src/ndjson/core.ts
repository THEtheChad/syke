import { LineSplitter } from '../lines/core.ts'
import { SykeError } from '../shared/errors.ts'
import type { Sink, Transformer } from '../shared/transformer.ts'

export type JsonReviver = (this: any, key: string, value: any) => any
export type JsonReplacer = (this: any, key: string, value: any) => any

export interface NdjsonParseOptions {
  /**
   * Longest line allowed, in characters, before the stream fails with a
   * `LineTooLongError`. Defaults to 16 MiB; `Infinity` turns the guard off.
   */
  maxLineLength?: number
  /**
   * Passed to `JSON.parse`. Returning `undefined` for the whole value (key
   * `''`) drops the line, and mapping a top-level `null` to something else lets
   * `null` lines through.
   */
  reviver?: JsonReviver
}

export interface NdjsonStringifyOptions {
  /** Passed to `JSON.stringify`. */
  replacer?: JsonReplacer
}

/** A line of NDJSON input wasn't valid JSON, or was a top-level `null`. */
export class NdjsonParseError extends SykeError {
  override readonly code: 'ERR_SYKE_NDJSON' = 'ERR_SYKE_NDJSON'
  override name = 'NdjsonParseError'
  /** 1-based line number, counting blank lines. */
  readonly line: number

  constructor(message: string, line: number, cause?: unknown) {
    super(message, cause === undefined ? undefined : { cause })
    this.line = line
  }
}

/** A value couldn't be serialised as one line of JSON. */
export class NdjsonStringifyError extends SykeError {
  override readonly code: 'ERR_SYKE_NDJSON_STRINGIFY' = 'ERR_SYKE_NDJSON_STRINGIFY'
  override name = 'NdjsonStringifyError'
  /** 1-based number of the value (and so the output line) that failed. */
  readonly line: number

  constructor(message: string, line: number, cause?: unknown) {
    super(message, cause === undefined ? undefined : { cause })
    this.line = line
  }
}

/** Only JSON whitespace: a line of it is skipped rather than parsed. */
const BLANK = /^[ \t\r]*$/

export class NdjsonParseCore<T = unknown> implements Transformer<Uint8Array | string, T> {
  readonly #splitter: LineSplitter
  readonly #reviver: JsonReviver | undefined

  constructor({ maxLineLength, reviver }: NdjsonParseOptions) {
    if (reviver !== undefined && typeof reviver !== 'function') {
      throw new TypeError('syke: reviver must be a function')
    }
    this.#splitter = new LineSplitter({ maxLineLength })
    this.#reviver = reviver
  }

  transform(chunk: Uint8Array | string, sink: Sink<T>): void {
    this.#splitter.write(chunk, (line, number) => this.#parse(line, number, sink))
  }

  flush(sink: Sink<T>): void {
    this.#splitter.end((line, number) => this.#parse(line, number, sink))
  }

  #parse(line: string, number: number, sink: Sink<T>) {
    if (BLANK.test(line)) return
    let value: unknown
    try {
      value = JSON.parse(line, this.#reviver)
    } catch (error) {
      throw new NdjsonParseError(`syke: invalid JSON on line ${number}: ${(error as Error).message}`, number, error)
    }
    // A Node object-mode stream ends when it's handed null, so neither runtime emits one.
    if (value === null) {
      throw new NdjsonParseError(
        `syke: line ${number} is null, which a stream can't carry; map it with a reviver to let it through`,
        number,
      )
    }
    if (value !== undefined) sink.push(value as T)
  }
}

export class NdjsonStringifyCore implements Transformer<unknown, Uint8Array> {
  readonly #encoder = new TextEncoder()
  readonly #replacer: JsonReplacer | undefined
  #count = 0

  constructor({ replacer }: NdjsonStringifyOptions) {
    if (replacer !== undefined && typeof replacer !== 'function') {
      throw new TypeError('syke: replacer must be a function')
    }
    this.#replacer = replacer
  }

  transform(value: unknown, sink: Sink<Uint8Array>): void {
    const number = ++this.#count
    let json: string | undefined
    try {
      json = JSON.stringify(value, this.#replacer)
    } catch (error) {
      throw new NdjsonStringifyError(
        `syke: value ${number} can't be serialised as JSON: ${(error as Error).message}`,
        number,
        error,
      )
    }
    // undefined, a function or a symbol (or a replacer that returned undefined) has no JSON form.
    if (json === undefined) {
      throw new NdjsonStringifyError(`syke: value ${number} has no JSON form (${describe(value)})`, number)
    }
    sink.push(this.#encoder.encode(`${json}\n`))
  }
}

function describe(value: unknown): string {
  return value === undefined ? 'undefined' : typeof value
}
