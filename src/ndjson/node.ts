import { CoreTransform, type NodeStreamOptions, streamOptions } from '../shared/node.ts'
import {
  NdjsonParseCore,
  type NdjsonParseOptions,
  NdjsonStringifyCore,
  type NdjsonStringifyOptions,
} from './core.ts'

/**
 * Parses newline-delimited JSON: takes UTF-8 bytes (or strings) and emits one
 * value per line, skipping blank lines. Invalid JSON, or a top-level `null`,
 * fails the stream with an `NdjsonParseError`.
 */
export class NdjsonParse<T = unknown> extends CoreTransform<Uint8Array | string, T> {
  constructor(options: NdjsonParseOptions & NodeStreamOptions = {}) {
    const { maxLineLength, reviver } = options
    super(new NdjsonParseCore<T>({ maxLineLength, reviver }), {
      ...streamOptions(options),
      readableObjectMode: true,
      decodeStrings: false,
    })
  }
}

/**
 * Serialises values as newline-delimited JSON: `JSON.stringify(value) + '\n'`
 * per value, as UTF-8 bytes. A value with no JSON form fails the stream with
 * an `NdjsonStringifyError`.
 */
export class NdjsonStringify<T = unknown> extends CoreTransform<T, Uint8Array> {
  constructor(options: NdjsonStringifyOptions & NodeStreamOptions = {}) {
    const { replacer } = options
    super(new NdjsonStringifyCore({ replacer }), { ...streamOptions(options), writableObjectMode: true })
  }
}
