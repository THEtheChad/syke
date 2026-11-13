import { CoreTransformStream } from '../shared/web.ts'
import {
  NdjsonParseCore,
  type NdjsonParseOptions,
  NdjsonStringifyCore,
  type NdjsonStringifyOptions,
} from './core.ts'

/**
 * Parses newline-delimited JSON: takes UTF-8 bytes and emits one value per
 * line, skipping blank lines. Invalid JSON, or a top-level `null`, fails the
 * stream with an `NdjsonParseError`.
 */
export class NdjsonParse<T = unknown> extends CoreTransformStream<Uint8Array, T> {
  constructor(options: NdjsonParseOptions = {}) {
    super(new NdjsonParseCore<T>(options))
  }
}

/**
 * Serialises values as newline-delimited JSON: `JSON.stringify(value) + '\n'`
 * per value, as UTF-8 bytes. A value with no JSON form fails the stream with
 * an `NdjsonStringifyError`.
 */
export class NdjsonStringify<T = unknown> extends CoreTransformStream<T, Uint8Array> {
  constructor(options: NdjsonStringifyOptions = {}) {
    super(new NdjsonStringifyCore(options))
  }
}
