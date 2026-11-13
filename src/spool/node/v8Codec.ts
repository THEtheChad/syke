import { deserialize, serialize } from 'node:v8'
import type { Codec } from '../core/records.ts'

/**
 * The structured-clone format, via `node:v8`: keeps Dates, Maps, Sets,
 * typed arrays, Buffers, `undefined`, BigInts and circular references.
 * Throws on functions and symbols. The default codec for an object-mode
 * `Spool` in Node.
 */
export const v8Codec: Codec = {
  encode: (value) => serialize(value),
  decode: (bytes) => deserialize(bytes),
}
