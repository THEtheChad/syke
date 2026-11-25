import type { ByteChunk } from '../shared/bytes.ts'
import { CoreTransformStream } from '../shared/web.ts'
import { LinesCore, type LinesOptions } from './core.ts'

/**
 * Decodes bytes (or takes strings, such as a `TextDecoderStream`'s output)
 * and emits one string per line, without its `\n` or `\r\n`. Bytes can be
 * any `ByteChunk`: an `ArrayBuffer`, `SharedArrayBuffer` or any view of one.
 * A line longer than `maxLineLength` fails the stream with a
 * `LineTooLongError`.
 */
export class Lines extends CoreTransformStream<ByteChunk | string, string> {
  constructor(options: LinesOptions = {}) {
    super(new LinesCore(options))
  }
}
