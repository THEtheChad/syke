import { CoreTransformStream } from '../shared/web.ts'
import { LinesCore, type LinesOptions } from './core.ts'

/**
 * Decodes bytes (or takes strings, such as a `TextDecoderStream`'s output)
 * and emits one string per line, without its `\n` or `\r\n`.
 * A line longer than `maxLineLength` fails the stream with a
 * `LineTooLongError`.
 */
export class Lines extends CoreTransformStream<Uint8Array | string, string> {
  constructor(options: LinesOptions = {}) {
    super(new LinesCore(options))
  }
}
