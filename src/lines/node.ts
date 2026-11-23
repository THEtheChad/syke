import { CoreTransform, type NodeStreamOptions, streamOptions } from '../shared/node.ts'
import { LinesCore, type LinesOptions } from './core.ts'

/**
 * Decodes bytes (or takes strings) and emits one string per line, without
 * its `\n` or `\r\n`. The output side is in object mode, so an empty line is
 * an empty string. A line longer than `maxLineLength` fails the stream with a
 * `LineTooLongError`.
 */
export class Lines extends CoreTransform<Uint8Array | string, string> {
  constructor(options: LinesOptions & NodeStreamOptions = {}) {
    const { encoding, maxLineLength } = options
    super(new LinesCore({ encoding, maxLineLength }), {
      ...streamOptions(options),
      readableObjectMode: true,
      // Strings are already text; don't let Node re-encode them as UTF-8 bytes.
      decodeStrings: false,
    })
  }
}
