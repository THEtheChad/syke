import { SykeError } from '../shared/errors.ts'
import type { Sink, Transformer } from '../shared/transformer.ts'

export interface LinesOptions {
  /** Any encoding `TextDecoder` supports. Defaults to `'utf-8'`. */
  encoding?: string
  /**
   * Longest line allowed, in UTF-16 code units (`string.length`), not counting
   * the line ending. Longer lines fail the stream with a `LineTooLongError`.
   * Defaults to 16 MiB (`16 * 1024 * 1024`); `Infinity` turns the guard off.
   */
  maxLineLength?: number
}

/** Large enough for any sane record, small enough to stop a newline-free input before it exhausts memory. */
export const DEFAULT_MAX_LINE_LENGTH: number = 16 * 1024 * 1024

/** A line ran past `maxLineLength` before its newline arrived. */
export class LineTooLongError extends SykeError {
  override readonly code: 'ERR_SYKE_LINE_TOO_LONG' = 'ERR_SYKE_LINE_TOO_LONG'
  override name = 'LineTooLongError'
  /** 1-based number of the offending line. */
  readonly line: number
  readonly maxLineLength: number

  constructor(line: number, maxLineLength: number) {
    super(`syke: line ${line} is longer than the maximum of ${maxLineLength} characters`)
    this.line = line
    this.maxLineLength = maxLineLength
  }
}

/**
 * Decodes bytes and splits the text into lines, calling `onLine` for each.
 * Shared by `Lines` and `NdjsonParse`.
 */
export class LineSplitter {
  readonly #decoder: TextDecoder
  readonly #maxLineLength: number
  /** The unfinished line, kept as pieces so a line spanning many chunks is joined once. */
  #parts: string[] = []
  #partsLength = 0
  /** Lines completed so far. */
  #line = 0

  constructor({ encoding = 'utf-8', maxLineLength = DEFAULT_MAX_LINE_LENGTH }: LinesOptions) {
    if (!(maxLineLength === Number.POSITIVE_INFINITY || (Number.isInteger(maxLineLength) && maxLineLength > 0))) {
      throw new RangeError(`syke: maxLineLength must be a positive integer or Infinity, got ${maxLineLength}`)
    }
    this.#maxLineLength = maxLineLength
    try {
      this.#decoder = new TextDecoder(encoding)
    } catch {
      throw new RangeError(`syke: unsupported encoding '${encoding}'`)
    }
  }

  /** Feeds a chunk: bytes in `encoding`, or text that's already decoded. */
  write(chunk: Uint8Array | string, onLine: (line: string, number: number) => void): void {
    // A string arriving mid-character ends that character (as U+FFFD) first.
    const text = typeof chunk === 'string' ? this.#decoder.decode() + chunk : this.#decoder.decode(chunk, { stream: true })
    this.#split(text, onLine)
  }

  /** Ends the input, emitting a final line that had no newline. */
  end(onLine: (line: string, number: number) => void): void {
    this.#split(this.#decoder.decode(), onLine)
    if (this.#partsLength > 0) {
      const line = stripCR(this.#take(''))
      this.#check(line.length)
      onLine(line, ++this.#line)
    }
  }

  #split(text: string, onLine: (line: string, number: number) => void) {
    let start = 0
    let newline = text.indexOf('\n')
    while (newline !== -1) {
      const line = stripCR(this.#take(text.slice(start, newline)))
      this.#check(line.length)
      onLine(line, ++this.#line)
      start = newline + 1
      newline = text.indexOf('\n', start)
    }
    if (start < text.length) {
      const rest = start === 0 ? text : text.slice(start)
      this.#parts.push(rest)
      this.#partsLength += rest.length
      // A trailing '\r' may yet be stripped, so it doesn't count against the limit.
      this.#check(this.#partsLength - (rest.endsWith('\r') ? 1 : 0))
    }
  }

  /** Joins the unfinished line with `tail` and resets it. */
  #take(tail: string): string {
    if (this.#parts.length === 0) return tail
    this.#parts.push(tail)
    const line = this.#parts.join('')
    this.#parts = []
    this.#partsLength = 0
    return line
  }

  #check(length: number) {
    if (length > this.#maxLineLength) throw new LineTooLongError(this.#line + 1, this.#maxLineLength)
  }
}

function stripCR(line: string): string {
  return line.endsWith('\r') ? line.slice(0, -1) : line
}

export class LinesCore implements Transformer<Uint8Array | string, string> {
  readonly #splitter: LineSplitter

  constructor(options: LinesOptions) {
    this.#splitter = new LineSplitter(options)
  }

  transform(chunk: Uint8Array | string, sink: Sink<string>): void {
    this.#splitter.write(chunk, (line) => sink.push(line))
  }

  flush(sink: Sink<string>): void {
    this.#splitter.end((line) => sink.push(line))
  }
}
