import { describeChunk, isByteChunk } from '../shared/bytes.ts'
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
  /** True until the first character arrives, which is dropped if it's a byte order mark. */
  #atStart = true

  constructor({ encoding = 'utf-8', maxLineLength = DEFAULT_MAX_LINE_LENGTH }: LinesOptions) {
    if (!(maxLineLength === Number.POSITIVE_INFINITY || (Number.isInteger(maxLineLength) && maxLineLength > 0))) {
      throw new RangeError(`syke: maxLineLength must be a positive integer or Infinity, got ${maxLineLength}`)
    }
    this.#maxLineLength = maxLineLength
    try {
      // The BOM is stripped by hand (in `#text`), so a string chunk carrying one
      // is treated like bytes carrying one, and only at the very start: a
      // TextDecoder would strip one again after every flush.
      this.#decoder = new TextDecoder(encoding, { ignoreBOM: true })
    } catch {
      throw new RangeError(`syke: unsupported encoding '${encoding}'`)
    }
  }

  /** Feeds a chunk: bytes in `encoding`, or text that's already decoded. */
  write(chunk: Uint8Array | string, onLine: (line: string, number: number) => void): void {
    let text: string
    // A string arriving mid-character ends that character (as U+FFFD) first.
    if (typeof chunk === 'string') text = this.#decoder.decode() + chunk
    else if (isByteChunk(chunk)) text = this.#decoder.decode(chunk, { stream: true })
    else throw new TypeError(`syke: expected Uint8Array or string chunks, got ${describeChunk(chunk)}`)
    this.#split(this.#text(text), onLine)
  }

  /** Ends the input, emitting a final line that had no newline. */
  end(onLine: (line: string, number: number) => void): void {
    this.#split(this.#text(this.#decoder.decode()), onLine)
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
      // Stripping a '\r' shortens a line by at most one: fail an overlong
      // line before joining its pieces, then check the exact length.
      this.#check(this.#partsLength + (newline - start) - 1)
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

  /** Drops a byte order mark at the start of the stream. */
  #text(text: string): string {
    if (!this.#atStart || text === '') return text
    this.#atStart = false
    return text.charCodeAt(0) === 0xfeff ? text.slice(1) : text
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
