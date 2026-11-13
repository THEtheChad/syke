/** A run of unread bytes in a spool file: [start, end). */
export interface Extent {
  start: number
  end: number
}

/** A region of the spool file to write to or read from. */
export interface Region {
  position: number
  length: number
}

/**
 * Tracks the unread bytes in a random-access spool file (a temp file in Node,
 * an OPFS file in the browser) as a short list of extents in stream order,
 * and decides where new bytes go so space the reader has finished with is
 * reused before the file grows.
 *
 * Reads take from the first extent. Writes extend the last extent, or start a
 * new one in a free gap, or grow the file. Runtime-neutral: it does no I/O.
 *
 * Expects one write and one read in flight at most: callers `place()` then
 * `wrote()` for a write, and `nextRead()` then `read()` for a read.
 */
export class ExtentMap {
  /** High-water size of the file, in bytes. */
  fileSize = 0
  readonly #extents: Extent[] = []
  readonly #chunkSize: number
  readonly #maxBytes: number

  /**
   * `chunkSize` is the largest read, and the smallest gap worth writing into
   * (to avoid fragmenting the file). `maxBytes` caps the file's size.
   */
  constructor(chunkSize: number, maxBytes: number = Number.POSITIVE_INFINITY) {
    this.#chunkSize = chunkSize
    this.#maxBytes = maxBytes
  }

  /** No unread bytes. */
  get empty(): boolean {
    return this.#extents.length === 0
  }

  /** Unread bytes. */
  get unread(): number {
    let total = 0
    for (const extent of this.#extents) total += extent.end - extent.start
    return total
  }

  /** Whether the file is already as large as `maxBytes` allows. */
  get atMaxSize(): boolean {
    return this.fileSize >= this.#maxBytes
  }

  /** Whether growing by `growth` bytes would reach `maxBytes`. */
  wouldReachMax(growth: number): boolean {
    return this.fileSize + Math.max(0, growth) >= this.#maxBytes
  }

  /**
   * Finds space for up to `want` bytes: right after the last extent if
   * possible, otherwise the first free gap. The file may grow by `growth`
   * bytes (capped by `maxBytes`). Unless `anySize`, gaps smaller than
   * `chunkSize` are skipped. Returns `null` if nothing fits.
   */
  place(want: number, growth: number, anySize: boolean): Region | null {
    const limit = Math.min(this.#maxBytes, this.fileSize + Math.max(0, growth))
    const tail = this.#extents.at(-1)
    const minGap = anySize ? 1 : Math.min(want, this.#chunkSize)

    let first: Region | null = null
    for (const gap of this.#gaps(limit)) {
      const length = Math.min(want, gap.end - gap.start)
      if (tail && gap.start === tail.end) return { position: gap.start, length }
      if (!first && gap.end - gap.start >= minGap) first = { position: gap.start, length }
    }
    return first
  }

  /** Records that `length` bytes were written at `position` (a region from `place()`). */
  wrote(position: number, length: number): void {
    const end = position + length
    const tail = this.#extents.at(-1)
    // The tail may have been fully read (and dropped) while the write was in flight.
    if (tail && tail.end === position) tail.end = end
    else this.#extents.push({ start: position, end })
    this.fileSize = Math.max(this.fileSize, end)
  }

  /** The next region to read, at most `chunkSize` bytes, or `null` if nothing is unread. */
  nextRead(): Region | null {
    const head = this.#extents[0]
    if (!head) return null
    return { position: head.start, length: Math.min(this.#chunkSize, head.end - head.start) }
  }

  /** Marks `length` bytes at the start of the oldest extent as read. */
  read(length: number): void {
    const head = this.#extents[0]
    if (!head || length > head.end - head.start) {
      throw new RangeError(`syke: read ${length} bytes past the unread data`)
    }
    head.start += length
    if (head.start === head.end) this.#extents.shift()
  }

  /** Free space in [0, limit), in file order. */
  #gaps(limit: number) {
    const gaps: Extent[] = []
    let cursor = 0
    for (const extent of [...this.#extents].sort((a, b) => a.start - b.start)) {
      if (extent.start > cursor) gaps.push({ start: cursor, end: Math.min(extent.start, limit) })
      cursor = Math.max(cursor, extent.end)
    }
    if (limit > cursor) gaps.push({ start: cursor, end: limit })
    return gaps.filter((gap) => gap.end > gap.start)
  }
}
