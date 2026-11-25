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
  // Extents in stream order from `#head` on; entries before it are fully read.
  #extents: Extent[] = []
  #head = 0
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
    return this.#head === this.#extents.length
  }

  /** Unread bytes. */
  get unread(): number {
    let total = 0
    for (let i = this.#head; i < this.#extents.length; i++) {
      const extent = this.#extents[i] as Extent
      total += extent.end - extent.start
    }
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
   * `chunkSize` are skipped. Returns `null` if nothing fits (or `want` is not
   * positive).
   */
  place(want: number, growth: number, anySize: boolean): Region | null {
    if (!(want > 0)) return null
    const limit = Math.min(this.#maxBytes, this.fileSize + Math.max(0, growth))
    const tail = this.#tail()
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
    if (length === 0) return
    if (!(length > 0) || !(position >= 0)) {
      throw new RangeError(`syke: cannot record a write of ${length} bytes at ${position}`)
    }
    const end = position + length
    const tail = this.#tail()
    // The tail may have been fully read (and dropped) while the write was in flight.
    if (tail && tail.end === position) tail.end = end
    else this.#extents.push({ start: position, end })
    this.fileSize = Math.max(this.fileSize, end)
  }

  /** The next region to read, at most `chunkSize` bytes, or `null` if nothing is unread. */
  nextRead(): Region | null {
    const head = this.#extents[this.#head]
    if (!head) return null
    return { position: head.start, length: Math.min(this.#chunkSize, head.end - head.start) }
  }

  /** Marks `length` bytes at the start of the oldest extent as read. */
  read(length: number): void {
    if (length === 0) return
    const head = this.#extents[this.#head]
    if (!head || !(length > 0) || length > head.end - head.start) {
      throw new RangeError(`syke: read ${length} bytes past the unread data`)
    }
    head.start += length
    if (head.start !== head.end) return
    this.#head++
    if (this.#head === this.#extents.length) {
      this.#extents = []
      this.#head = 0
    } else if (this.#head >= 32 && this.#head * 2 >= this.#extents.length) {
      this.#extents = this.#extents.slice(this.#head)
      this.#head = 0
    }
  }

  /**
   * Forgets the file's size once everything has been read, after the caller
   * has truncated the file to zero bytes. Throws if anything is unread.
   */
  reset(): void {
    if (!this.empty) {
      throw new RangeError(`syke: cannot reset a spool file with ${this.unread} unread bytes`)
    }
    this.fileSize = 0
  }

  #tail(): Extent | undefined {
    return this.empty ? undefined : this.#extents[this.#extents.length - 1]
  }

  /** Free space in [0, limit), in file order. */
  #gaps(limit: number): Extent[] {
    const count = this.#extents.length - this.#head
    let sorted: Extent[]
    if (count === 0) sorted = []
    else if (count === 1) sorted = [this.#extents[this.#head] as Extent]
    else if (count === 2) {
      const a = this.#extents[this.#head] as Extent
      const b = this.#extents[this.#head + 1] as Extent
      sorted = a.start <= b.start ? [a, b] : [b, a]
    } else {
      sorted = this.#extents.slice(this.#head).sort((a, b) => a.start - b.start)
    }

    const gaps: Extent[] = []
    let cursor = 0
    for (const extent of sorted) {
      const end = Math.min(extent.start, limit)
      if (end > cursor) gaps.push({ start: cursor, end })
      cursor = Math.max(cursor, extent.end)
    }
    if (limit > cursor) gaps.push({ start: cursor, end: limit })
    return gaps
  }
}
