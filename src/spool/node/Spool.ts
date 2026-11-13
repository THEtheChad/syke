import { randomUUID } from 'node:crypto'
import { type FileHandle, open, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Duplex, type DuplexOptions, pipeline, Readable } from 'node:stream'
import type { ReadableStream } from 'node:stream/web'
import {
  type FreeSpaceFloor,
  type MinFreeSpace,
  freeSpaceBudget,
  parseMinFreeSpace,
  warnIfMemoryBacked,
} from './diskSpace.ts'
import { nonNegativeInt, positiveInt } from '../../shared/validate.ts'
import { ExtentMap, type Region } from '../core/extents.ts'
import { SpoolLimitError } from '../core/limits.ts'
import { releaseFile, trackFile } from './fileRegistry.ts'

export interface SpoolOptions
  extends Pick<
    DuplexOptions,
    'highWaterMark' | 'readableHighWaterMark' | 'writableHighWaterMark' | 'signal'
  > {
  /** Directory for the temporary spool file. Defaults to `os.tmpdir()`. */
  dir?: string
  /**
   * Largest chunk, in bytes, read back from disk and pushed downstream.
   * Defaults to the readable high-water mark.
   */
  chunkSize?: number
  /**
   * Bytes to hold in memory before spilling to disk. Defaults to `0`, so
   * everything the consumer is not ready for goes straight to disk.
   */
  memoryThreshold?: number
  /**
   * Stop growing the spool file before free space on `dir`'s filesystem drops
   * below this: a percentage of the filesystem (`'10%'`), a number of bytes,
   * or `false` for no floor. Defaults to `'10%'`.
   */
  minFreeSpace?: MinFreeSpace
  /** Largest size, in bytes, the spool file may reach. Defaults to no cap. */
  maxDiskBytes?: number
  /**
   * What to do when a disk limit is reached. `'backpressure'` (the default)
   * pauses the source until the consumer frees space; `'error'` fails the
   * stream with a `SpoolLimitError`.
   */
  onLimit?: 'backpressure' | 'error'
}

/** Anything `Spool.from()` can read from. */
export type SpoolSource =
  | NodeJS.ReadableStream
  | ReadableStream
  | Iterable<Buffer | Uint8Array | string>
  | AsyncIterable<Buffer | Uint8Array | string>

export { SpoolLimitError }

/** How often to recheck free space while waiting on `minFreeSpace`. */
const FREE_SPACE_POLL_MS = 250

/**
 * A pass-through stream that doesn't apply backpressure to its source.
 * Whatever the consumer is not ready for is spooled to a temporary file and
 * read back, `chunkSize` bytes at a time, as the consumer drains. Disk use can
 * be capped with `maxDiskBytes` and `minFreeSpace`.
 */
export class Spool extends Duplex {
  /** Path of the spool file. `null` until the stream first spills to disk. */
  filepath: string | null = null

  readonly #dir: string
  readonly #chunkSize: number
  readonly #memoryThreshold: number
  readonly #maxDiskBytes: number
  readonly #floor: FreeSpaceFloor
  readonly #onLimit: 'backpressure' | 'error'

  #file: Promise<FileHandle> | null = null
  // Unread bytes on disk, in stream order, and where new bytes go.
  readonly #extents: ExtentMap
  #reading = false
  #writing = false
  #wakeWriter: (() => void) | null = null
  #pending = new Set<Promise<unknown>>()

  #memory: Buffer[] = []
  #memoryBytes = 0

  #wantsData = false
  #writeEnded = false
  #readEnded = false

  /**
   * Reads `source` through a new Spool, so the source is never held back by
   * a slow consumer: `Spool.from(res).pipe(slowWriter)`. Accepts a Node
   * readable, a web `ReadableStream`, or an iterable or async iterable of
   * chunks. Errors and destruction propagate both ways, as with `pipeline`.
   *
   * Overrides the inherited `Duplex.from()`, which would return a plain
   * `Duplex` with no disk spilling.
   */
  static override from(source: SpoolSource, options?: SpoolOptions): Spool {
    const spool = new Spool(options)
    // Errors surface on `spool` itself; the callback only stops pipeline from
    // reporting them a second time.
    pipeline(toNodeReadable(source), spool, () => {})
    return spool
  }

  constructor({
    dir,
    chunkSize,
    memoryThreshold,
    minFreeSpace,
    maxDiskBytes,
    onLimit,
    ...options
  }: SpoolOptions = {}) {
    super({ ...options, objectMode: false, decodeStrings: true })
    this.#dir = dir ?? tmpdir()
    this.#chunkSize = positiveInt('chunkSize', chunkSize ?? this.readableHighWaterMark)
    this.#memoryThreshold = nonNegativeInt('memoryThreshold', memoryThreshold ?? 0)
    this.#maxDiskBytes =
      maxDiskBytes === undefined
        ? Number.POSITIVE_INFINITY
        : positiveInt('maxDiskBytes', maxDiskBytes)
    this.#floor = parseMinFreeSpace(minFreeSpace ?? '10%')
    if (onLimit !== undefined && onLimit !== 'backpressure' && onLimit !== 'error') {
      throw new RangeError(
        `syke: onLimit must be 'backpressure' or 'error', got ${String(onLimit)}`,
      )
    }
    this.#onLimit = onLimit ?? 'backpressure'
    this.#extents = new ExtentMap(this.#chunkSize, this.#maxDiskBytes)
  }

  override _write(
    chunk: Buffer,
    _encoding: BufferEncoding,
    done: (error?: Error | null) => void,
  ): void {
    if (this.#passThrough(chunk)) return done()

    // Memory only takes data while the disk is empty, so memory is always older.
    if (this.#diskEmpty() && this.#memoryBytes + chunk.length <= this.#memoryThreshold) {
      this.#memory.push(chunk)
      this.#memoryBytes += chunk.length
      done()
      this.#flush()
      return
    }

    this.#track(this.#spill(chunk)).then(() => {
      done()
      this.#flush()
    }, done)
  }

  override _final(done: (error?: Error | null) => void): void {
    this.#writeEnded = true
    this.#flush()
    done()
  }

  override _read(): void {
    this.#wantsData = true
    this.#flush()
  }

  override _destroy(error: Error | null, done: (error?: Error | null) => void): void {
    this.#memory = []
    this.#memoryBytes = 0
    this.#wakeWriter?.()
    this.#cleanup().then(
      () => done(error),
      (cleanupError: Error) => done(error ?? cleanupError),
    )
  }

  /** Hands `chunk` straight to a waiting consumer if nothing is queued ahead of it. */
  #passThrough(chunk: Buffer) {
    if (!this.#wantsData || !this.#diskEmpty() || this.#memory.length > 0) return false
    this.#wantsData = this.push(chunk)
    return true
  }

  /** No unread bytes on disk and no write in flight. */
  #diskEmpty() {
    return this.#extents.empty && !this.#writing
  }

  async #spill(chunk: Buffer) {
    let rest = chunk
    while (rest.length > 0) {
      if (this.destroyed) return

      // Prefer space inside the file; only ask the filesystem when it must grow.
      let budget = 0
      let target = this.#extents.place(rest.length, 0, false)
      if (!target && !this.#extents.atMaxSize) {
        budget = this.#floor ? await freeSpaceBudget(this.#dir, this.#floor) : Number.POSITIVE_INFINITY
        // State may have changed during the await, so place again from scratch.
        // Everything from here to `#writing = true` is synchronous.
        target = this.#extents.place(rest.length, budget, false)
      }
      // At a limit, any free gap will do, however small.
      target ??= this.#extents.place(rest.length, budget, true)

      if (!target) {
        // At a limit with nothing queued and the consumer waiting: hand it over.
        if (this.#passThrough(rest)) return
        if (this.#onLimit === 'error') throw new SpoolLimitError(this.#limitMessage(budget))
        await this.#waitForSpace()
        continue
      }

      const piece = rest.subarray(0, target.length)
      this.#writing = true
      try {
        const file = await this.#openFile()
        let offset = 0
        while (offset < piece.length) {
          const { bytesWritten } = await file.write(
            piece,
            offset,
            piece.length - offset,
            target.position + offset,
          )
          offset += bytesWritten
        }
      } finally {
        this.#writing = false
      }

      this.#extents.wrote(target.position, piece.length)
      rest = rest.subarray(piece.length)
      this.#flush()
    }
  }

  #limitMessage(budget: number) {
    if (this.#extents.wouldReachMax(budget)) {
      return `syke: spool file reached maxDiskBytes (${this.#maxDiskBytes} bytes)`
    }
    return `syke: spilling more would take free space on ${this.#dir} below minFreeSpace`
  }

  /** Resolves when the reader frees space, the consumer asks for data, or free space may have changed. */
  #waitForSpace() {
    return new Promise<void>((resolve) => {
      const wake = () => {
        clearTimeout(timer)
        this.#wakeWriter = null
        resolve()
      }
      const timer = setTimeout(wake, FREE_SPACE_POLL_MS)
      this.#wakeWriter = wake
    })
  }

  #openFile() {
    if (!this.#file) {
      const filepath = join(this.#dir, `syke-spool-${randomUUID()}`)
      this.filepath = filepath
      // `wx+` refuses to reuse an existing file; 0o600 keeps it private.
      this.#file = warnIfMemoryBacked(this.#dir)
        .then(() => open(filepath, 'wx+', 0o600))
        .then((file) => {
          trackFile(this, filepath, file)
          return file
        })
    }
    return this.#file
  }

  #flush() {
    if (this.#readEnded || this.destroyed) return
    this.#wakeWriter?.()
    if (this.#reading) return

    while (this.#wantsData && this.#memory.length > 0) {
      const chunk = this.#memory.shift() as Buffer
      this.#memoryBytes -= chunk.length
      this.#wantsData = this.push(chunk)
    }

    if (this.#memory.length > 0) return

    if (!this.#extents.empty) {
      if (this.#wantsData) this.#readFromDisk()
      return
    }

    if (this.#writeEnded && this.#diskEmpty()) {
      this.#readEnded = true
      this.push(null)
    }
  }

  #readFromDisk() {
    this.#reading = true
    const { position, length } = this.#extents.nextRead() as Region
    const read = this.#track(
      this.#openFile().then((file) => file.read(Buffer.allocUnsafe(length), 0, length, position)),
    )

    read.then(
      ({ bytesRead, buffer }) => {
        this.#reading = false
        if (this.destroyed) return
        if (bytesRead === 0) {
          this.destroy(new Error(`syke: spool file ended early (${this.filepath})`))
          return
        }

        this.#extents.read(bytesRead)
        this.#wantsData = this.push(bytesRead < length ? buffer.subarray(0, bytesRead) : buffer)
        this.#flush()
      },
      (error: Error) => {
        this.#reading = false
        this.destroy(error)
      },
    )
  }

  #track<T>(promise: Promise<T>) {
    this.#pending.add(promise)
    const untrack = () => this.#pending.delete(promise)
    promise.then(untrack, untrack)
    return promise
  }

  async #cleanup() {
    await Promise.allSettled(this.#pending)
    if (!this.#file) return

    const filepath = this.filepath as string
    const file = this.#file
    this.#file = null
    releaseFile(this, filepath)

    const opened = await Promise.allSettled([file])
    if (opened[0].status === 'fulfilled') await opened[0].value.close()
    await rm(filepath, { force: true })
  }
}

function toNodeReadable(source: SpoolSource): NodeJS.ReadableStream {
  if (typeof (source as NodeJS.ReadableStream).pipe === 'function') {
    return source as NodeJS.ReadableStream
  }
  if (typeof (source as ReadableStream).getReader === 'function') {
    return Readable.fromWeb(source as ReadableStream)
  }
  return Readable.from(source as Iterable<unknown> | AsyncIterable<unknown>, { objectMode: false })
}
