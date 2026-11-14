import { randomUUID } from 'node:crypto'
import { type FileHandle, open, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Duplex, type DuplexOptions, getDefaultHighWaterMark, pipeline, Readable } from 'node:stream'
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
import { type Codec, decodeRecord, encodeFrame, encodeRecord, FrameDecoder, SpoolCodecError } from '../core/records.ts'
import { SpoolLimitError } from '../core/limits.ts'
import { releaseFile, trackFile } from './fileRegistry.ts'
import { v8Codec } from './v8Codec.ts'

export interface SpoolOptions
  extends Pick<
    DuplexOptions,
    'highWaterMark' | 'readableHighWaterMark' | 'writableHighWaterMark' | 'signal'
  > {
  /**
   * Carry records (any value but `null`) instead of bytes. Records pass
   * through and wait in memory as they are; only records that spill to disk
   * go through `codec`. Defaults to `false`.
   */
  objectMode?: boolean
  /**
   * How records are written to disk in object mode. Must round-trip every
   * record the stream carries. Defaults to `v8Codec`.
   */
  codec?: Codec
  /** Directory for the temporary spool file. Defaults to `os.tmpdir()`. */
  dir?: string
  /**
   * Largest chunk, in bytes, read back from disk and pushed downstream.
   * Defaults to the readable high-water mark (64 KiB in object mode).
   */
  chunkSize?: number
  /**
   * Bytes (records, in object mode) to hold in memory before spilling to
   * disk. Defaults to `0`, so everything the consumer is not ready for goes
   * straight to disk.
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

/**
 * Anything `Spool.from()` can read from. `T` is the chunk type of an
 * iterable: bytes by default, any record in object mode.
 */
export type SpoolSource<T = Buffer | Uint8Array | string> =
  | NodeJS.ReadableStream
  | ReadableStream
  | Iterable<T>
  | AsyncIterable<T>

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
  readonly #objectMode: boolean
  readonly #codec: Codec
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

  // Chunks (records, in object mode) waiting in memory, oldest first, and
  // their total size: bytes, or a count of records.
  #memory: unknown[] = []
  #memorySize = 0

  // Object mode: records decoded from disk but not yet pushed. Always older
  // than anything in memory or on disk.
  #decoded: unknown[] = []
  #decoder = new FrameDecoder()

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
  static override from(source: SpoolSource, options?: SpoolOptions & { objectMode?: false }): Spool
  /** Reads records from `source`: `Spool.from(cursor, { objectMode: true })`. */
  static override from(source: SpoolSource<unknown>, options: SpoolOptions & { objectMode: true }): Spool
  static override from(source: SpoolSource<unknown>, options?: SpoolOptions): Spool {
    const spool = new Spool(options)
    // Errors surface on `spool` itself; the callback only stops pipeline from
    // reporting them a second time.
    pipeline(toNodeReadable(source, spool.#objectMode), spool, () => {})
    return spool
  }

  constructor({
    objectMode,
    codec,
    dir,
    chunkSize,
    memoryThreshold,
    minFreeSpace,
    maxDiskBytes,
    onLimit,
    ...options
  }: SpoolOptions = {}) {
    if (objectMode !== undefined && typeof objectMode !== 'boolean') {
      throw new TypeError(`syke: objectMode must be a boolean, got ${String(objectMode)}`)
    }
    if (codec !== undefined) {
      if (!objectMode) throw new TypeError('syke: codec needs objectMode: true')
      if (typeof codec?.encode !== 'function' || typeof codec.decode !== 'function') {
        throw new TypeError('syke: codec must have encode() and decode() methods')
      }
    }
    super({ ...options, objectMode: objectMode ?? false, decodeStrings: true })
    this.#objectMode = objectMode ?? false
    this.#codec = codec ?? v8Codec
    this.#dir = dir ?? tmpdir()
    // In object mode the high-water mark counts records, so it can't size reads.
    this.#chunkSize = positiveInt(
      'chunkSize',
      chunkSize ?? (this.#objectMode ? getDefaultHighWaterMark(false) : this.readableHighWaterMark),
    )
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
    // Batching records that queue up while a spill is in flight lets them
    // share one encode-and-write. Byte mode keeps one write per chunk.
    if (this.#objectMode) {
      this._writev = (chunks, done) => this.#writeRecords(chunks.map(({ chunk }) => chunk), done)
    }
  }

  override _write(
    chunk: unknown,
    _encoding: BufferEncoding,
    done: (error?: Error | null) => void,
  ): void {
    if (this.#objectMode) return this.#writeRecords([chunk], done)

    const bytes = chunk as Buffer
    if (this.#passThrough(bytes)) return done()
    if (this.#toMemory(bytes, bytes.length)) {
      done()
      this.#flush()
      return
    }

    this.#track(this.#spill(bytes)).then(() => {
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
    this.#memorySize = 0
    this.#decoded = []
    this.#decoder = new FrameDecoder()
    this.#wakeWriter?.()
    this.#cleanup().then(
      () => done(error),
      (cleanupError: Error) => done(error ?? cleanupError),
    )
  }

  /** Hands `chunk` straight to a waiting consumer if nothing is queued ahead of it. */
  #passThrough(chunk: unknown) {
    if (!this.#wantsData || this.#queued()) return false
    this.#wantsData = this.push(chunk)
    return true
  }

  /** Something is waiting to be pushed: decoded records, memory or disk. */
  #queued() {
    return this.#decoded.length > 0 || this.#memory.length > 0 || !this.#diskEmpty()
  }

  /** Holds `chunk` in memory if it fits under `memoryThreshold`. */
  #toMemory(chunk: unknown, size: number) {
    // Memory only takes data while the disk is empty, so memory is always older.
    if (!this.#diskEmpty() || this.#memorySize + size > this.#memoryThreshold) return false
    this.#memory.push(chunk)
    this.#memorySize += size
    return true
  }

  /**
   * Object mode: passes records through or holds them in memory, as they
   * are, while that keeps them in order; encodes the rest to disk.
   */
  #writeRecords(records: unknown[], done: (error?: Error | null) => void) {
    let start = 0
    while (start < records.length && this.#acceptRecord(records[start])) start++
    if (start === records.length) {
      done()
      this.#flush()
      return
    }
    this.#track(this.#spillRecords(records, start)).then(() => {
      done()
      this.#flush()
    }, done)
  }

  #acceptRecord(record: unknown) {
    return this.#passThrough(record) || this.#toMemory(record, 1)
  }

  /** Encodes `records[start..]` and spills them, about `chunkSize` bytes per write. */
  async #spillRecords(records: unknown[], start: number) {
    let frames: Uint8Array[] = []
    let bytes = 0
    const write = async () => {
      const batch = frames.length === 1 ? (frames[0] as Uint8Array) : Buffer.concat(frames, bytes)
      frames = []
      bytes = 0
      await this.#spill(batch)
    }

    for (let i = start; i < records.length; i++) {
      if (this.destroyed) return
      // If the consumer caught up mid-batch, the rest can skip the codec.
      if (i > start && frames.length === 0 && this.#acceptRecord(records[i])) continue
      const frame = encodeFrame(encodeRecord(this.#codec, records[i]))
      frames.push(frame)
      bytes += frame.byteLength
      if (bytes >= this.#chunkSize) await write()
    }
    if (frames.length > 0) await write()
  }

  /** Object mode: decodes every record completed by `bytes` into the decoded queue. */
  #decodeRecords(bytes: Uint8Array) {
    for (const payload of this.#decoder.push(bytes)) {
      const record = decodeRecord(this.#codec, payload)
      // push(null) would end the stream and silently drop the rest.
      if (record === null) {
        throw new SpoolCodecError('syke: codec decoded a record to null, which a Node stream cannot carry')
      }
      this.#decoded.push(record)
    }
  }

  /**
   * At a disk limit with nothing queued and the consumer waiting, hands
   * `bytes` (the rest of a spill) straight over rather than waiting for space.
   */
  #handOver(bytes: Uint8Array) {
    if (!this.#objectMode) return this.#passThrough(bytes)
    if (!this.#wantsData || this.#queued()) return false
    // The decoder may hold the start of the frame these bytes finish.
    this.#decodeRecords(bytes)
    this.#flush()
    return true
  }

  /** No unread bytes on disk and no write in flight. */
  #diskEmpty() {
    return this.#extents.empty && !this.#writing
  }

  async #spill(chunk: Uint8Array) {
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
        if (this.#handOver(rest)) return
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

    while (this.#wantsData && this.#decoded.length > 0) {
      this.#wantsData = this.push(this.#decoded.shift())
    }
    if (this.#decoded.length > 0) return

    while (this.#wantsData && this.#memory.length > 0) {
      const chunk = this.#memory.shift()
      this.#memorySize -= this.#objectMode ? 1 : (chunk as Buffer).length
      this.#wantsData = this.push(chunk)
    }

    if (this.#memory.length > 0) return

    if (!this.#extents.empty) {
      if (this.#wantsData) this.#readFromDisk()
      return
    }

    if (this.#writeEnded && this.#diskEmpty()) {
      if (this.#decoder.pendingBytes > 0) {
        this.destroy(new Error(`syke: spool file ended mid-record (${this.filepath})`))
        return
      }
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
        const bytes = bytesRead < length ? buffer.subarray(0, bytesRead) : buffer
        if (this.#objectMode) {
          try {
            this.#decodeRecords(bytes)
          } catch (error) {
            this.destroy(error as Error)
            return
          }
        } else {
          this.#wantsData = this.push(bytes)
        }
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

function toNodeReadable(source: SpoolSource<unknown>, objectMode: boolean): NodeJS.ReadableStream {
  if (typeof (source as NodeJS.ReadableStream).pipe === 'function') {
    return source as NodeJS.ReadableStream
  }
  if (typeof (source as ReadableStream).getReader === 'function') {
    return Readable.fromWeb(source as ReadableStream, { objectMode })
  }
  return Readable.from(source as Iterable<unknown> | AsyncIterable<unknown>, { objectMode })
}
