import { randomUUID } from 'node:crypto'
import { type FileHandle, open, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Duplex, type DuplexOptions, finished, getDefaultHighWaterMark, Readable } from 'node:stream'
import type { ReadableStream } from 'node:stream/web'
import {
  type FreeSpaceFloor,
  type MinFreeSpace,
  diskFreed,
  diskFull,
  freeSpaceBudget,
  isNoSpaceError,
  parseMinFreeSpace,
  reserveGrowth,
  warnIfMemoryBacked,
} from './diskSpace.ts'
import { nonNegativeInt, oneOf, positiveInt } from '../../shared/validate.ts'
import { ExtentMap, type Region } from '../core/extents.ts'
import {
  type Codec,
  decodeRecord,
  encodeRecord,
  FRAME_HEADER_BYTES,
  FrameDecoder,
  SpoolCodecError,
  writeFrame,
} from '../core/records.ts'
import { SpoolLimitError } from '../core/limits.ts'
import { disownFile, releaseFile, removeFile, trackFile } from './fileRegistry.ts'
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

/** A chunk as `_writev` hands it over. */
interface Entry {
  chunk: unknown
}

/** A FIFO queue with O(1) `shift()`, unlike an array's. */
class Queue<T> {
  #items: T[] = []
  #head = 0

  get length(): number {
    return this.#items.length - this.#head
  }

  push(item: T): void {
    this.#items.push(item)
  }

  shift(): T {
    const item = this.#items[this.#head] as T
    this.#items[this.#head] = undefined as T
    this.#head++
    if (this.#head === this.#items.length) {
      this.#items = []
      this.#head = 0
    } else if (this.#head >= 1024 && this.#head * 2 >= this.#items.length) {
      this.#items = this.#items.slice(this.#head)
      this.#head = 0
    }
    return item
  }
}

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
  // Set while the drained file is being truncated; writes wait for it.
  #truncating: Promise<void> | null = null
  #wakeWriter: (() => void) | null = null
  #pending = new Set<Promise<unknown>>()

  // Chunks (records, in object mode) waiting in memory, oldest first, and
  // their total size: bytes, or a count of records.
  #memory = new Queue<unknown>()
  #memorySize = 0

  // Read back from disk (decoded, in object mode) but not yet pushed. Always
  // older than anything in memory or on disk.
  #ready = new Queue<unknown>()
  #decoder = new FrameDecoder()

  #wantsData = false
  #writeEnded = false
  #readEnded = false
  // `push()` can call back into the stream (a 'data' listener calling
  // `read()`, `write()` or `destroy()`), so `#flush()` must not nest.
  #flushing = false
  #flushAgain = false

  /**
   * Reads `source` through a new Spool, so the source is never held back by
   * a slow consumer: `Spool.from(res).pipe(slowWriter)`. Accepts a Node
   * readable, a web `ReadableStream`, or an iterable or async iterable of
   * chunks. Source errors destroy the spool with that error (emitted on the
   * spool, so they surface however it's consumed); destroying the spool
   * destroys the source.
   *
   * Overrides the inherited `Duplex.from()`, which would return a plain
   * `Duplex` with no disk spilling.
   */
  static override from(source: SpoolSource, options?: SpoolOptions & { objectMode?: false }): Spool
  /** Reads records from `source`: `Spool.from(cursor, { objectMode: true })`. */
  static override from(source: SpoolSource<unknown>, options: SpoolOptions & { objectMode: true }): Spool
  /** Reads bytes or records from `source`, as `options.objectMode` says. */
  static override from(source: SpoolSource<unknown>, options?: SpoolOptions): Spool
  static override from(source: SpoolSource<unknown>, options?: SpoolOptions): Spool {
    // Checked before the spool exists, so a bad source can't leave it (and
    // its `signal` listener) behind.
    if (!isSpoolSource(source)) {
      throw new TypeError(
        `syke: Spool.from() needs a Node readable, a web ReadableStream, or an iterable or async iterable, got ${describe(source)}`,
      )
    }
    const spool = new Spool(options)
    connect(toNodeReadable(source, spool.#objectMode), spool)
    return spool
  }

  constructor(options: SpoolOptions = {}) {
    const {
      objectMode,
      codec,
      dir,
      chunkSize,
      memoryThreshold,
      minFreeSpace,
      maxDiskBytes,
      onLimit,
      highWaterMark,
      readableHighWaterMark,
      writableHighWaterMark,
      signal,
    } = options
    if (objectMode !== undefined && typeof objectMode !== 'boolean') {
      throw new TypeError(`syke: objectMode must be a boolean, got ${String(objectMode)}`)
    }
    if (codec !== undefined) {
      if (!objectMode) throw new TypeError('syke: codec needs objectMode: true')
      if (typeof codec?.encode !== 'function' || typeof codec.decode !== 'function') {
        throw new TypeError('syke: codec must have encode() and decode() methods')
      }
    }
    // Only stream options Spool documents: anything else (`autoDestroy`,
    // `readableObjectMode`, ...) would break its assumptions.
    super({
      highWaterMark,
      readableHighWaterMark,
      writableHighWaterMark,
      signal,
      objectMode: objectMode ?? false,
      decodeStrings: true,
    })
    this.#objectMode = objectMode ?? false
    this.#codec = codec ?? v8Codec
    this.#dir = dir ?? tmpdir()
    // In object mode the high-water mark counts records, so it can't size
    // reads; a high-water mark of 0 can't either.
    const readSize =
      this.#objectMode || this.readableHighWaterMark === 0
        ? getDefaultHighWaterMark(false)
        : this.readableHighWaterMark
    this.#chunkSize = positiveInt('chunkSize', chunkSize ?? readSize)
    this.#memoryThreshold = nonNegativeInt('memoryThreshold', memoryThreshold ?? 0)
    this.#maxDiskBytes =
      maxDiskBytes === undefined
        ? Number.POSITIVE_INFINITY
        : positiveInt('maxDiskBytes', maxDiskBytes)
    this.#floor = parseMinFreeSpace(minFreeSpace ?? '10%')
    this.#onLimit = oneOf('onLimit', onLimit ?? 'backpressure', ['backpressure', 'error'])
    this.#extents = new ExtentMap(this.#chunkSize, this.#maxDiskBytes)
  }

  override _write(
    chunk: unknown,
    _encoding: BufferEncoding,
    done: (error?: Error | null) => void,
  ): void {
    if (this.#objectMode) this.#writeRecords([{ chunk }], done)
    else this.#writeBytes([{ chunk }], done)
  }

  // Chunks that queue up while a spill is in flight arrive here together, so
  // they can share one write.
  override _writev(chunks: Entry[], done: (error?: Error | null) => void): void {
    if (this.#objectMode) this.#writeRecords(chunks, done)
    else this.#writeBytes(chunks, done)
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
    this.#memory = new Queue()
    this.#memorySize = 0
    this.#ready = new Queue()
    this.#decoder = new FrameDecoder()
    this.#wantsData = false
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

  /** Something is waiting to be pushed: read-back data, memory or disk. */
  #queued() {
    return this.#ready.length > 0 || this.#memory.length > 0 || !this.#diskEmpty()
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
   * Byte mode: passes chunks through or holds them in memory while that
   * keeps them in order; spills the rest, coalesced into fewer writes.
   */
  #writeBytes(chunks: Entry[], done: (error?: Error | null) => void) {
    if (this.destroyed) return done()
    let start = 0
    while (start < chunks.length && this.#acceptBytes((chunks[start] as Entry).chunk as Buffer)) start++
    if (start === chunks.length) {
      done()
      this.#flush()
      return
    }
    this.#track(this.#spillBytes(chunks, start)).then(() => {
      done()
      this.#flush()
    }, done)
  }

  #acceptBytes(bytes: Buffer) {
    // An empty chunk carries nothing, but would still take a slot in memory.
    return bytes.length === 0 || this.#passThrough(bytes) || this.#toMemory(bytes, bytes.length)
  }

  /** Spills `chunks[start..]`, about `chunkSize` bytes per write. */
  async #spillBytes(chunks: Entry[], start: number) {
    let batch: Buffer[] = []
    let bytes = 0
    const write = async () => {
      const data = batch.length === 1 ? (batch[0] as Buffer) : Buffer.concat(batch, bytes)
      batch = []
      bytes = 0
      await this.#spill(data)
    }

    for (let i = start; i < chunks.length; i++) {
      if (this.destroyed) return
      const chunk = (chunks[i] as Entry).chunk as Buffer
      if (chunk.length === 0) continue
      // If the consumer caught up mid-batch, the rest can skip the disk.
      if (i > start && batch.length === 0 && this.#acceptBytes(chunk)) continue
      batch.push(chunk)
      bytes += chunk.length
      if (bytes >= this.#chunkSize) await write()
    }
    if (batch.length > 0 && !this.destroyed) await write()
  }

  /**
   * Object mode: passes records through or holds them in memory, as they
   * are, while that keeps them in order; encodes the rest to disk.
   */
  #writeRecords(records: Entry[], done: (error?: Error | null) => void) {
    if (this.destroyed) return done()
    let start = 0
    while (start < records.length && this.#acceptRecord((records[start] as Entry).chunk)) start++
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
  async #spillRecords(records: Entry[], start: number) {
    let payloads: Uint8Array[] = []
    let bytes = 0
    const write = async () => {
      // Frames go straight into one buffer: no per-record frame copies.
      const batch = Buffer.allocUnsafe(bytes)
      let offset = 0
      for (const payload of payloads) offset = writeFrame(batch, offset, payload)
      payloads = []
      bytes = 0
      await this.#spill(batch)
    }

    for (let i = start; i < records.length; i++) {
      if (this.destroyed) return
      const record = (records[i] as Entry).chunk
      // If the consumer caught up mid-batch, the rest can skip the codec.
      if (i > start && payloads.length === 0 && this.#acceptRecord(record)) continue
      const payload = encodeRecord(this.#codec, record)
      payloads.push(payload)
      bytes += FRAME_HEADER_BYTES + payload.byteLength
      if (bytes >= this.#chunkSize) await write()
    }
    if (payloads.length > 0 && !this.destroyed) await write()
  }

  /** Object mode: decodes every record completed by `bytes` into the ready queue. */
  #decodeRecords(bytes: Uint8Array) {
    for (const payload of this.#decoder.push(bytes)) {
      const record = decodeRecord(this.#codec, payload)
      // push(null) would end the stream and silently drop the rest.
      if (record === null) {
        throw new SpoolCodecError('syke: codec decoded a record to null, which a Node stream cannot carry')
      }
      this.#ready.push(record)
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
      if (this.#truncating) {
        await this.#truncating
        continue
      }

      // Prefer space inside the file; only ask the filesystem when it must grow.
      let budget = 0
      let target = this.#extents.place(rest.length, 0, false)
      if (!target && !this.#extents.atMaxSize) {
        budget = this.#floor ? await freeSpaceBudget(this.#dir, this.#floor) : Number.POSITIVE_INFINITY
        // State may have changed during the await, so check and place again
        // from scratch. Everything from here to `#writing = true` is synchronous.
        if (this.destroyed || this.#truncating) continue
        target = this.#extents.place(rest.length, budget, false)
      }
      // At a limit, any free gap will do, however small.
      target ??= this.#extents.place(rest.length, budget, true)

      if (!target) {
        // At a limit with nothing queued and the consumer waiting: hand it over.
        if (this.#handOver(rest)) return
        if (this.#onLimit === 'error') throw new SpoolLimitError(this.#limitMessage(budget))
        // Free space on the filesystem can change by itself; space in a
        // full-size file only comes back when the consumer reads.
        await this.#waitForSpace(!this.#extents.atMaxSize && this.#floor !== null)
        continue
      }

      const { position, length } = target
      const piece = rest.subarray(0, length)
      const settle = reserveGrowth(this.#dir, position + length - this.#extents.fileSize)
      const sizeBefore = this.#extents.fileSize
      this.#writing = true
      let written = 0
      let failure: unknown = null
      try {
        const file = await this.#openFile()
        while (written < length && !this.destroyed) {
          const { bytesWritten } = await file.write(piece, written, length - written, position + written)
          if (!(bytesWritten > 0)) {
            throw new Error(`syke: writing the spool file made no progress (${this.filepath})`)
          }
          written += bytesWritten
        }
      } catch (error) {
        failure = error
      } finally {
        this.#writing = false
      }

      this.#extents.wrote(position, written)
      settle(this.#extents.fileSize - sizeBefore)
      if (this.destroyed) return
      rest = rest.subarray(written)

      if (failure !== null) {
        if (!isNoSpaceError(failure)) throw failure
        // The filesystem is full, whatever minFreeSpace says: a disk limit.
        diskFull(this.#dir)
        if (written > 0) this.#flush()
        if (this.#handOver(rest)) return
        if (this.#onLimit === 'error') {
          throw new SpoolLimitError(`syke: the filesystem under ${this.#dir} is out of space`, {
            cause: failure,
          })
        }
        await this.#waitForSpace(true)
        continue
      }
      this.#flush()
    }
  }

  #limitMessage(budget: number) {
    if (this.#extents.wouldReachMax(budget)) {
      return `syke: spool file reached maxDiskBytes (${this.#maxDiskBytes} bytes)`
    }
    return `syke: spilling more would take free space on ${this.#dir} below minFreeSpace`
  }

  /**
   * Resolves when the reader frees space, the consumer asks for data, the
   * stream is destroyed or, with `poll`, after a while, as free space may
   * have changed. The poll timer doesn't keep the process alive.
   */
  #waitForSpace(poll: boolean) {
    return new Promise<void>((resolve) => {
      let timer: NodeJS.Timeout | undefined
      const wake = () => {
        clearTimeout(timer)
        this.#wakeWriter = null
        resolve()
      }
      if (poll) {
        timer = setTimeout(wake, FREE_SPACE_POLL_MS)
        timer.unref?.()
      }
      this.#wakeWriter = wake
    })
  }

  #openFile(): Promise<FileHandle> {
    if (!this.#file) {
      const filepath = join(this.#dir, `syke-spool-${randomUUID()}`)
      // `wx+` refuses to reuse an existing file; 0o600 keeps it private.
      const opening = warnIfMemoryBacked(this.#dir)
        .then(() => open(filepath, 'wx+', 0o600))
        .then((file) => {
          // Only a file that exists gets a path, so cleanup never chases one that doesn't.
          this.filepath = filepath
          trackFile(this, filepath, file)
          return file
        })
      // A failed open (out of space, say) may be retried with a new file.
      opening.catch(() => {
        if (this.#file === opening) this.#file = null
      })
      this.#file = opening
    }
    return this.#file
  }

  #flush() {
    if (this.#flushing) {
      this.#flushAgain = true
      return
    }
    this.#flushing = true
    try {
      do {
        this.#flushAgain = false
        this.#flushOnce()
      } while (this.#flushAgain && !this.destroyed && !this.#readEnded)
    } finally {
      this.#flushing = false
    }
  }

  #flushOnce() {
    if (this.#readEnded || this.destroyed) return
    this.#wakeWriter?.()
    if (this.#reading) return

    // Each push() may run listeners that destroy the stream, so recheck after it.
    while (this.#wantsData && this.#ready.length > 0) {
      this.#wantsData = this.push(this.#ready.shift())
      if (this.destroyed) return
    }
    if (this.#ready.length > 0) return

    while (this.#wantsData && this.#memory.length > 0) {
      const chunk = this.#memory.shift()
      this.#memorySize -= this.#objectMode ? 1 : (chunk as Buffer).length
      this.#wantsData = this.push(chunk)
      if (this.destroyed) return
    }
    if (this.#memory.length > 0) return

    if (!this.#extents.empty) {
      if (this.#wantsData && !this.#reading) this.#readFromDisk()
      return
    }

    if (this.#writeEnded && this.#diskEmpty()) {
      if (this.#decoder.pendingBytes > 0) {
        this.destroy(new Error(`syke: spool file ended mid-record (${this.filepath})`))
        return
      }
      this.#readEnded = true
      this.push(null)
      return
    }
    this.#truncateIfDrained()
  }

  /** Gives the drained file's space back to the filesystem. */
  #truncateIfDrained() {
    const opening = this.#file
    if (
      !opening ||
      this.#truncating ||
      this.#writing ||
      this.#reading ||
      this.#writeEnded ||
      this.destroyed ||
      !this.#extents.empty ||
      this.#extents.fileSize === 0
    ) {
      return
    }
    const size = this.#extents.fileSize
    // `#spill()` waits on this before placing anything, so no write can land
    // in the file while it's cut back to nothing.
    this.#truncating = this.#track(
      opening
        .then((file) => file.truncate(0))
        .then(
          () => {
            if (this.#extents.empty) this.#extents.reset()
            diskFreed(this.#dir, size)
          },
          // Keep the file as it was; its space is still reused.
          () => {},
        )
        .finally(() => {
          this.#truncating = null
          this.#flush()
        }),
    )
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
          this.#ready.push(bytes)
        }
        this.#flush()
      },
      (error: Error) => {
        this.#reading = false
        if (!this.destroyed) this.destroy(error)
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
    // Anything in flight may start more (a write opening the file, say).
    while (this.#pending.size > 0) await Promise.allSettled([...this.#pending])
    const opening = this.#file
    if (!opening) return
    this.#file = null

    const [opened] = await Promise.allSettled([opening])
    const filepath = this.filepath
    if (opened.status !== 'fulfilled' || !filepath) return
    // Garbage collection no longer needs to remove it; the exit hook still
    // does until it's gone, in case the process exits first.
    disownFile(this)
    try {
      await removeFile(filepath, opened.value)
    } finally {
      releaseFile(this, filepath)
    }
  }
}

function isSpoolSource(source: unknown): source is SpoolSource<unknown> {
  if (typeof source === 'string') return true
  if (source === null || (typeof source !== 'object' && typeof source !== 'function')) return false
  const candidate = source as Record<PropertyKey, unknown>
  return (
    typeof candidate.pipe === 'function' ||
    typeof candidate.getReader === 'function' ||
    typeof candidate[Symbol.asyncIterator] === 'function' ||
    typeof candidate[Symbol.iterator] === 'function'
  )
}

function describe(value: unknown): string {
  if (value === null) return 'null'
  if (typeof value !== 'object') return typeof value
  return (value as object).constructor?.name ?? 'object'
}

function toNodeReadable(source: SpoolSource<unknown>, objectMode: boolean): Readable {
  if (source instanceof Readable) return source
  if (typeof (source as NodeJS.ReadableStream).pipe === 'function') {
    // An old-style or foreign stream: wrap it so it can be watched and destroyed.
    return new Readable({ objectMode }).wrap(source as NodeJS.ReadableStream)
  }
  if (typeof (source as ReadableStream).getReader === 'function') {
    return Readable.fromWeb(source as ReadableStream, { objectMode })
  }
  return Readable.from(source as Iterable<unknown> | AsyncIterable<unknown>, { objectMode })
}

/**
 * Pipes `source` into `spool` and propagates failure both ways. Unlike
 * `pipeline()`, it adds no 'error' listener of its own to `spool`, which
 * would swallow errors a `.pipe()` consumer never sees.
 */
function connect(source: Readable, spool: Spool): void {
  source.pipe(spool)
  finished(source, (error) => {
    if (error && !spool.destroyed) spool.destroy(error)
  })
  spool.once('close', () => {
    if (!source.destroyed && !source.readableEnded) source.destroy()
  })
}
