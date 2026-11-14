import { nonNegativeInt, oneOf, positiveInt } from '../../shared/validate.ts'
import { type FreeSpaceFloor, type MinFreeSpace, parseMinFreeSpace, SpoolLimitError } from '../core/limits.ts'
import { type Codec, decodeRecord, encodeFrame, encodeRecord, FrameDecoder, jsonCodec } from '../core/records.ts'
import { openStore, type SpoolStorageChoice } from './openStore.ts'
import { isQuotaError, storageBudget, storageFull, storageGrew } from './quota.ts'
import { NoStore, type SpoolStorage, type SpoolStore } from './store.ts'

export interface SpoolOptions<T = Uint8Array> {
  /**
   * Largest chunk, in bytes, read back from storage at once (and, in byte
   * mode, pushed downstream). Defaults to 64 KiB.
   */
  chunkSize?: number
  /**
   * How much to hold in memory before spilling to storage: bytes, or records
   * in object mode. Defaults to `0`, so everything the consumer is not ready
   * for goes to storage.
   */
  memoryThreshold?: number
  /**
   * Stop growing storage before the origin's free storage (quota minus usage,
   * from `navigator.storage.estimate()`) drops below this: a percentage of
   * the quota (`'10%'`), a number of bytes, or `false` for no floor.
   * Defaults to `'10%'`.
   */
  minFreeSpace?: MinFreeSpace
  /** Most bytes this spool may keep in storage. Defaults to no cap. */
  maxDiskBytes?: number
  /**
   * What to do at a storage limit. `'backpressure'` (the default) pauses the
   * source until the consumer frees space; `'error'` fails the stream with a
   * `SpoolLimitError`.
   */
  onLimit?: 'backpressure' | 'error'
  /**
   * Where to spill: `'auto'` (the default) uses OPFS if this page can, else
   * IndexedDB, else nothing (see `onLimit`). `'opfs'` or `'indexeddb'` fail
   * with a `SpoolStorageError` if that backend is unavailable.
   */
  storage?: SpoolStorageChoice
  /** Carry arbitrary values instead of bytes. They're encoded with `codec` only when spilled. */
  objectMode?: boolean
  /** Turns records into bytes for storage, in object mode. Defaults to `jsonCodec`. */
  codec?: Codec<T>
}

/** Anything `Spool.from()` can read from. */
export type SpoolSource<T = Uint8Array> = ReadableStream<T> | Iterable<T> | AsyncIterable<T>

/**
 * Bytes collected before a storage write. Writes go one at a time and chunks
 * arriving meanwhile join the next, so batches grow with the write rate up to
 * this (IndexedDB transactions cost up to ~2.5 ms each).
 */
const BATCH_BYTES = 1024 * 1024
const DEFAULT_CHUNK_SIZE = 64 * 1024
/** How often to recheck the storage quota while waiting on `minFreeSpace`. */
const FREE_SPACE_POLL_MS = 250

// Deletes the storage of a spool that was garbage-collected without ending.
const abandoned =
  typeof FinalizationRegistry === 'function'
    ? new FinalizationRegistry<SpoolStore>((store) => {
        store.close().catch(() => {})
      })
    : null

/**
 * A pass-through `{ readable, writable }` pair that doesn't apply
 * backpressure to its writable side while storage has room. Whatever the
 * consumer is not ready for is spilled to the origin's storage (OPFS, or
 * IndexedDB as a fallback) and read back, `chunkSize` bytes at a time, as the
 * consumer drains. Use it with `pipeThrough()`.
 */
export class Spool<T = Uint8Array> {
  readonly readable: ReadableStream<T>
  readonly writable: WritableStream<T>
  /**
   * Settles once the spool's storage has been deleted and released, after
   * the stream ends, errors or is cancelled. Rejects if deleting failed (the
   * next page's sweep removes the leftovers). Never an unhandled rejection.
   */
  readonly closed: Promise<void>

  readonly #chunkSize: number
  readonly #memoryThreshold: number
  readonly #maxDiskBytes: number
  readonly #floor: FreeSpaceFloor
  readonly #onLimit: 'backpressure' | 'error'
  readonly #storageChoice: SpoolStorageChoice
  readonly #codec: Codec<T> | null
  readonly #decoder: FrameDecoder | null

  #readableController!: ReadableStreamDefaultController<T>
  #writableController!: WritableStreamDefaultController
  #settleClosed!: (cleanup: Promise<void>) => void

  #store: SpoolStore | null = null
  #storeOpening: Promise<SpoolStore> | null = null
  readonly #pending = new Set<Promise<unknown>>()

  // Data in stream order: #memory (oldest), #out (read back from storage),
  // storage, the write in flight, then #batch (newest).
  #memory: T[] = []
  #memorySize = 0
  #out: T[] = []
  readonly #batch = new Batch()

  #wantsData = false
  #pullDone: (() => void) | null = null
  #writerRunning = false
  #writing = false
  #reading = false
  #writeEnded = false
  #done = false
  #pumping = false
  #pumpAgain = false
  #wakeWriter: (() => void) | null = null
  #batchWaiter: { resolve: () => void; reject: (error: unknown) => void } | null = null

  /**
   * Reads `source` through a new Spool, so the source is never held back by
   * a slow consumer: `Spool.from(res.body).readable.pipeTo(slowWritable)`.
   * Accepts a `ReadableStream` or an iterable or async iterable of chunks.
   * Errors and cancellation propagate both ways, as with `pipeTo`.
   */
  static from<T = Uint8Array>(source: SpoolSource<T>, options?: SpoolOptions<T>): Spool<T> {
    const spool = new Spool<T>(options)
    // Errors surface on spool.readable; this only stops an unhandled rejection.
    toReadableStream(source)
      .pipeTo(spool.writable)
      .catch(() => {})
    return spool
  }

  constructor(options: SpoolOptions<T> = {}) {
    const { chunkSize, memoryThreshold, minFreeSpace, maxDiskBytes, onLimit, storage, objectMode, codec } = options
    this.#chunkSize = positiveInt('chunkSize', chunkSize ?? DEFAULT_CHUNK_SIZE)
    this.#memoryThreshold = nonNegativeInt('memoryThreshold', memoryThreshold ?? 0)
    this.#maxDiskBytes =
      maxDiskBytes === undefined ? Number.POSITIVE_INFINITY : positiveInt('maxDiskBytes', maxDiskBytes)
    this.#floor = parseMinFreeSpace(minFreeSpace ?? '10%')
    this.#onLimit = oneOf('onLimit', onLimit ?? 'backpressure', ['backpressure', 'error'])
    this.#storageChoice = oneOf('storage', storage ?? 'auto', ['auto', 'opfs', 'indexeddb'])
    if (objectMode !== undefined && typeof objectMode !== 'boolean') {
      throw new TypeError(`syke: objectMode must be a boolean, got ${String(objectMode)}`)
    }
    if (codec !== undefined) {
      if (!objectMode) throw new TypeError('syke: codec only applies with objectMode: true')
      if (typeof codec?.encode !== 'function' || typeof codec.decode !== 'function') {
        throw new TypeError('syke: codec must have encode() and decode() methods')
      }
    }
    this.#codec = objectMode ? (codec ?? (jsonCodec as Codec<T>)) : null
    this.#decoder = objectMode ? new FrameDecoder() : null

    this.closed = new Promise<void>((resolve, reject) => {
      this.#settleClosed = (cleanup) => cleanup.then(resolve, reject)
    })
    this.closed.catch(() => {})

    this.readable = new ReadableStream<T>(
      {
        start: (controller) => {
          this.#readableController = controller
        },
        // Resolves once something is delivered, so a pending pull means the
        // consumer is waiting.
        pull: () =>
          new Promise<void>((resolve) => {
            this.#pullDone = resolve
            this.#wantsData = true
            this.#pump()
          }),
        cancel: (reason) => this.#fail(reason),
      },
      { highWaterMark: 1 },
    )

    this.writable = new WritableStream<T>(
      {
        start: (controller) => {
          this.#writableController = controller
        },
        write: (chunk) =>
          this.#write(chunk).catch((error: unknown) => {
            this.#fail(error)
            throw error
          }),
        close: () => {
          this.#writeEnded = true
          this.#pump()
        },
        abort: (reason) => this.#fail(reason),
      },
      { highWaterMark: 1 },
    )
  }

  /**
   * Where the backlog is kept: `'opfs'`, `'indexeddb'`, or `'memory'` when no
   * storage was available. `null` until the spool first spills.
   */
  get storage(): SpoolStorage | null {
    return this.#store?.kind ?? null
  }

  async #write(chunk: T): Promise<void> {
    if (!this.#codec && !isUint8Array(chunk)) {
      throw new TypeError(`syke: Spool chunks must be Uint8Array (or use objectMode), got ${describe(chunk)}`)
    }

    // The consumer is waiting and nothing is queued ahead: hand it straight over.
    if (this.#wantsData && this.#memory.length === 0 && !this.#spilled()) {
      this.#deliver(chunk)
      return
    }

    // Memory only takes data while nothing is spilled, so memory is always older.
    const size = this.#codec ? 1 : (chunk as Uint8Array).byteLength
    if (!this.#spilled() && this.#memorySize + size <= this.#memoryThreshold) {
      this.#memory.push(chunk)
      this.#memorySize += size
      return
    }

    // Copied into the batch: the caller's chunk is never kept or transferred.
    let bytes = this.#codec ? encodeFrame(encodeRecord(this.#codec, chunk)) : (chunk as Uint8Array)
    while (bytes.byteLength > 0) {
      if (this.#batch.space === 0) await this.#batchSpace()
      const taken = this.#batch.append(bytes)
      bytes = bytes.subarray(taken)
      this.#startWriter()
    }
    this.#pump()
  }

  /** Something is queued in storage, in flight or in the batch. */
  #spilled(): boolean {
    return (
      !this.#batch.empty ||
      this.#writing ||
      this.#reading ||
      this.#out.length > 0 ||
      !(this.#store?.empty ?? true) ||
      (this.#decoder?.pendingBytes ?? 0) > 0
    )
  }

  #startWriter() {
    if (this.#writerRunning || this.#done) return
    this.#writerRunning = true
    this.#track(this.#runWriter()).then(
      () => {
        this.#writerRunning = false
        // Data may have arrived after the loop's last check.
        if (!this.#batch.empty) this.#startWriter()
      },
      (error: unknown) => {
        this.#writerRunning = false
        this.#fail(error)
      },
    )
  }

  /** Moves the batch into storage, one write at a time, honouring the limits. */
  async #runWriter(): Promise<void> {
    while (!this.#done && !this.#batch.empty) {
      const store = await this.#openStore()
      if (this.#done) return
      const { room, budget } = await this.#room(store)
      if (this.#done || this.#batch.empty) return

      if (room === 0) {
        // At a limit with the consumer waiting and nothing ahead: it takes the batch.
        this.#pump()
        if (this.#batch.empty) return
        if (this.#onLimit === 'error') throw new SpoolLimitError(this.#limitMessage(store, budget))
        await this.#waitForSpace(store)
        continue
      }

      const bytes = this.#batch.take(room)
      this.#writing = true
      this.#notifyBatchSpace()
      const result = await store.write(bytes).then(
        (grew) => ({ grew, error: null }),
        (error: unknown) => ({ grew: 0, error }),
      )
      this.#writing = false
      if (this.#done) return

      if (result.error !== null) {
        const returned = (result.error as { bytes?: unknown }).bytes
        const intact = returned instanceof Uint8Array ? returned : bytes
        if (!isQuotaError(result.error) || intact.byteLength !== room) throw result.error
        // The browser's quota is a hard limit, whatever minFreeSpace says.
        this.#batch.unshift(intact)
        storageFull()
        if (this.#onLimit === 'error') {
          throw new SpoolLimitError('syke: the browser refused to store more (storage quota exceeded)', {
            cause: result.error,
          })
        }
        await this.#waitForSpace(store)
        continue
      }

      storageGrew(result.grew)
      this.#pump()
    }
  }

  #openStore(): Promise<SpoolStore> {
    this.#storeOpening ??= this.#track(
      openStore(this.#storageChoice, this.#chunkSize, this.#maxDiskBytes).then((store) => {
        this.#store = store
        abandoned?.register(this, store, this)
        return store
      }),
    )
    return this.#storeOpening
  }

  /** How much of the batch storage can take now, asking for the quota only if it must grow. */
  async #room(store: SpoolStore): Promise<{ room: number; budget: number }> {
    let room = store.room(this.#batch.length, 0, false)
    if (room > 0) return { room, budget: 0 }
    let budget = 0
    if (!store.atMax) {
      budget = await storageBudget(this.#floor)
      // The batch may have changed during the await; everything from here to
      // the write starting is synchronous.
      room = store.room(this.#batch.length, budget, false)
      if (room > 0) return { room, budget }
    }
    // At a limit, any free space will do, however small.
    return { room: store.room(this.#batch.length, budget, true), budget }
  }

  #limitMessage(store: SpoolStore, budget: number) {
    if (store instanceof NoStore) return `syke: no browser storage is available to spill to (${store.reason})`
    if (store.wouldReachMax(budget)) return `syke: spool storage reached maxDiskBytes (${this.#maxDiskBytes} bytes)`
    return "syke: spilling more would take the origin's free storage below minFreeSpace"
  }

  /** Resolves when the reader frees space, the consumer asks for data, or the quota may have changed. */
  #waitForSpace(store: SpoolStore) {
    return new Promise<void>((resolve) => {
      const wake = () => {
        clearTimeout(timer)
        this.#wakeWriter = null
        resolve()
      }
      // With no storage only the consumer can make room; otherwise poll the quota.
      const timer = store instanceof NoStore ? undefined : setTimeout(wake, FREE_SPACE_POLL_MS)
      this.#wakeWriter = wake
    })
  }

  #batchSpace() {
    return new Promise<void>((resolve, reject) => {
      this.#batchWaiter = { resolve, reject }
    })
  }

  #notifyBatchSpace() {
    if (this.#batch.space === 0) return
    const waiter = this.#batchWaiter
    this.#batchWaiter = null
    waiter?.resolve()
  }

  /** Delivers whatever the consumer can have, in order, and ends the stream when all is read. */
  #pump() {
    if (this.#pumping) {
      this.#pumpAgain = true
      return
    }
    this.#pumping = true
    try {
      do {
        this.#pumpAgain = false
        this.#pumpOnce()
      } while (this.#pumpAgain && !this.#done)
    } catch (error) {
      this.#fail(error)
    } finally {
      this.#pumping = false
    }
  }

  #pumpOnce() {
    if (this.#done) return
    this.#wakeWriter?.()

    while (this.#wantsData) {
      if (this.#memory.length > 0) {
        const chunk = this.#memory.shift() as T
        this.#memorySize -= this.#codec ? 1 : (chunk as Uint8Array).byteLength
        this.#deliver(chunk)
        continue
      }
      if (this.#out.length > 0) {
        this.#deliver(this.#out.shift() as T)
        continue
      }
      if (this.#reading) return
      if (this.#store && !this.#store.empty) {
        this.#startRead(this.#store)
        return
      }
      // The write in flight is older than the batch: wait for it to land.
      if (this.#writing) return
      if (!this.#batch.empty) {
        // Nothing stored: read the batch straight from memory.
        this.#feed(this.#batch.take(this.#batch.length))
        this.#notifyBatchSpace()
        continue
      }
      break
    }

    if (this.#writeEnded && this.#memory.length === 0 && !this.#spilled()) {
      if ((this.#decoder?.pendingBytes ?? 0) > 0) throw new Error('syke: spool ended mid-record')
      this.#done = true
      this.#readableController.close()
      this.#release()
    }
  }

  #deliver(chunk: T) {
    this.#wantsData = false
    this.#readableController.enqueue(chunk)
    const pullDone = this.#pullDone
    this.#pullDone = null
    pullDone?.()
  }

  /** Queues bytes read back from storage (or the batch) for delivery. */
  #feed(bytes: Uint8Array) {
    if (this.#codec) {
      for (const payload of (this.#decoder as FrameDecoder).push(bytes)) this.#out.push(decodeRecord(this.#codec, payload) as T)
      return
    }
    for (let offset = 0; offset < bytes.byteLength; offset += this.#chunkSize) {
      this.#out.push(bytes.subarray(offset, offset + this.#chunkSize) as T)
    }
  }

  #startRead(store: SpoolStore) {
    this.#reading = true
    this.#track(store.read()).then(
      (pieces) => {
        this.#reading = false
        if (this.#done) return
        try {
          for (const piece of pieces) this.#feed(piece)
        } catch (error) {
          this.#fail(error)
          return
        }
        this.#pump()
      },
      (error: unknown) => {
        this.#reading = false
        this.#fail(error)
      },
    )
  }

  /** Fails both sides with `reason` (also how cancel and abort land here) and cleans up. */
  #fail(reason: unknown) {
    if (this.#done) return
    this.#done = true
    // Either may already be closed or errored; erroring them again is a no-op.
    this.#readableController.error(reason)
    this.#writableController.error(reason)
    this.#batchWaiter?.reject(reason)
    this.#batchWaiter = null
    this.#release()
  }

  #release() {
    const pullDone = this.#pullDone
    this.#pullDone = null
    pullDone?.()
    this.#wakeWriter?.()
    this.#memory = []
    this.#memorySize = 0
    this.#out = []
    this.#batch.clear()
    this.#settleClosed(this.#cleanup())
  }

  async #cleanup() {
    // Let I/O in flight (including opening storage) settle before deleting.
    while (this.#pending.size > 0) await Promise.allSettled([...this.#pending])
    const store = this.#store
    if (!store) return
    abandoned?.unregister(this)
    await store.close()
  }

  #track<R>(promise: Promise<R>): Promise<R> {
    this.#pending.add(promise)
    const untrack = () => this.#pending.delete(promise)
    promise.then(untrack, untrack)
    return promise
  }
}

/**
 * Bytes waiting to be written to storage: one growable buffer of copies of
 * the caller's chunks, at most `BATCH_BYTES` (more only after a refused write
 * is put back). Whatever `take()` returns belongs to the caller.
 */
class Batch {
  #buffer: Uint8Array | null = null
  #start = 0
  #end = 0

  get length(): number {
    return this.#end - this.#start
  }

  get empty(): boolean {
    return this.#end === this.#start
  }

  get space(): number {
    return Math.max(0, BATCH_BYTES - this.length)
  }

  /** Copies as much of `bytes` as fits; returns how many. */
  append(bytes: Uint8Array): number {
    const count = Math.min(bytes.byteLength, this.space)
    if (count === 0) return 0
    if (!this.#buffer || this.#end + count > this.#buffer.byteLength) {
      const length = this.length
      const next = new Uint8Array(Math.min(BATCH_BYTES, Math.max(2 * (length + count), 16 * 1024)))
      if (this.#buffer) next.set(this.#buffer.subarray(this.#start, this.#end))
      this.#buffer = next
      this.#start = 0
      this.#end = length
    }
    this.#buffer.set(bytes.subarray(0, count), this.#end)
    this.#end += count
    return count
  }

  /** Removes the first `count` bytes and hands them over. */
  take(count: number): Uint8Array {
    const buffer = this.#buffer as Uint8Array
    if (count >= this.length) {
      // The whole buffer goes with them; the next append starts a new one.
      const all = buffer.subarray(this.#start, this.#end)
      this.clear()
      return all
    }
    const part = buffer.slice(this.#start, this.#start + count)
    this.#start += count
    return part
  }

  /** Puts `bytes` back in front (a write storage refused). */
  unshift(bytes: Uint8Array): void {
    const next = new Uint8Array(bytes.byteLength + this.length)
    next.set(bytes)
    if (this.#buffer) next.set(this.#buffer.subarray(this.#start, this.#end), bytes.byteLength)
    this.#buffer = next
    this.#start = 0
    this.#end = next.byteLength
  }

  clear(): void {
    this.#buffer = null
    this.#start = 0
    this.#end = 0
  }
}

function isUint8Array(value: unknown): value is Uint8Array {
  // Not `instanceof`: chunks may come from another realm (an iframe).
  return ArrayBuffer.isView(value) && (value as Uint8Array)[Symbol.toStringTag] === 'Uint8Array'
}

function describe(value: unknown): string {
  if (value === null) return 'null'
  if (typeof value !== 'object') return typeof value
  return (value as object).constructor?.name ?? 'object'
}

function toReadableStream<T>(source: SpoolSource<T>): ReadableStream<T> {
  if (typeof (source as ReadableStream<T>).pipeTo === 'function') return source as ReadableStream<T>
  const iterator =
    Symbol.asyncIterator in (source as object)
      ? (source as AsyncIterable<T>)[Symbol.asyncIterator]()
      : (source as Iterable<T>)[Symbol.iterator]()
  return new ReadableStream<T>(
    {
      async pull(controller) {
        const { value, done } = await iterator.next()
        if (done) controller.close()
        else controller.enqueue(value)
      },
      async cancel(reason) {
        await iterator.return?.(reason)
      },
    },
    { highWaterMark: 0 },
  )
}
