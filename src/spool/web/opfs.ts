import { ExtentMap, type Region } from '../core/extents.ts'
import { OPFS_WORKER_SOURCE } from './opfsWorker.ts'
import { storageFreed } from './quota.ts'
import { FILE_PREFIX, type SpoolStorage, type SpoolStore } from './store.ts'

/** How long a new worker gets to report in before OPFS is given up on. */
const WORKER_START_TIMEOUT_MS = 10_000

interface Reply {
  id?: number
  ready?: boolean
  value?: unknown
  error?: { name: string; message: string }
}

/**
 * The page's OPFS worker, shared by every spool on it and terminated when
 * the last one closes. Requests are matched to replies by id.
 */
class WorkerChannel {
  readonly #worker: Worker
  readonly #pending = new Map<number, { resolve: (value: unknown) => void; reject: (error: Error) => void }>()
  #nextId = 0
  #users = 0
  #dead: Error | null = null

  constructor(worker: Worker) {
    this.#worker = worker
    worker.addEventListener('message', (event: MessageEvent<Reply>) => {
      const { id, value, error } = event.data
      if (id === undefined) return
      const call = this.#pending.get(id)
      if (!call) return
      this.#pending.delete(id)
      if (error) call.reject(Object.assign(new DOMException(`syke: OPFS ${error.message}`, error.name), { returned: value }))
      else call.resolve(value)
    })
    worker.addEventListener('error', (event) => {
      event.preventDefault?.()
      this.#die(new Error(`syke: the OPFS worker failed${event.message ? `: ${event.message}` : ''}`))
    })
    // A reply that couldn't be deserialized can't be matched to its request,
    // which would then wait forever: fail them all.
    worker.addEventListener('messageerror', () => {
      this.#die(new Error('syke: a reply from the OPFS worker could not be read'))
    })
  }

  call<T>(message: object, transfer: Transferable[] = []): Promise<T> {
    if (this.#dead) return Promise.reject(this.#dead)
    const id = this.#nextId++
    return new Promise<T>((resolve, reject) => {
      this.#pending.set(id, { resolve: resolve as (value: unknown) => void, reject })
      try {
        this.#worker.postMessage({ ...message, id }, transfer)
      } catch (error) {
        this.#pending.delete(id)
        reject(error as Error)
      }
    })
  }

  get alive(): boolean {
    return this.#dead === null
  }

  ref(): void {
    this.#users++
  }

  unref(): void {
    if (--this.#users > 0) return
    this.#die(new Error('syke: the OPFS worker was terminated'))
  }

  #die(error: Error) {
    if (this.#dead) return
    this.#dead = error
    this.#worker.terminate()
    if (current === this) current = null
    for (const call of this.#pending.values()) call.reject(error)
    this.#pending.clear()
  }
}

let current: WorkerChannel | null = null
let starting: Promise<WorkerChannel> | null = null
// Set once a worker can't start on this page (CSP, no sync access handles):
// retrying would only repeat the failure and its console noise.
let unsupported: Error | null = null

/** Starts the OPFS worker, resolving once it reports sync access handles work. */
function startWorker(): Promise<WorkerChannel> {
  return new Promise<WorkerChannel>((resolve, reject) => {
    if (typeof Worker !== 'function' || typeof URL.createObjectURL !== 'function') {
      throw new Error('syke: this runtime has no Worker')
    }
    const url = URL.createObjectURL(new Blob([OPFS_WORKER_SOURCE], { type: 'text/javascript' }))
    let worker: Worker
    try {
      // A `worker-src` CSP without `blob:` throws here in some browsers and
      // fires `error` on the worker in others.
      worker = new Worker(url, { name: 'syke-spool' })
    } catch (error) {
      URL.revokeObjectURL(url)
      throw error
    }
    const done = (error: Error | null) => {
      clearTimeout(timer)
      worker.removeEventListener('message', onMessage)
      worker.removeEventListener('error', onError)
      worker.removeEventListener('messageerror', onMessageError)
      URL.revokeObjectURL(url)
      if (!error) return resolve(new WorkerChannel(worker))
      worker.terminate()
      reject(error)
    }
    const onMessage = (event: MessageEvent<Reply>) => {
      if (event.data?.ready === undefined) return
      done(event.data.ready ? null : new Error('syke: OPFS sync access handles are not supported here'))
    }
    const onError = (event: ErrorEvent) => {
      event.preventDefault?.()
      done(new Error(`syke: the OPFS worker could not start${event.message ? `: ${event.message}` : ''}`))
    }
    const onMessageError = () => {
      done(new Error('syke: the OPFS worker could not start: its message could not be read'))
    }
    const timer = setTimeout(() => {
      // A slow start may just be a busy page, so it isn't remembered as unsupported.
      done(Object.assign(new Error('syke: the OPFS worker did not start in time'), { transient: true }))
    }, WORKER_START_TIMEOUT_MS)
    worker.addEventListener('message', onMessage)
    worker.addEventListener('error', onError)
    worker.addEventListener('messageerror', onMessageError)
  })
}

async function acquireChannel(): Promise<WorkerChannel> {
  for (;;) {
    if (unsupported) throw unsupported
    if (current?.alive) {
      current.ref()
      return current
    }
    starting ??= startWorker().then(
      (channel) => {
        starting = null
        current = channel
        return channel
      },
      (error: Error & { transient?: boolean }) => {
        starting = null
        if (!error.transient) unsupported = error
        throw error
      },
    )
    await starting
  }
}

/**
 * Spool storage in one OPFS file, written and read through a sync access
 * handle in the page's worker. Space the reader is done with is reused
 * before the file grows (`ExtentMap`), as with the Node spool's temp file.
 */
export class OpfsStore implements SpoolStore {
  readonly kind: SpoolStorage = 'opfs'
  readonly #channel: WorkerChannel
  readonly #name: string
  readonly #extents: ExtentMap
  readonly #release: () => void
  #placed: Region | null = null
  #writing = false
  #closing: Promise<void> | null = null

  static async open(id: string, chunkSize: number, maxBytes: number, release: () => void): Promise<OpfsStore> {
    const channel = await acquireChannel()
    const name = FILE_PREFIX + id
    try {
      await channel.call({ op: 'open', name })
    } catch (error) {
      // The file may exist even though the handle failed.
      await channel.call({ op: 'close', name }).catch(() => {})
      // The origin may never use OPFS (a private window, an opaque origin):
      // don't start a worker for every spool just to hear that again.
      if ((error as { name?: unknown }).name === 'SecurityError') unsupported ??= error as Error
      channel.unref()
      throw error
    }
    return new OpfsStore(channel, name, new ExtentMap(chunkSize, maxBytes), release)
  }

  private constructor(channel: WorkerChannel, name: string, extents: ExtentMap, release: () => void) {
    this.#channel = channel
    this.#name = name
    this.#extents = extents
    this.#release = release
  }

  get empty(): boolean {
    return this.#extents.empty
  }

  get atMax(): boolean {
    return this.#extents.atMaxSize
  }

  room(want: number, growth: number, anySize: boolean): number {
    this.#placed = this.#extents.place(want, growth, anySize)
    return this.#placed?.length ?? 0
  }

  wouldReachMax(growth: number): boolean {
    return this.#extents.wouldReachMax(growth)
  }

  async write(bytes: Uint8Array): Promise<number> {
    const placed = this.#placed
    const { byteOffset, byteLength } = bytes
    if (!placed || byteLength > placed.length) throw new Error('syke: spool write without room')
    this.#placed = null
    const before = this.#extents.fileSize
    const buffer = bytes.buffer as ArrayBuffer
    this.#writing = true
    try {
      await this.#channel.call(
        { op: 'write', name: this.#name, at: placed.position, buffer, offset: byteOffset, length: byteLength },
        [buffer],
      )
    } catch (error) {
      // The buffer was transferred; hand back the copy the worker returned.
      const returned = (error as { returned?: unknown }).returned
      if (returned instanceof ArrayBuffer) {
        Object.assign(error as object, { bytes: new Uint8Array(returned, byteOffset, byteLength) })
      }
      throw error
    } finally {
      this.#writing = false
    }
    // `bytes` is detached now; use the lengths saved before the transfer.
    this.#extents.wrote(placed.position, byteLength)
    return this.#extents.fileSize - before
  }

  async read(): Promise<Uint8Array[]> {
    const { position, length } = this.#extents.nextRead() as Region
    const result = await this.#channel.call<{ buffer: ArrayBuffer; length: number }>({
      op: 'read',
      name: this.#name,
      at: position,
      length,
    })
    if (result.length < length) throw new Error(`syke: spool file ${this.#name} ended early`)
    this.#extents.read(length)
    this.#shrinkIfEmpty()
    return [new Uint8Array(result.buffer, 0, length)]
  }

  /**
   * Everything is read: give the file's space back to the origin. Only when
   * no write is placed or in flight, so none can land past the new end; the
   * worker runs requests in order, so later writes follow the truncate.
   * Writes are placed from the start of the file at once, but the space is
   * only given back to the quota budget once the truncate has happened.
   */
  #shrinkIfEmpty() {
    const size = this.#extents.fileSize
    if (!this.#extents.empty || this.#writing || this.#placed || size === 0) return
    this.#extents.reset()
    // On failure the file just stays larger: everything in it is read, and
    // rewriting that space counts as growth again, which errs on the safe side.
    this.#channel.call({ op: 'truncate', name: this.#name, size: 0 }).then(
      () => storageFreed(size),
      () => {},
    )
  }

  close(): Promise<void> {
    this.#closing ??= this.#channel.call({ op: 'close', name: this.#name }).finally(() => {
      this.#channel.unref()
      this.#release()
    }) as Promise<void>
    return this.#closing
  }
}
