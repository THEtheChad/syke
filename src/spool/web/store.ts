import { SykeError } from '../../shared/errors.ts'

/** Where a browser spool keeps its backlog. `'memory'` means no storage was available. */
export type SpoolStorage = 'opfs' | 'indexeddb' | 'memory'

/** The storage a spool asked for (`'auto'`) or insisted on was unavailable. */
export class SpoolStorageError extends SykeError {
  override readonly code: 'ERR_SYKE_SPOOL_STORAGE' = 'ERR_SYKE_SPOOL_STORAGE'
  override name = 'SpoolStorageError'
}

/**
 * One spool's backlog in storage: a FIFO of bytes. Writes and reads come from
 * a single spool, at most one of each in flight.
 */
export interface SpoolStore {
  readonly kind: SpoolStorage
  /** No committed bytes are waiting to be read. */
  readonly empty: boolean
  /** Storage can't take more bytes, whatever the free space. */
  readonly atMax: boolean
  /**
   * How many of `want` bytes the next `write()` may store, if storage may
   * grow by `growth` bytes. Unless `anySize`, room smaller than a chunk is
   * not offered (to avoid fragmentation). `0` means no room.
   */
  room(want: number, growth: number, anySize: boolean): number
  /** Whether growing by `growth` bytes would reach `maxDiskBytes`. */
  wouldReachMax(growth: number): boolean
  /**
   * Stores `bytes` (at most what `room()` just offered). The caller hands over
   * `bytes`: its buffer may be transferred. Resolves to how many bytes
   * storage grew by. On failure, if `bytes` was transferred, the error's
   * `bytes` property holds them again.
   */
  write(bytes: Uint8Array): Promise<number>
  /** Takes the oldest stored bytes, in order. Only called when not `empty`. */
  read(): Promise<Uint8Array[]>
  /** Deletes everything stored and releases the store. Safe to call twice. */
  close(): Promise<void>
}

/** Used when no storage is available: never has room, so `onLimit` applies. */
export class NoStore implements SpoolStore {
  readonly kind: SpoolStorage = 'memory'
  /** Why no storage was available. */
  readonly reason: string
  constructor(reason: string) {
    this.reason = reason
  }
  readonly empty = true
  readonly atMax = true
  room(): number {
    return 0
  }
  wouldReachMax(): boolean {
    return true
  }
  write(): Promise<number> {
    return Promise.reject(new Error('syke: no spool storage'))
  }
  read(): Promise<Uint8Array[]> {
    return Promise.resolve([])
  }
  close(): Promise<void> {
    return Promise.resolve()
  }
}

/** Prefix of every OPFS file a spool creates. */
export const FILE_PREFIX = 'syke-spool-'
/** Prefix of every spool's Web Lock name. */
export const LOCK_PREFIX = 'syke-spool:'

/** A random spool id. `crypto.randomUUID` needs a secure context, so fall back. */
export function spoolId(): string {
  if (typeof crypto.randomUUID === 'function') return crypto.randomUUID()
  const bytes = crypto.getRandomValues(new Uint8Array(16))
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')
}

function locks(): LockManager | undefined {
  try {
    return globalThis.navigator?.locks
  } catch {
    return undefined
  }
}

/** Opaque origins (sandboxed iframes, `data:` URLs) refuse Web Locks, as they refuse storage. */
function isSecurityError(error: unknown): boolean {
  return (error as { name?: unknown } | null)?.name === 'SecurityError'
}

/**
 * Holds the spool's Web Lock until the returned function is called. The
 * browser releases it if the page goes away, which is how the sweep tells an
 * abandoned spool from a live one. Without Web Locks, or where the origin
 * may not use them (an opaque origin, which can't share storage with another
 * page's sweep either), there's nothing to hold.
 */
export async function holdLock(id: string): Promise<() => void> {
  const manager = locks()
  if (!manager) return () => {}
  let release!: () => void
  const held = new Promise<void>((resolve, reject) => {
    manager
      .request(LOCK_PREFIX + id, () => {
        resolve()
        return new Promise<void>((done) => {
          release = done
        })
      })
      .catch(reject)
  })
  try {
    await held
  } catch (error) {
    if (isSecurityError(error)) return () => {}
    throw error
  }
  let released = false
  return () => {
    if (released) return
    released = true
    release()
  }
}

/** Runs `fn` with the lock of spool `id` if it's free, i.e. its page has gone. */
export async function ifAbandoned(id: string, fn: () => Promise<void>): Promise<void> {
  const manager = locks()
  if (!manager) return
  await manager.request(LOCK_PREFIX + id, { ifAvailable: true }, async (lock) => {
    if (lock) await fn()
  })
}

/** Whether a sweep can tell live spools from abandoned ones. */
export function canSweep(): boolean {
  return locks() !== undefined
}
