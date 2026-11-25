import { type FreeSpaceFloor, reservedBytes } from '../core/limits.ts'

/** How long a `navigator.storage.estimate()` result is trusted. */
const ESTIMATE_TTL_MS = 1000

interface Snapshot {
  quota: number
  usage: number
  at: number
}

// Shared by every spool on the page: growth is counted against one snapshot,
// so parallel spools can't each spend the same free space.
let snapshot: Snapshot | null = null
let refreshing: Promise<void> | null = null
let grownSinceSnapshot = 0
/** When storage last refused a write, or `null`. */
let fullAt: number | null = null

function estimate(): (() => Promise<StorageEstimate>) | null {
  const storage = globalThis.navigator?.storage
  return typeof storage?.estimate === 'function' ? () => storage.estimate() : null
}

function recentlyFull(): boolean {
  return fullAt !== null && Date.now() - fullAt < ESTIMATE_TTL_MS
}

function refresh(): Promise<void> {
  refreshing ??= (async () => {
    // Growth and refusals reported while the estimate is read must survive it:
    // the estimate may predate them.
    const grownBefore = grownSinceSnapshot
    const fullBefore = fullAt
    let next: Snapshot | null = null
    try {
      const read = estimate()
      const { quota, usage } = read ? await read() : {}
      if (typeof quota === 'number' && typeof usage === 'number') next = { quota, usage, at: Date.now() }
    } catch {
      next = null
    }
    grownSinceSnapshot = Math.max(0, grownSinceSnapshot - grownBefore)
    if (fullAt !== fullBefore && next) next.usage = next.quota
    snapshot = next
    refreshing = null
  })()
  return refreshing
}

/**
 * How many more bytes spools on this page may add to the origin's storage
 * before its free space (quota minus usage) drops below `floor`. `Infinity`
 * when there's no floor or the quota can't be read, unless storage just
 * refused a write.
 */
export async function storageBudget(floor: FreeSpaceFloor): Promise<number> {
  if (!floor) return recentlyFull() ? 0 : Number.POSITIVE_INFINITY
  if (!snapshot || Date.now() - snapshot.at >= ESTIMATE_TTL_MS) await refresh()
  if (!snapshot) return recentlyFull() ? 0 : Number.POSITIVE_INFINITY
  const { quota, usage } = snapshot
  return Math.max(0, Math.floor(quota - usage - grownSinceSnapshot - reservedBytes(floor, quota)))
}

/** Records that a spool on this page grew storage by `bytes`. */
export function storageGrew(bytes: number): void {
  grownSinceSnapshot += bytes
}

/**
 * Records that a spool on this page released `bytes` of storage. Only growth
 * since the last estimate is given back: the estimate is the authority.
 */
export function storageFreed(bytes: number): void {
  grownSinceSnapshot = Math.max(0, grownSinceSnapshot - bytes)
}

/**
 * Storage refused a write (`QuotaExceededError`): treat it as full, with or
 * without a `minFreeSpace` floor, until a fresh estimate is due.
 */
export function storageFull(): void {
  const now = Date.now()
  fullAt = now
  grownSinceSnapshot = 0
  if (snapshot) {
    snapshot.usage = snapshot.quota
    snapshot.at = now
  } else {
    snapshot = { quota: 0, usage: 0, at: now }
  }
}

export function isQuotaError(error: unknown): boolean {
  return (error as { name?: unknown } | null)?.name === 'QuotaExceededError'
}
