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

function estimate(): (() => Promise<StorageEstimate>) | null {
  const storage = globalThis.navigator?.storage
  return typeof storage?.estimate === 'function' ? () => storage.estimate() : null
}

function refresh(): Promise<void> {
  refreshing ??= (async () => {
    const read = estimate()
    try {
      const { quota, usage } = read ? await read() : {}
      snapshot =
        typeof quota === 'number' && typeof usage === 'number'
          ? { quota, usage, at: Date.now() }
          : null
    } catch {
      snapshot = null
    }
    grownSinceSnapshot = 0
    refreshing = null
  })()
  return refreshing
}

/**
 * How many more bytes spools on this page may add to the origin's storage
 * before its free space (quota minus usage) drops below `floor`. `Infinity`
 * when there's no floor or the quota can't be read.
 */
export async function storageBudget(floor: FreeSpaceFloor): Promise<number> {
  if (!floor) return Number.POSITIVE_INFINITY
  if (!snapshot || Date.now() - snapshot.at >= ESTIMATE_TTL_MS) await refresh()
  if (!snapshot) return Number.POSITIVE_INFINITY
  const { quota, usage } = snapshot
  return Math.floor(quota - usage - grownSinceSnapshot - reservedBytes(floor, quota))
}

/** Records that a spool on this page grew storage by `bytes`. */
export function storageGrew(bytes: number): void {
  grownSinceSnapshot += bytes
}

/** Storage refused a write (`QuotaExceededError`): treat it as full until the next estimate. */
export function storageFull(): void {
  if (snapshot) snapshot.usage = snapshot.quota
  else snapshot = { quota: 0, usage: 0, at: Date.now() }
}

export function isQuotaError(error: unknown): boolean {
  return (error as { name?: unknown } | null)?.name === 'QuotaExceededError'
}
