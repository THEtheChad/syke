import { statfs } from 'node:fs/promises'
import { resolve } from 'node:path'
import { type FreeSpaceFloor, reservedBytes } from '../core/limits.ts'

export { type FreeSpaceFloor, type MinFreeSpace, parseMinFreeSpace } from '../core/limits.ts'

/** How long a `statfs()` result is trusted before it's read again. */
const STATFS_TTL_MS = 100

interface Snapshot {
  /** Bytes free to unprivileged users when read. */
  available: number
  /** Size of the filesystem, in bytes. */
  total: number
  at: number
}

/**
 * What's known about one directory's filesystem. Every spool in the process
 * writing there counts its growth against the same snapshot, so parallel
 * spools can't each spend the same free space, and a spill doesn't need a
 * `statfs()` per chunk.
 */
interface DirState {
  /** `null` when free space can't be read (no floor applies). */
  snapshot: Snapshot | null
  refreshing: Promise<void> | null
  /** Bytes spools grew the filesystem by since the snapshot was read. */
  grown: number
  /** Of `grown`, bytes reserved for writes still in flight. */
  inFlight: number
}

const dirs = new Map<string, DirState>()
/** Size `dirs` may reach before it's swept for idle entries. */
const SWEEP_AT = 16
let sweepAt = SWEEP_AT

function stateOf(dir: string): DirState {
  const key = resolve(dir)
  let state = dirs.get(key)
  if (!state) {
    if (dirs.size >= sweepAt) sweepIdle()
    state = { snapshot: null, refreshing: null, grown: 0, inFlight: 0 }
    dirs.set(key, state)
  }
  return state
}

/**
 * Drops directories nothing is waiting on. An idle entry is one whose reading
 * is stale with no write in flight or reading pending: the next budget would
 * read the filesystem again and keep nothing of it, so a fresh entry is the
 * same. Sweeps grow further apart while many directories are busy, so
 * creating entries stays O(1) amortized.
 */
function sweepIdle(): void {
  for (const [key, state] of dirs) {
    if (state.inFlight === 0 && state.refreshing === null && stale(state)) dirs.delete(key)
  }
  sweepAt = Math.max(SWEEP_AT, 2 * dirs.size)
}

/** How many directories are tracked, for tests. */
export function trackedDirCount(): number {
  return dirs.size
}

/** How many directories' memory checks are remembered, for tests. */
export function memoryCheckCount(): number {
  return memoryChecks.size
}

function stale(state: DirState): boolean {
  return state.snapshot === null || Date.now() - state.snapshot.at >= STATFS_TTL_MS
}

function refresh(dir: string, state: DirState): Promise<void> {
  state.refreshing ??= (async () => {
    let snapshot: Snapshot | null = null
    try {
      const stats = await statfs(dir)
      // f_frsize is the unit of the block counts where it differs from f_bsize.
      const blockSize = (stats as { frsize?: number }).frsize || stats.bsize
      // ramfs and some virtual filesystems report no blocks at all: unknown, not full.
      if (stats.blocks > 0 && blockSize > 0) {
        snapshot = { available: stats.bavail * blockSize, total: stats.blocks * blockSize, at: Date.now() }
      }
    } catch {}
    state.snapshot = snapshot
    // Writes that finished are in the new reading; ones in flight may not be.
    state.grown = state.inFlight
    state.refreshing = null
  })()
  return state.refreshing
}

/**
 * How many more bytes can be written to `dir` before free space drops below
 * `floor`. `Infinity` when there is no floor or free space can't be read.
 */
export async function freeSpaceBudget(dir: string, floor: FreeSpaceFloor): Promise<number> {
  if (!floor) return Number.POSITIVE_INFINITY
  const state = stateOf(dir)
  if (stale(state)) await refresh(dir, state)
  const { snapshot } = state
  if (!snapshot) return Number.POSITIVE_INFINITY
  return Math.floor(snapshot.available - state.grown - reservedBytes(floor, snapshot.total))
}

/**
 * Counts `bytes` of growth in `dir` against its free space now, before the
 * write lands. Call the returned function once the write settles, with how
 * many bytes it actually grew the file by.
 */
export function reserveGrowth(dir: string, bytes: number): (grew: number) => void {
  if (!(bytes > 0)) return () => {}
  const state = stateOf(dir)
  state.grown += bytes
  state.inFlight += bytes
  let settled = false
  return (grew) => {
    if (settled) return
    settled = true
    state.inFlight = Math.max(0, state.inFlight - bytes)
    state.grown -= bytes - Math.min(bytes, Math.max(0, grew))
  }
}

/** The filesystem refused a write for lack of space: treat it as full until it's read again. */
export function diskFull(dir: string): void {
  const state = stateOf(dir)
  state.snapshot = { available: 0, total: state.snapshot?.total ?? 0, at: Date.now() }
  state.grown = state.inFlight
}

/** A spool shrank its file by `bytes`: they count as free again until the next reading. */
export function diskFreed(dir: string, bytes: number): void {
  const state = dirs.get(resolve(dir))
  if (state && bytes > 0) state.grown -= bytes
}

/** Whether `error` is the filesystem (or a quota) running out of space. */
export function isNoSpaceError(error: unknown): boolean {
  const code = (error as { code?: unknown } | null)?.code
  return code === 'ENOSPC' || code === 'EDQUOT'
}

const TMPFS_MAGIC = 0x01021994
const RAMFS_MAGIC = 0x858458f6
/** Directories whose check is remembered; the least recently used is forgotten first. */
const MEMORY_CHECKS_KEPT = 64
// One check per directory, whatever it found, shared by concurrent callers.
// Map order is recency: oldest first.
const memoryChecks = new Map<string, Promise<void>>()

/**
 * Warns once per directory when "disk" is really memory (tmpfs/ramfs). Only
 * the last `MEMORY_CHECKS_KEPT` directories are remembered, so a process that
 * cycles through more may check (and warn about) one again.
 */
export function warnIfMemoryBacked(dir: string): Promise<void> {
  if (process.platform !== 'linux') return Promise.resolve()
  const key = resolve(dir)
  let check = memoryChecks.get(key)
  if (check) {
    memoryChecks.delete(key)
    memoryChecks.set(key, check)
  } else {
    const current: Promise<void> = statfs(key).then(
      ({ type }) => {
        if (type !== TMPFS_MAGIC && type !== RAMFS_MAGIC) return
        process.emitWarning(
          `${key} is a memory-backed filesystem, so data a Spool spills there still uses RAM. Pass a 'dir' on a real disk to keep memory bounded.`,
          { code: 'SYKE_SPOOL_TMPFS' },
        )
      },
      () => {
        // Couldn't tell (the directory may not exist yet): ask again next time.
        if (memoryChecks.get(key) === current) memoryChecks.delete(key)
      },
    )
    check = current
    memoryChecks.set(key, check)
    if (memoryChecks.size > MEMORY_CHECKS_KEPT) {
      memoryChecks.delete(memoryChecks.keys().next().value as string)
    }
  }
  return check
}
