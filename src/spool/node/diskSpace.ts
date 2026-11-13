import { statfs } from 'node:fs/promises'
import { type FreeSpaceFloor, reservedBytes } from '../core/limits.ts'

export { type FreeSpaceFloor, type MinFreeSpace, parseMinFreeSpace } from '../core/limits.ts'

/**
 * How many more bytes can be written to `dir` before free space drops below
 * `floor`. `Infinity` when there is no floor or free space can't be read.
 */
export async function freeSpaceBudget(dir: string, floor: FreeSpaceFloor): Promise<number> {
  if (!floor) return Number.POSITIVE_INFINITY
  try {
    const { bavail, blocks, bsize } = await statfs(dir)
    const reserve = reservedBytes(floor, blocks * bsize)
    return Math.floor(bavail * bsize - reserve)
  } catch {
    return Number.POSITIVE_INFINITY
  }
}

const TMPFS_MAGIC = 0x01021994
const RAMFS_MAGIC = 0x858458f6
const warnedDirs = new Set<string>()

/** Warns once per directory when "disk" is really memory (tmpfs/ramfs). */
export async function warnIfMemoryBacked(dir: string): Promise<void> {
  if (process.platform !== 'linux' || warnedDirs.has(dir)) return
  try {
    const { type } = await statfs(dir)
    if (type !== TMPFS_MAGIC && type !== RAMFS_MAGIC) return
  } catch {
    return
  }
  warnedDirs.add(dir)
  process.emitWarning(
    `${dir} is a memory-backed filesystem, so data a Spool spills there still uses RAM. Pass a 'dir' on a real disk to keep memory bounded.`,
    { code: 'SYKE_SPOOL_TMPFS' },
  )
}
