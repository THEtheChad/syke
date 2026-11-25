import { SykeError } from '../../shared/errors.ts'

/**
 * A spool hit `maxDiskBytes` or `minFreeSpace` with `onLimit: 'error'` (or,
 * in the browser, had no storage to spill to). Shared by `syke/node` and
 * `syke/web`, so `instanceof` works whichever entry it came from.
 */
export class SpoolLimitError extends SykeError {
  override readonly code: 'ERR_SYKE_SPOOL_LIMIT' = 'ERR_SYKE_SPOOL_LIMIT'
  override name = 'SpoolLimitError'
}

/**
 * Free space a spool must leave: a percentage (of the filesystem in Node, of
 * the origin's storage quota in the browser), a number of bytes, or `false`
 * for no floor.
 */
export type MinFreeSpace = `${number}%` | number | false

export type FreeSpaceFloor = { percent: number } | { bytes: number } | null

export function parseMinFreeSpace(value: MinFreeSpace): FreeSpaceFloor {
  if (value === false) return null

  if (typeof value === 'number') {
    // Any integer, as in 0.1.0: a floor past 2^53 bytes can never be met
    // either way, so clamp it rather than reject it.
    if (!Number.isInteger(value) || value < 0) {
      throw new RangeError(
        `syke: minFreeSpace must be a non-negative integer of bytes, a percentage like '10%', or false, got ${value}`,
      )
    }
    return { bytes: Math.min(value, Number.MAX_SAFE_INTEGER) }
  }

  // Not coerced: an object that stringifies to '10%' is a mistake. A
  // RangeError, as in 0.1.0 and the other option checks.
  if (typeof value !== 'string') {
    throw new RangeError(
      `syke: minFreeSpace must be a non-negative integer of bytes, a percentage like '10%', or false, got ${String(value)}`,
    )
  }
  const match = /^(\d+(?:\.\d+)?)%$/.exec(value)
  const percent = match ? Number(match[1]) : Number.NaN
  if (!(percent >= 0 && percent <= 100)) {
    throw new RangeError(
      `syke: minFreeSpace must be a percentage between '0%' and '100%', got '${value}'`,
    )
  }
  return { percent }
}

/** Bytes `floor` reserves out of `total`. */
export function reservedBytes(floor: { percent: number } | { bytes: number }, total: number): number {
  return 'bytes' in floor ? floor.bytes : (floor.percent / 100) * total
}
