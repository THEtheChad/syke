import { randomUUID } from 'node:crypto'
import { statfs } from 'node:fs/promises'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  diskFreed,
  diskFull,
  freeSpaceBudget,
  isNoSpaceError,
  memoryCheckCount,
  reserveGrowth,
  trackedDirCount,
  warnIfMemoryBacked,
} from '../../../src/spool/node/diskSpace.ts'

vi.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>()
  return { ...actual, statfs: vi.fn(actual.statfs) }
})

const EXT4 = 0xef53
const TMPFS = 0x01021994
const RAMFS = 0x858458f6

/** Results are cached per directory for the whole process, so each test gets its own. */
const freshDir = () => `/syke-test-${randomUUID()}`

function fakeStatfs(stats: { bavail: number; blocks: number; bsize: number; frsize?: number; type?: number }) {
  vi.mocked(statfs).mockImplementation((async () => ({
    type: EXT4,
    bfree: stats.bavail,
    files: 0,
    ffree: 0,
    ...stats,
  })) as never)
}

let now = 1_000_000
beforeEach(() => {
  vi.mocked(statfs).mockReset()
  vi.spyOn(Date, 'now').mockImplementation(() => now)
})
afterEach(() => {
  vi.restoreAllMocks()
})

describe('freeSpaceBudget()', () => {
  it('is unlimited without a floor, without asking the filesystem', async () => {
    expect(await freeSpaceBudget(freshDir(), null)).toBe(Number.POSITIVE_INFINITY)
    expect(statfs).not.toHaveBeenCalled()
  })

  it('subtracts the floor from the free space', async () => {
    fakeStatfs({ bavail: 500, blocks: 1000, bsize: 1024 })
    expect(await freeSpaceBudget(freshDir(), { bytes: 100 * 1024 })).toBe(400 * 1024)
    expect(await freeSpaceBudget(freshDir(), { percent: 10 })).toBe(400 * 1024)
  })

  it('counts blocks in f_frsize units where statfs reports one', async () => {
    fakeStatfs({ bavail: 500, blocks: 1000, bsize: 65536, frsize: 4096 })
    expect(await freeSpaceBudget(freshDir(), { bytes: 0 })).toBe(500 * 4096)
  })

  it.each([
    ['no blocks (ramfs)', { bavail: 0, blocks: 0, bsize: 4096, type: RAMFS }],
    ['a zero block size', { bavail: 10, blocks: 10, bsize: 0 }],
  ])('treats %s as unknown, not full', async (_name, stats) => {
    fakeStatfs(stats)
    expect(await freeSpaceBudget(freshDir(), { percent: 10 })).toBe(Number.POSITIVE_INFINITY)
  })

  it("is unlimited when free space can't be read", async () => {
    vi.mocked(statfs).mockRejectedValue(Object.assign(new Error('nope'), { code: 'ENOSYS' }))
    expect(await freeSpaceBudget(freshDir(), { bytes: 0 })).toBe(Number.POSITIVE_INFINITY)
  })

  it('reads the filesystem once per directory while the reading is fresh', async () => {
    fakeStatfs({ bavail: 1000, blocks: 1000, bsize: 1 })
    const dir = freshDir()
    await Promise.all([freeSpaceBudget(dir, { bytes: 0 }), freeSpaceBudget(dir, { bytes: 0 })])
    await freeSpaceBudget(dir, { bytes: 0 })
    expect(statfs).toHaveBeenCalledTimes(1)

    now += 1000
    await freeSpaceBudget(dir, { bytes: 0 })
    expect(statfs).toHaveBeenCalledTimes(2)
  })

  it('shares one reading between equivalent paths', async () => {
    fakeStatfs({ bavail: 1000, blocks: 1000, bsize: 1 })
    const dir = freshDir()
    await freeSpaceBudget(dir, { bytes: 0 })
    await freeSpaceBudget(`${dir}/./`, { bytes: 0 })
    expect(statfs).toHaveBeenCalledTimes(1)
  })
})

describe('growth accounting', () => {
  it('counts reserved growth against the budget of every spool in the directory', async () => {
    fakeStatfs({ bavail: 1000, blocks: 1000, bsize: 1 })
    const dir = freshDir()
    expect(await freeSpaceBudget(dir, { bytes: 0 })).toBe(1000)

    const first = reserveGrowth(dir, 300)
    const second = reserveGrowth(dir, 200)
    expect(await freeSpaceBudget(dir, { bytes: 0 })).toBe(500)

    // A write that grew the file by less than it reserved gives the rest back.
    first(100)
    expect(await freeSpaceBudget(dir, { bytes: 0 })).toBe(700)
    // One that grew it by all it reserved keeps it all counted.
    second(200)
    expect(await freeSpaceBudget(dir, { bytes: 0 })).toBe(700)
    // Settling twice changes nothing.
    second(0)
    expect(await freeSpaceBudget(dir, { bytes: 0 })).toBe(700)
  })

  it('keeps writes still in flight counted after a fresh reading', async () => {
    fakeStatfs({ bavail: 1000, blocks: 1000, bsize: 1 })
    const dir = freshDir()
    await freeSpaceBudget(dir, { bytes: 0 })
    const landed = reserveGrowth(dir, 100)
    const inFlight = reserveGrowth(dir, 50)
    landed(100)

    // The new reading includes the landed write (the fake doesn't, so the
    // budget shows just the in-flight one held back).
    now += 1000
    expect(await freeSpaceBudget(dir, { bytes: 0 })).toBe(950)
    inFlight(50)
    expect(await freeSpaceBudget(dir, { bytes: 0 })).toBe(950)
  })

  it('treats the filesystem as full after ENOSPC, until the next reading', async () => {
    fakeStatfs({ bavail: 1000, blocks: 1000, bsize: 1 })
    const dir = freshDir()
    await freeSpaceBudget(dir, { bytes: 0 })
    diskFull(dir)
    expect(await freeSpaceBudget(dir, { bytes: 0 })).toBeLessThanOrEqual(0)
    now += 1000
    expect(await freeSpaceBudget(dir, { bytes: 0 })).toBe(1000)
  })

  it('credits space a spool gave back by truncating', async () => {
    fakeStatfs({ bavail: 1000, blocks: 1000, bsize: 1 })
    const dir = freshDir()
    await freeSpaceBudget(dir, { bytes: 0 })
    reserveGrowth(dir, 400)(400)
    expect(await freeSpaceBudget(dir, { bytes: 0 })).toBe(600)
    diskFreed(dir, 400)
    expect(await freeSpaceBudget(dir, { bytes: 0 })).toBe(1000)
  })

  it('ignores growth in a directory it has not read', () => {
    expect(() => reserveGrowth(freshDir(), 0)(0)).not.toThrow()
    expect(() => diskFreed(freshDir(), 10)).not.toThrow()
  })
})

describe('directory bookkeeping', () => {
  it('forgets directories once their reading is stale and nothing is in flight', async () => {
    fakeStatfs({ bavail: 1000, blocks: 1000, bsize: 1 })
    for (let i = 0; i < 1000; i++) {
      const dir = freshDir()
      await freeSpaceBudget(dir, { bytes: 0 })
      reserveGrowth(dir, 10)(10)
      diskFreed(dir, 10)
      now += 1000
    }
    expect(trackedDirCount()).toBeLessThanOrEqual(32)
  })

  it('keeps a directory with a write in flight, so its growth still counts', async () => {
    fakeStatfs({ bavail: 1000, blocks: 1000, bsize: 1 })
    const busy = freshDir()
    await freeSpaceBudget(busy, { bytes: 0 })
    const settle = reserveGrowth(busy, 300)
    now += 1000
    for (let i = 0; i < 100; i++) await freeSpaceBudget(freshDir(), { bytes: 0 })
    // A fresh reading doesn't include the write still in flight.
    expect(await freeSpaceBudget(busy, { bytes: 0 })).toBe(700)
    settle(300)
  })
})

describe('isNoSpaceError()', () => {
  it.each([
    ['ENOSPC', true],
    ['EDQUOT', true],
    ['EIO', false],
  ])('%s → %s', (code, expected) => {
    expect(isNoSpaceError(Object.assign(new Error(code), { code }))).toBe(expected)
  })

  it('handles non-errors', () => {
    expect(isNoSpaceError(null)).toBe(false)
    expect(isNoSpaceError('ENOSPC')).toBe(false)
  })
})

describe.runIf(process.platform === 'linux')('warnIfMemoryBacked()', () => {
  it('checks each directory once, even when asked concurrently, and warns once', async () => {
    fakeStatfs({ bavail: 1, blocks: 1, bsize: 1, type: TMPFS })
    const warn = vi.spyOn(process, 'emitWarning').mockImplementation(() => {})
    const dir = freshDir()
    await Promise.all([warnIfMemoryBacked(dir), warnIfMemoryBacked(dir), warnIfMemoryBacked(`${dir}/`)])
    await warnIfMemoryBacked(dir)
    expect(statfs).toHaveBeenCalledTimes(1)
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn.mock.calls[0]?.[1]).toMatchObject({ code: 'SYKE_SPOOL_TMPFS' })
  })

  it('remembers a directory on a real disk too, without warning', async () => {
    fakeStatfs({ bavail: 1, blocks: 1, bsize: 1, type: EXT4 })
    const warn = vi.spyOn(process, 'emitWarning').mockImplementation(() => {})
    const dir = freshDir()
    await warnIfMemoryBacked(dir)
    await warnIfMemoryBacked(dir)
    expect(statfs).toHaveBeenCalledTimes(1)
    expect(warn).not.toHaveBeenCalled()
  })

  it('remembers only the most recently used directories', async () => {
    fakeStatfs({ bavail: 1, blocks: 1, bsize: 1, type: EXT4 })
    const first = freshDir()
    const kept = freshDir()
    await warnIfMemoryBacked(first)
    await warnIfMemoryBacked(kept)
    for (let i = 0; i < 200; i++) {
      await warnIfMemoryBacked(freshDir())
      await warnIfMemoryBacked(kept)
    }
    expect(memoryCheckCount()).toBeLessThanOrEqual(64)
    vi.mocked(statfs).mockClear()
    await warnIfMemoryBacked(kept)
    expect(statfs).not.toHaveBeenCalled()
    await warnIfMemoryBacked(first)
    expect(statfs).toHaveBeenCalledTimes(1)
  })

  it("asks again when the directory couldn't be checked", async () => {
    vi.mocked(statfs).mockRejectedValue(Object.assign(new Error('nope'), { code: 'ENOENT' }))
    const dir = freshDir()
    await warnIfMemoryBacked(dir)
    await warnIfMemoryBacked(dir)
    expect(statfs).toHaveBeenCalledTimes(2)
  })
})
