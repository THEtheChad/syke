import { describe, expect, it } from 'vitest'
import { ExtentMap } from '../../../src/spool/core/extents.ts'

/** Places and records a write of `want` bytes, returning where it went. */
function write(map: ExtentMap, want: number, growth = Number.POSITIVE_INFINITY) {
  const region = map.place(want, growth, false) ?? map.place(want, growth, true)
  if (region) map.wrote(region.position, region.length)
  return region
}

describe('ExtentMap', () => {
  it('appends to the file while nothing has been read', () => {
    const map = new ExtentMap(10)
    expect(write(map, 25)).toEqual({ position: 0, length: 25 })
    expect(write(map, 5)).toEqual({ position: 25, length: 5 })
    expect(map.fileSize).toBe(30)
    expect(map.unread).toBe(30)
  })

  it('reads in chunkSize pieces from the oldest data', () => {
    const map = new ExtentMap(10)
    write(map, 25)
    expect(map.nextRead()).toEqual({ position: 0, length: 10 })
    map.read(10)
    map.read(10)
    expect(map.nextRead()).toEqual({ position: 20, length: 5 })
    map.read(5)
    expect(map.empty).toBe(true)
    expect(map.nextRead()).toBeNull()
  })

  it('reuses space the reader has freed once growth is capped', () => {
    const map = new ExtentMap(10, 30)
    write(map, 30)
    map.read(10)
    // The file is at maxBytes; the freed first 10 bytes are reused.
    expect(write(map, 10)).toEqual({ position: 0, length: 10 })
    expect(map.fileSize).toBe(30)
    // Stream order: the remaining 20 bytes at 10..30, then the wrapped 10 at 0..10.
    map.read(10)
    map.read(10)
    expect(map.nextRead()).toEqual({ position: 0, length: 10 })
  })

  it('prefers a free gap to growing the file', () => {
    const map = new ExtentMap(10)
    write(map, 40)
    map.read(10)
    map.read(10)
    // No growth allowed: the 20 freed bytes at the start are used.
    expect(map.place(15, 0, false)).toEqual({ position: 0, length: 15 })
  })

  it('skips gaps smaller than chunkSize unless any size will do', () => {
    const map = new ExtentMap(10, 40)
    write(map, 40)
    map.read(5)
    expect(map.place(20, 0, false)).toBeNull()
    expect(map.place(20, 0, true)).toEqual({ position: 0, length: 5 })
  })

  it('extends the tail when a write lands right after it', () => {
    const map = new ExtentMap(100)
    write(map, 10)
    write(map, 10)
    expect(map.nextRead()).toEqual({ position: 0, length: 20 })
  })

  it('reports when growth would reach maxBytes', () => {
    const map = new ExtentMap(10, 50)
    write(map, 40)
    expect(map.atMaxSize).toBe(false)
    expect(map.wouldReachMax(5)).toBe(false)
    expect(map.wouldReachMax(10)).toBe(true)
  })

  it('refuses to read past the unread data', () => {
    const map = new ExtentMap(10)
    write(map, 5)
    expect(() => map.read(6)).toThrow(RangeError)
  })
})
