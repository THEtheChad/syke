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

describe('ExtentMap edge cases', () => {
  it('places nothing for a zero or negative want', () => {
    const map = new ExtentMap(10)
    expect(map.place(0, Number.POSITIVE_INFINITY, true)).toBeNull()
    expect(map.place(-5, Number.POSITIVE_INFINITY, true)).toBeNull()
    expect(map.fileSize).toBe(0)
  })

  it('treats zero-length writes and reads as no-ops', () => {
    const map = new ExtentMap(10)
    map.wrote(0, 0)
    expect(map.empty).toBe(true)
    expect(map.fileSize).toBe(0)
    write(map, 5)
    map.read(0)
    expect(map.unread).toBe(5)
  })

  it('rejects negative writes and reads', () => {
    const map = new ExtentMap(10)
    expect(() => map.wrote(0, -1)).toThrow(RangeError)
    write(map, 5)
    expect(() => map.read(-1)).toThrow(RangeError)
    expect(map.unread).toBe(5)
  })

  it('resets the file size only once everything is read', () => {
    const map = new ExtentMap(10)
    write(map, 25)
    expect(() => map.reset()).toThrow(RangeError)
    map.read(10)
    map.read(10)
    map.read(5)
    map.reset()
    expect(map.fileSize).toBe(0)
    // Writes start again from the beginning of the (truncated) file.
    expect(write(map, 5)).toEqual({ position: 0, length: 5 })
  })

  it('finds gaps among more than two extents out of file order', () => {
    const map = new ExtentMap(1, 30)
    write(map, 30)
    map.read(10)
    // Wraps into 0..10, leaving extents 10..30 then 0..4 in stream order.
    expect(write(map, 4)).toEqual({ position: 0, length: 4 })
    map.read(10)
    // Three extents: 20..30, 0..4 and a new one in the gap at 4..14.
    expect(map.place(10, 0, false)).toEqual({ position: 4, length: 10 })
    map.wrote(4, 10)
    map.read(10)
    expect(map.nextRead()).toEqual({ position: 0, length: 1 })
    expect(map.unread).toBe(14)
  })

  it('keeps stream order through many wrap-arounds (model check)', () => {
    // Simulates a file of sequence numbers: whatever the placement, reads
    // must return 0, 1, 2, ... in order and never overwrite unread bytes.
    let seed = 42
    const random = (n: number) => {
      seed = (seed * 1_103_515_245 + 12_345) % 2 ** 31
      return seed % n
    }
    for (const [chunkSize, maxBytes] of [
      [7, 50],
      [16, 200],
      [1, 13],
    ] as const) {
      const map = new ExtentMap(chunkSize, maxBytes)
      const file: (number | undefined)[] = []
      let nextWrite = 0
      let nextRead = 0
      for (let step = 0; step < 5000; step++) {
        if (random(2) === 0) {
          const region = map.place(1 + random(30), random(40), random(4) === 0)
          if (!region) continue
          for (let i = 0; i < region.length; i++) {
            expect(file[region.position + i]).toBeUndefined()
            file[region.position + i] = nextWrite++
          }
          map.wrote(region.position, region.length)
          expect(map.fileSize).toBeLessThanOrEqual(maxBytes)
        } else {
          const region = map.nextRead()
          if (!region) continue
          const length = 1 + random(region.length)
          for (let i = 0; i < length; i++) {
            expect(file[region.position + i]).toBe(nextRead++)
            file[region.position + i] = undefined
          }
          map.read(length)
        }
      }
      expect(map.unread).toBe(nextWrite - nextRead)
    }
  })
})
