import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Batch as NodeBatch } from '../../src/node.ts'
import { Batch as WebBatch } from '../../src/web.ts'
import { BatchCore } from '../../src/batch/core.ts'
import { pick, run, runSettled, runtimes } from '../harness.ts'
import { drive } from '../streamDriver.ts'

const range = (n: number) => Array.from({ length: n }, (_, i) => i)

describe.each(runtimes)('Batch (%s)', (runtime) => {
  const Batch = pick(runtime, { node: NodeBatch, web: WebBatch })

  it('groups records into batches of size, with the last partial batch on flush', async () => {
    expect(await run(new Batch({ size: 3 }), range(8))).toEqual([[0, 1, 2], [3, 4, 5], [6, 7]])
  })

  it('emits no empty batch when the input divides evenly', async () => {
    expect(await run(new Batch({ size: 2 }), range(4))).toEqual([
      [0, 1],
      [2, 3],
    ])
  })

  it('emits nothing for empty input', async () => {
    expect(await run(new Batch({ size: 3 }), [])).toEqual([])
  })

  it('emits single-record batches with size 1', async () => {
    expect(await run(new Batch({ size: 1, maxWait: 10 }), range(3))).toEqual([[0], [1], [2]])
  })

  it('carries any record, including falsy ones', async () => {
    expect(await run(new Batch({ size: 2 }), [0, '', false, { a: 1 }])).toEqual([
      [0, ''],
      [false, { a: 1 }],
    ])
  })

  it('passes a source error through', async () => {
    async function* failing() {
      yield 1
      throw new Error('boom')
    }
    const { error } = await runSettled(new Batch({ size: 2, maxWait: 1000 }), failing())
    expect(error).toMatchObject({ message: 'boom' })
  })

  it('rejects invalid options', () => {
    // @ts-expect-error size is required
    expect(() => new Batch({})).toThrow(TypeError)
    // @ts-expect-error options are required
    expect(() => new Batch()).toThrow(TypeError)
    expect(() => new Batch({ size: 0 })).toThrow(RangeError)
    expect(() => new Batch({ size: 2.5 })).toThrow(RangeError)
    expect(() => new Batch({ size: 2, maxWait: 0 })).toThrow(RangeError)
    expect(() => new Batch({ size: 2, maxWait: -5 })).toThrow(RangeError)
    expect(() => new Batch({ size: 2, maxWait: Number.NaN })).toThrow(RangeError)
  })

  describe('maxWait', () => {
    beforeEach(() => {
      vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
    })
    afterEach(() => {
      vi.useRealTimers()
    })

    it("emits a partial batch maxWait ms after the batch's first record", async () => {
      const stream = drive<number[]>(runtime, new Batch({ size: 5, maxWait: 100 }))
      await stream.write(1)
      await vi.advanceTimersByTimeAsync(60)
      await stream.write(2)
      await vi.advanceTimersByTimeAsync(39)
      expect(stream.output).toEqual([])
      await vi.advanceTimersByTimeAsync(1)
      expect(stream.output).toEqual([[1, 2]])

      // The next batch's clock starts at its own first record.
      await vi.advanceTimersByTimeAsync(500)
      await stream.write(3)
      await vi.advanceTimersByTimeAsync(99)
      expect(stream.output).toEqual([[1, 2]])
      await vi.advanceTimersByTimeAsync(1)
      expect(stream.output).toEqual([[1, 2], [3]])

      await stream.end()
      expect(await stream.done).toBeUndefined()
      expect(stream.output).toEqual([[1, 2], [3]])
    })

    it('cancels the timer when a batch fills up', async () => {
      const stream = drive<number[]>(runtime, new Batch({ size: 2, maxWait: 100 }))
      await stream.write(1)
      await stream.write(2)
      expect(stream.output).toEqual([[1, 2]])
      expect(vi.getTimerCount()).toBe(0)
      await vi.advanceTimersByTimeAsync(1000)
      expect(stream.output).toEqual([[1, 2]])
      await stream.end()
      await stream.done
    })

    it('clears the timer on flush', async () => {
      const stream = drive<number[]>(runtime, new Batch({ size: 5, maxWait: 100 }))
      await stream.write(1)
      expect(vi.getTimerCount()).toBe(1)
      await stream.end()
      await stream.done
      expect(stream.output).toEqual([[1]])
      expect(vi.getTimerCount()).toBe(0)
    })

    it('waits out a maxWait longer than setTimeout can take', async () => {
      const day = 24 * 60 * 60 * 1000
      const stream = drive<number[]>(runtime, new Batch({ size: 5, maxWait: 60 * day }))
      await stream.write(1)
      // An unclamped timer would wrap around and fire after 1 ms.
      await vi.advanceTimersByTimeAsync(1000)
      expect(stream.output).toEqual([])
      await vi.advanceTimersByTimeAsync(60 * day - 1001)
      expect(stream.output).toEqual([])
      await vi.advanceTimersByTimeAsync(1)
      expect(stream.output).toEqual([[1]])
      expect(vi.getTimerCount()).toBe(0)
      await stream.end()
      await stream.done
    })

    it('clears the timer when the stream is aborted', async () => {
      const stream = drive<number[]>(runtime, new Batch({ size: 5, maxWait: 100 }))
      await stream.write(1)
      expect(vi.getTimerCount()).toBe(1)
      stream.abort(new Error('stop'))
      await stream.done
      expect(vi.getTimerCount()).toBe(0)
      await vi.advanceTimersByTimeAsync(1000)
      expect(stream.output).toEqual([])
    })
  })
})

describe('BatchCore', () => {
  it('drops a partial batch when closed, rather than holding it', () => {
    const pushed: number[][] = []
    const sink = { push: (batch: number[]) => pushed.push(batch), end() {}, error() {}, full: false }
    const core = new BatchCore<number>({ size: 10 })
    core.start(sink)
    core.transform(1)
    core.transform(2)
    core.close()
    core.flush()
    expect(pushed).toEqual([])
  })
})

describe('Batch (node)', () => {
  it('forwards only highWaterMark and signal to the Node stream', () => {
    const batch = new NodeBatch({ size: 2, highWaterMark: 4, objectMode: false } as never)
    expect(batch.writableObjectMode).toBe(true)
    expect(batch.readableObjectMode).toBe(true)
    expect(batch.writableHighWaterMark).toBe(4)
  })
})
