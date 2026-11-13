import { describe, expect, it } from 'vitest'
import { merge as nodeMerge } from '../../src/node.ts'
import { merge as webMerge } from '../../src/web.ts'
import { pick, runtimes } from '../harness.ts'
import { collect, collectSettled, generator, probe, reader, readUpTo, settle } from '../streamHarness.ts'

describe.each(runtimes)('merge (%s)', (runtime) => {
  const merge = pick(runtime, { node: nodeMerge, web: webMerge }) as (sources: unknown) => any

  it('emits every chunk of every source, in order within each, and ends when all have', async () => {
    const a = probe(runtime, { length: 50, chunk: (i) => ({ from: 'a', i }) })
    const b = probe(runtime, { length: 20, chunk: (i) => ({ from: 'b', i }) })
    const output = await collect<{ from: string; i: number }>(merge([a.stream, b.stream, [{ from: 'c', i: 0 }]]))
    expect(output).toHaveLength(71)
    for (const from of ['a', 'b', 'c']) {
      const own = output.filter((chunk) => chunk.from === from).map((chunk) => chunk.i)
      expect(own).toEqual([...own].sort((x, y) => x - y))
    }
  })

  it('interleaves chunks as they arrive, so a slow source does not hold back the others', async () => {
    let release = () => {}
    async function* slow() {
      yield 'slow-1'
      await new Promise<void>((resolve) => {
        release = resolve
      })
      yield 'slow-2'
    }
    const r = reader(merge([slow(), probe(runtime, { length: 5, chunk: (i) => `fast-${i}` }).stream]))
    const first = await readUpTo(r, 10)
    expect(first).toHaveLength(6)
    expect(first).toContain('slow-1')
    release()
    expect(await readUpTo(r, 10)).toEqual(['slow-2'])
    expect(await r.read()).toMatchObject({ done: true })
  })

  it('takes an async iterable of sources, which join as they arrive', async () => {
    async function* sources() {
      yield ['a']
      yield probe(runtime, { length: 2, chunk: (i) => `b${i}` }).stream
    }
    expect((await collect(merge(sources()))).sort()).toEqual(['a', 'b0', 'b1'])
  })

  it('ends at once for no sources, and handles empty ones', async () => {
    expect(await collect(merge([]))).toEqual([])
    expect(await collect(merge([[], probe(runtime, { length: 0 }).stream]))).toEqual([])
  })

  it('reads no faster than the consumer', async () => {
    const a = probe(runtime)
    const b = probe(runtime)
    const r = reader(merge([a.stream, b.stream]))
    await settle()
    const before = a.pulls + b.pulls
    expect(before).toBeLessThan(40)
    const read = await readUpTo(r, 100)
    expect(read).toHaveLength(100)
    await settle()
    const after = a.pulls + b.pulls
    await settle()
    expect(a.pulls + b.pulls).toBe(after)
    // About one pull per chunk read, plus what the buffers hold.
    expect(after).toBeLessThan(100 + 40)
    await r.cancel()
  })

  it('errors the output and cancels the rest when one source fails', async () => {
    const error = new Error('one failed')
    const healthy = probe(runtime)
    const { error: caught } = await collectSettled(
      merge([healthy.stream, probe(runtime, { length: 10, failAt: 3, error }).stream]),
    )
    expect(caught).toBe(error)
    await settle()
    expect(healthy.cancelled).toBe(true)
  })

  it('errors the output when the async iterable of sources fails, or yields no source', async () => {
    const error = new Error('no more sources')
    async function* failing() {
      yield [1]
      throw error
    }
    expect((await collectSettled(merge(failing()))).error).toBe(error)
    async function* invalid() {
      yield 42
    }
    expect((await collectSettled(merge(invalid()))).error).toBeInstanceOf(TypeError)
  })

  it('cancels every source when the output is cancelled', async () => {
    const a = probe(runtime)
    const b = generator()
    const r = reader(merge([a.stream, b]))
    await readUpTo(r, 10)
    await r.cancel()
    await settle()
    expect(a.cancelled).toBe(true)
    expect(b.returned).toBe(true)
  })

  it('rejects invalid sources', () => {
    expect(() => merge(42)).toThrow(/syke: sources must be an iterable or async iterable/)
    expect(() => merge([[1], 'text'])).toThrow(/wrap it in an array/)
  })
})

describe('merge (node)', () => {
  it('emits in object mode', async () => {
    const output = nodeMerge([[Buffer.from('a')]])
    expect(output.readableObjectMode).toBe(true)
    expect(await collect(output)).toEqual([Buffer.from('a')])
  })
})
