import { Readable } from 'node:stream'
import { describe, expect, it } from 'vitest'
import { concat as nodeConcat } from '../../src/node.ts'
import { concat as webConcat } from '../../src/web.ts'
import { pick, runtimes } from '../harness.ts'
import {
  bystanders,
  collect,
  collectSettled,
  endedSource,
  endedSourceKinds,
  generator,
  probe,
  reader,
  readUpTo,
  settle,
  staysOpenAtEnd,
  untouched,
} from '../streamHarness.ts'

describe.each(runtimes)('concat (%s)', (runtime) => {
  const concat = pick(runtime, { node: nodeConcat, web: webConcat }) as (sources: unknown) => any

  it('emits each source in turn, whatever its kind', async () => {
    async function* later() {
      yield 'g1'
      yield 'g2'
    }
    const output = await collect(
      concat([
        probe(runtime, { length: 2, chunk: (i) => `s${i}` }).stream,
        [{ object: true }, 7],
        later(),
        () => ['f1'],
        async () => probe(runtime, { length: 1, chunk: () => 'p0' }).stream,
      ]),
    )
    expect(output).toEqual(['s0', 's1', { object: true }, 7, 'g1', 'g2', 'f1', 'p0'])
  })

  it('takes an async iterable of sources', async () => {
    async function* sources() {
      yield ['a']
      yield () => ['b']
    }
    expect(await collect(concat(sources()))).toEqual(['a', 'b'])
  })

  it('opens a factory only when its turn comes', async () => {
    let opened = false
    const first = probe(runtime, { length: 3 })
    const r = reader(
      concat([
        first.stream,
        () => {
          opened = true
          return ['next']
        },
      ]),
    )
    expect(await readUpTo(r, 2)).toEqual([0, 1])
    expect(opened).toBe(false)
    expect(await readUpTo(r, 10)).toEqual([2, 'next'])
    expect(opened).toBe(true)
  })

  it('ends at once for no sources, and skips empty ones', async () => {
    expect(await collect(concat([]))).toEqual([])
    expect(await collect(concat([[], probe(runtime, { length: 0 }).stream, [1], []]))).toEqual([1])
  })

  it('reads no faster than the consumer', async () => {
    const source = probe(runtime)
    const r = reader(concat([source.stream]))
    await readUpTo(r, 5)
    await settle()
    const pulls = source.pulls
    await settle()
    expect(source.pulls).toBe(pulls)
    expect(pulls).toBeLessThan(40)
    await r.cancel()
  })

  it('errors the output and cancels the open sources when one fails', async () => {
    const error = new Error('second failed')
    const third = probe(runtime)
    let factoryCalled = false
    const { output, error: caught } = await collectSettled(
      concat([
        [1],
        probe(runtime, { length: 5, failAt: 1, error }).stream,
        third.stream,
        () => {
          factoryCalled = true
          return [9]
        },
      ]),
    )
    expect(caught).toBe(error)
    expect(output[0]).toBe(1)
    await settle()
    expect(third.cancelled).toBe(true)
    expect(factoryCalled).toBe(false)
  })

  it('errors the output when a waiting source fails', async () => {
    const error = new Error('failed while waiting')
    const waiting = probe(runtime, { failAt: 0, error })
    const first = probe(runtime)
    const r = reader(concat([first.stream, waiting.stream]))
    // A stream starts pulling (and so fails) without being read.
    if (runtime === 'node') waiting.stream.read(0)
    await expect(readUpTo(r, 100)).rejects.toBe(error)
    await settle()
    expect(first.cancelled).toBe(true)
  })

  it('errors the output when a factory fails or returns no source', async () => {
    const error = new Error('no source')
    const { error: thrown } = await collectSettled(
      concat([
        [1],
        () => {
          throw error
        },
      ]),
    )
    expect(thrown).toBe(error)
    const { error: invalid } = await collectSettled(concat([() => 42]))
    expect(invalid).toBeInstanceOf(TypeError)
  })

  it('cancels the current source and the rest when the output is cancelled', async () => {
    const current = probe(runtime)
    const next = probe(runtime)
    const sources = generator(() => ['never'])
    const r = reader(concat([current.stream, next.stream]))
    await readUpTo(r, 3)
    await r.cancel()
    await settle()
    expect(current.cancelled).toBe(true)
    expect(next.cancelled).toBe(true)

    const lazy = reader(concat(sources))
    await readUpTo(lazy, 3)
    await lazy.cancel()
    await settle()
    expect(sources.returned).toBe(true)
  })

  it('rejects a locked ReadableStream before opening anything, leaving the other sources untouched', async () => {
    const b = bystanders()
    const locked = new ReadableStream()
    locked.getReader()
    expect(() => concat([...b.sources, () => ['lazy'], locked])).toThrow(/syke: source is a locked ReadableStream/)
    expect(await b.state()).toEqual(untouched)
  })

  it('hands the other sources back untouched when one cannot be opened', async () => {
    // The same stream twice: the second getReader() throws.
    const b = bystanders()
    const twice = new ReadableStream({
      start(controller) {
        controller.enqueue('t')
        controller.close()
      },
    })
    expect(() => concat([...b.sources, () => ['lazy'], twice, twice])).toThrow(TypeError)
    expect(await b.state()).toEqual(untouched)
    expect(await collect(twice)).toEqual(['t'])

    // An iterable whose iterator can't be made.
    const error = new Error('no iterator')
    const broken = {
      [Symbol.iterator]() {
        throw error
      },
    }
    const c = bystanders()
    expect(() => concat([...c.sources, () => ['lazy'], broken])).toThrow(error)
    expect(await c.state()).toEqual(untouched)
  })

  it('takes the items of a non-array iterable as they are reached, not up front', async () => {
    let taken = 0
    function* sources() {
      for (let i = 0; i < 100; i++) {
        taken++
        yield [i]
      }
    }
    const r = reader(concat(sources()))
    expect(await readUpTo(r, 1)).toEqual([0])
    // Only as far as the output's own read-ahead.
    expect(taken).toBeLessThan(20)
    expect(await readUpTo(r, 200)).toHaveLength(99)
    expect(taken).toBe(100)
    expect(await collect(concat(new Set([['x'], ['y']])))).toEqual(['x', 'y'])
  })

  it('releases, with the reason, a source a factory returns after the output was cancelled', async () => {
    let resolveFactory: (source: unknown) => void = () => {}
    const factory = () =>
      new Promise((resolve) => {
        resolveFactory = resolve
      })
    const r = reader(concat([[1], factory]))
    expect(await readUpTo(r, 10)).toEqual([1])
    const reason = new Error('stop')
    await r.cancel(reason)
    const reasons: unknown[] = []
    resolveFactory(
      new ReadableStream({
        cancel(why) {
          reasons.push(why)
        },
      }),
    )
    await settle()
    expect(reasons).toEqual([reason])
  })

  it('ignores a factory that returns no source after the output was cancelled', async () => {
    let resolveFactory: (source: unknown) => void = () => {}
    const r = reader(concat([[1], () => new Promise((resolve) => (resolveFactory = resolve))]))
    expect(await readUpTo(r, 10)).toEqual([1])
    await r.cancel()
    resolveFactory(42)
    await settle()
  })

  it('reads sources whose Symbol.asyncIterator is present but undefined', async () => {
    const odd = (items: unknown[]) => ({ [Symbol.asyncIterator]: undefined, *[Symbol.iterator]() { yield* items } })
    expect(await collect(concat(odd([odd([1, 2]), [3]])))).toEqual([1, 2, 3])
    expect(await collect(concat([odd(['a'])]))).toEqual(['a'])
  })

  it('rejects invalid sources', () => {
    expect(() => concat({ [Symbol.iterator]: undefined, [Symbol.asyncIterator]: 1 })).toThrow(TypeError)
    expect(() => concat(42)).toThrow(/syke: sources must be an iterable or async iterable/)
    expect(() => concat([[1], 'text'])).toThrow(/wrap it in an array/)
  })
})

describe('concat (node)', () => {
  it('errors the output, and stops the source, when a source yields null', async () => {
    const source = generator((i) => (i === 2 ? null : i))
    const { output, error } = await collectSettled(nodeConcat([source]))
    expect(output).toEqual([0, 1])
    expect(error).toBeInstanceOf(TypeError)
    expect(error.message).toMatch(/syke: a source produced null/)
    expect(error.code).toBe('ERR_STREAM_NULL_VALUES')
    await settle()
    expect(source.returned).toBe(true)
  })

  it('errors the output with an Error when a source fails with a falsy value', async () => {
    for (const value of [undefined, null, '', 0]) {
      async function* failing() {
        yield 1
        throw value
      }
      const { output, error } = await collectSettled(nodeConcat([failing()]))
      expect(output).toEqual([1])
      expect(error).toBeInstanceOf(Error)
      expect(error.message).toMatch(/syke: a source failed with/)
      expect(error.cause).toBe(value)
    }
  })

  it('emits in object mode', async () => {
    const output = nodeConcat([[Buffer.from('a')], ['b']])
    expect(output.readableObjectMode).toBe(true)
    expect(await collect(output)).toEqual([Buffer.from('a'), 'b'])
  })

  // Stopped from the source's own 'end' listener, which runs before concat
  // hears of the end: the source has ended, but is still the current one.
  describe.each(endedSourceKinds)('with a %s that has ended', (kind) => {
    // Some Node releases destroy a half-open Duplex themselves at its end.
    const test = it.skipIf(!staysOpenAtEnd[kind])

    test('leaves it undestroyed when a waiting source fails', async () => {
      const source = endedSource(kind)
      const waiting = new Readable({ objectMode: true, read() {} })
      const boom = new Error('waiting source failed')
      source.stream.once('end', () => waiting.destroy(boom))
      const { error } = await collectSettled(nodeConcat([source.stream, waiting]))
      expect(error).toBe(boom)
      await settle()
      expect(source.alive()).toBe(true)
      source.stream.destroy()
    })

    test('leaves it undestroyed when the output is destroyed', async () => {
      const source = endedSource(kind)
      const waiting = new Readable({ objectMode: true, read() {} })
      const output = nodeConcat([source.stream, waiting])
      source.stream.once('end', () => output.destroy())
      await collectSettled(output)
      await settle()
      expect(waiting.destroyed).toBe(true)
      expect(source.alive()).toBe(true)
      source.stream.destroy()
    })

    test('leaves alone a source that had already ended when it was passed in', async () => {
      const source = endedSource(kind)
      await collect(source.stream)
      const output = nodeConcat([[1], source.stream])
      const r = reader(output)
      expect(await readUpTo(r, 1)).toEqual([1])
      await r.cancel()
      expect(source.alive()).toBe(true)
      source.stream.destroy()
    })
  })
})

describe('concat (web)', () => {
  it('passes null chunks through', async () => {
    expect(await collect(webConcat([[1, null, 2]]))).toEqual([1, null, 2])
  })
})
