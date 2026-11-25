import { describe, expect, expectTypeOf, it } from 'vitest'
import type {
  ByteSpoolOptions,
  Codec,
  ObjectSpoolOptions,
  Spool as SpoolClass,
  SpoolOptions,
  SpoolSource,
} from '../../../src/web.ts'

// Typechecked only (pnpm typecheck): every call below compiled against 0.1.0
// and must keep compiling. Nothing here runs.
const compat = (Spool: typeof SpoolClass, bytes: SpoolSource<Uint8Array>, strings: string[], flag: boolean) => {
  // A boolean objectMode, known only at runtime.
  expectTypeOf(Spool.from(bytes, { objectMode: flag })).toEqualTypeOf<SpoolClass<Uint8Array>>()
  expectTypeOf(Spool.from(strings, { objectMode: flag })).toEqualTypeOf<SpoolClass<string>>()
  expectTypeOf(new Spool({ objectMode: flag })).toEqualTypeOf<SpoolClass<Uint8Array>>()

  // Options passed through as a SpoolOptions value, or possibly undefined.
  const forward = (options: SpoolOptions) => Spool.from(bytes, options)
  expectTypeOf(forward).returns.toEqualTypeOf<SpoolClass<Uint8Array>>()
  const forwardOptional = (options?: SpoolOptions) => Spool.from(bytes, options)
  expectTypeOf(forwardOptional).returns.toEqualTypeOf<SpoolClass<Uint8Array>>()
  const construct = (options?: SpoolOptions<string>) => new Spool<string>(options)
  expectTypeOf(construct).returns.toEqualTypeOf<SpoolClass<string>>()

  // An explicit type argument, with a widened objectMode.
  const wide = { objectMode: true } as { objectMode: boolean }
  expectTypeOf(Spool.from<string>(strings, wide)).toEqualTypeOf<SpoolClass<string>>()
  expectTypeOf(Spool.from<string>(strings, { objectMode: true })).toEqualTypeOf<SpoolClass<string>>()

  // An explicit type argument and no options.
  expectTypeOf(Spool.from<Uint8Array>(bytes)).toEqualTypeOf<SpoolClass<Uint8Array>>()
  expectTypeOf(Spool.from<Uint8Array>(bytes, {})).toEqualTypeOf<SpoolClass<Uint8Array>>()
  expectTypeOf(new Spool<Uint8Array>()).toEqualTypeOf<SpoolClass<Uint8Array>>()
}

// An interface can extend SpoolOptions (it can't extend a union).
interface MyOptions extends SpoolOptions {
  label?: string
}
interface MyRecordOptions extends SpoolOptions<{ a: number }> {
  label?: string
}

const extended = (
  Spool: typeof SpoolClass,
  bytes: SpoolSource<Uint8Array>,
  records: { a: number }[],
  mine: MyOptions,
  mineRecords: MyRecordOptions,
) => {
  expectTypeOf(Spool.from(bytes, mine)).toEqualTypeOf<SpoolClass<Uint8Array>>()
  expectTypeOf(Spool.from(records, mineRecords)).toEqualTypeOf<SpoolClass<{ a: number }>>()
  expectTypeOf(new Spool(mine)).toEqualTypeOf<SpoolClass<Uint8Array>>()
}

// The narrow overloads still catch what fails at runtime.
const narrow = (Spool: typeof SpoolClass, bytes: SpoolSource<Uint8Array>, codec: Codec<{ a: number }>) => {
  // Inferred from the source, as in 0.1.0.
  expectTypeOf(Spool.from([new Uint8Array(1)])).toEqualTypeOf<SpoolClass<Uint8Array<ArrayBuffer>>>()
  expectTypeOf(Spool.from(bytes)).toEqualTypeOf<SpoolClass<Uint8Array>>()
  expectTypeOf(Spool.from([{ a: 1 }], { objectMode: true })).toEqualTypeOf<SpoolClass<{ a: number }>>()
  expectTypeOf(Spool.from([{ a: 1 }], { objectMode: true, codec })).toEqualTypeOf<SpoolClass<{ a: number }>>()
  // @ts-expect-error records without objectMode
  Spool.from([{ a: 1 }])
  // @ts-expect-error records with an explicit type argument but no options
  Spool.from<{ a: number }>([{ a: 1 }])
}

describe('Spool option types', () => {
  it('keeps the 0.1.0 SpoolOptions shape and default', () => {
    expectTypeOf<SpoolOptions>().toEqualTypeOf<SpoolOptions<Uint8Array>>()
    expectTypeOf<SpoolOptions['objectMode']>().toEqualTypeOf<boolean | undefined>()
    expectTypeOf<SpoolOptions<string>['codec']>().toEqualTypeOf<Codec<string> | undefined>()
    // The narrow option types are SpoolOptions too.
    expectTypeOf<ByteSpoolOptions>().toExtend<SpoolOptions>()
    expectTypeOf<ObjectSpoolOptions<string>>().toExtend<SpoolOptions<string>>()
    expect([compat, extended, narrow].every((fn) => typeof fn === 'function')).toBe(true)
  })
})
