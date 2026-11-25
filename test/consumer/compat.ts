// Compiled against the built declarations (dist/), as a consumer would:
// pnpm typecheck:dist, after pnpm build. Nothing here runs.
//
// Patterns that compiled against syke/web 0.1.0 and must keep compiling.
import { Digest, LengthCheck, type LengthCheckOptions, Lines, NdjsonParse, Rechunk } from '@thethechad/syke/web'

type Equals<X, Y> = (<T>() => T extends X ? 1 : 2) extends <T>() => T extends Y ? 1 : 2 ? true : false

declare const res: Response
declare const plainStream: ReadableStream<Uint8Array>
declare const sink: WritableStream<Uint8Array>

// The output of a checked response body, reassigned.
export function reassign(): ReadableStream<Uint8Array> {
  let cur = res.body!.pipeThrough(new LengthCheck({ max: 1024 }))
  cur = plainStream
  cur = cur.pipeThrough(new Rechunk({ size: 16 }))
  cur = cur.pipeThrough(new Digest({ algorithm: 'crc32' }))
  return cur
}
export const streams = [res.body!.pipeThrough(new LengthCheck({ max: 1024 }))]
streams.push(plainStream)

// A Uint8Array subtype.
interface Packet extends Uint8Array {
  tag: string
}
declare const packets: ReadableStream<Packet>
export function packet(): ReadableStream<Uint8Array> {
  let out = packets.pipeThrough(new LengthCheck({ max: 1024 }))
  out = plainStream
  return out
}

// An untyped ReadableStream (ReadableStream<any>), then untyped callbacks.
export async function untyped(): Promise<void> {
  const source = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(1))
      controller.close()
    },
  })
  const checked = source.pipeThrough(new LengthCheck({ max: 1024 }))
  const typed: ReadableStream<Uint8Array> = checked
  // Typed as in 0.1.0, not ReadableStream<any>.
  const notAny: Equals<typeof checked, ReadableStream<Uint8Array>> = true
  const mapped = checked.pipeThrough(
    new TransformStream({
      transform(chunk, controller) {
        controller.enqueue(chunk.byteLength)
      },
    }),
  )
  void mapped.pipeTo(
    new WritableStream({
      write(n) {
        n.toFixed()
      },
    }),
  )
  const reader = checked.getReader()
  const { value } = await reader.read()
  value?.byteLength
  ;[typed, notAny].map((s) => s)
  await checked.pipeTo(new WritableStream({ write: (chunk) => void chunk.byteLength }))
}

// Assigned to, piped and chained.
export async function positive(): Promise<void> {
  const out: ReadableStream<Uint8Array> = plainStream.pipeThrough(new LengthCheck({ expected: 4 }))
  const body: ReadableStream<Uint8Array> = res.body!.pipeThrough(new LengthCheck({ expected: 4 }))
  await body.pipeTo(sink)
  await res.body!.pipeThrough(new LengthCheck({ max: 4 })).pipeTo(sink)
  res.body!.pipeThrough(new LengthCheck({ max: 4 })).pipeThrough(new Rechunk({ size: 2 }))
  res.body!.pipeThrough(new LengthCheck({ max: 4 })).pipeThrough(new Digest({ algorithm: 'crc32' }))
  const lines: ReadableStream<string> = res.body!.pipeThrough(new LengthCheck({ max: 4 })).pipeThrough(new Lines())
  const records: ReadableStream<unknown> = out.pipeThrough(new LengthCheck({ max: 4 })).pipeThrough(new NdjsonParse())
  const pair: TransformStream<Uint8Array, Uint8Array> = new LengthCheck({ max: 4 })
  const annotated: LengthCheck = new LengthCheck({ max: 4 })
  const instance: InstanceType<typeof LengthCheck> = annotated
  const ctor: new (options: LengthCheckOptions) => TransformStream<Uint8Array, Uint8Array> = LengthCheck
  const unknownValue: unknown = annotated
  if (unknownValue instanceof LengthCheck) unknownValue.bytes.toFixed()
  void [lines, records, pair, instance, ctor]
}

// A generic helper over Uint8Array streams.
export function check<T extends Uint8Array>(source: ReadableStream<T>): ReadableStream<Uint8Array> {
  return source.pipeThrough(new LengthCheck({ max: 4 }))
}

// Subclassing.
export class CustomCheck extends LengthCheck {
  readonly label = 'custom'
}
export class FixedCheck extends LengthCheck {
  constructor() {
    super({ expected: 4 })
  }
  override get bytes(): number {
    return super.bytes
  }
}
export const C: typeof LengthCheck = CustomCheck
export const custom: LengthCheck = new CustomCheck({ max: 4 })
export const fixed: ReadableStream<Uint8Array> = plainStream.pipeThrough(new FixedCheck())
