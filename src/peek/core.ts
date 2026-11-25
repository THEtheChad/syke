import { type OnError, type Outlet, type Producer, type Puller, describe } from '../shared/combinator/core.ts'
import { nonNegativeInt } from '../shared/validate.ts'

/**
 * Reads the first bytes of a source, then replays them, followed by the rest
 * of the source, as a new stream. Only the chunks that cover the head are
 * held; the rest stream through as the consumer pulls.
 */
export class PeekCore implements Producer<unknown> {
  readonly #source: Puller<unknown>
  /** Chunks read while peeking, replayed from `#replayed` on. */
  #replay: unknown[] = []
  #replayed = 0
  #outlet: Outlet<unknown> | null = null
  #peeking = true
  #finished = false
  /** A failure that came before the output stream was made. */
  #error: { reason: unknown } | null = null

  constructor(open: (onError: OnError) => Puller<unknown>) {
    this.#source = open((error) => this.#fail(error))
  }

  /** Reads until `bytes` bytes are buffered or the source ends, and returns a copy of them. */
  async peek(bytes: number): Promise<Uint8Array> {
    let total = 0
    try {
      while (total < bytes) {
        const result = await this.#source.next()
        if (result.done) break
        const chunk = result.value
        if (!isUint8Array(chunk)) {
          throw new TypeError(`syke: peek() reads bytes, but the source produced ${describe(chunk)}`)
        }
        this.#replay.push(chunk)
        total += chunk.byteLength
      }
    } catch (error) {
      this.#finished = true
      this.#source.cancel(error)
      throw error
    } finally {
      this.#peeking = false
    }
    const head = new Uint8Array(Math.min(bytes, total))
    let offset = 0
    for (const chunk of this.#replay as Uint8Array[]) {
      if (offset === head.byteLength) break
      const piece = chunk.subarray(0, head.byteLength - offset)
      head.set(piece, offset)
      offset += piece.byteLength
    }
    return head
  }

  start(outlet: Outlet<unknown>): void {
    this.#outlet = outlet
    if (this.#error) outlet.error(this.#error.reason)
  }

  async pull(): Promise<void> {
    if (this.#replayed < this.#replay.length) {
      const chunk = this.#replay[this.#replayed++]
      // Drop the replay once it's done, so its chunks can be collected.
      if (this.#replayed === this.#replay.length) this.#clearReplay()
      this.#outlet?.push(chunk)
      return
    }
    let result: IteratorResult<unknown, undefined>
    try {
      result = await this.#source.next()
    } catch (error) {
      this.#fail(error)
      return
    }
    if (this.#finished) return
    if (result.done) {
      this.#finished = true
      this.#outlet?.end()
    } else this.#outlet?.push(result.value)
  }

  cancel(reason: unknown): void {
    if (this.#finished) return
    this.#finished = true
    this.#clearReplay()
    this.#source.cancel(reason)
  }

  #fail(error: unknown) {
    // While peeking, the pending read rejects too, and peek() reports it.
    if (this.#peeking || this.#finished) return
    this.#finished = true
    this.#clearReplay()
    if (this.#outlet) this.#outlet.error(error)
    else this.#error = { reason: error }
  }

  #clearReplay() {
    this.#replay = []
    this.#replayed = 0
  }
}

/**
 * `instanceof Uint8Array` fails for one from another realm (a `vm` context,
 * an iframe or worker); the `toStringTag` getter works across realms, and is
 * `'Uint8Array'` for a Node `Buffer` too.
 */
function isUint8Array(value: unknown): value is Uint8Array {
  return ArrayBuffer.isView(value) && (value as Uint8Array)[Symbol.toStringTag] === 'Uint8Array'
}

export function peekBytes(bytes: number): number {
  return nonNegativeInt('bytes', bytes)
}
