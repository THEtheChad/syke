import { assertBytes } from '../shared/bytes.ts'
import type { Sink, Transformer } from '../shared/transformer.ts'
import { positiveInt } from '../shared/validate.ts'

export interface RechunkOptions {
  /** Bytes per output chunk. Only the last chunk may be shorter. */
  size: number
}

export class RechunkCore implements Transformer<Uint8Array, Uint8Array> {
  readonly #size: number
  readonly #alloc: (size: number) => Uint8Array
  // Bytes carried over from earlier input, coalescing into the next chunk.
  // Always a buffer of our own, never one that has been handed downstream.
  #pending: Uint8Array | null = null
  #filled = 0

  /** `alloc` makes coalescing buffers; Node passes one that returns a `Buffer`. */
  constructor({ size }: RechunkOptions, alloc: (size: number) => Uint8Array = (n) => new Uint8Array(n)) {
    this.#size = positiveInt('size', size)
    this.#alloc = alloc
  }

  transform(chunk: Uint8Array, sink: Sink<Uint8Array>): void {
    assertBytes('Rechunk', chunk)
    const size = this.#size
    let offset = 0
    if (this.#pending) {
      offset = Math.min(size - this.#filled, chunk.byteLength)
      this.#pending.set(chunk.subarray(0, offset), this.#filled)
      this.#filled += offset
      if (this.#filled < size) return
      // Hand the buffer over for good; the next remainder gets a fresh one.
      sink.push(this.#pending)
      this.#pending = null
      this.#filled = 0
    }
    // Whole output chunks inside this input chunk go out as views, uncopied.
    for (; chunk.byteLength - offset >= size; offset += size) sink.push(chunk.subarray(offset, offset + size))
    if (offset < chunk.byteLength) {
      // Copy the remainder now rather than hold a view: the producer may reuse its buffer.
      this.#pending = this.#alloc(size)
      this.#pending.set(chunk.subarray(offset))
      this.#filled = chunk.byteLength - offset
    }
  }

  flush(sink: Sink<Uint8Array>): void {
    if (this.#pending) sink.push(this.#pending.subarray(0, this.#filled))
    this.#pending = null
  }

  close(): void {
    this.#pending = null
  }
}
