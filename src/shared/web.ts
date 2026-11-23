import { type Sink, type Transformer as SykeTransformer, toFailure } from './transformer.ts'

/** A web `TransformStream` driven by a runtime-neutral `Transformer`. */
export class CoreTransformStream<I, O> extends TransformStream<I, O> {
  constructor(
    core: SykeTransformer<I, O>,
    writableStrategy?: QueuingStrategy<I>,
    readableStrategy?: QueuingStrategy<O>,
  ) {
    let controller: TransformStreamDefaultController<O>
    let ended = false
    let closed = false
    const close = () => {
      if (closed) return
      closed = true
      core.close?.()
    }
    // Runs a step, closing the transformer if it fails.
    const guard = async (step: () => void | Promise<void>) => {
      try {
        await step()
      } catch (error) {
        close()
        // Same failure as in Node, where a falsy error would read as success.
        throw toFailure(error)
      }
    }

    const sink: Sink<O> = {
      push(chunk) {
        if (ended || closed) return
        try {
          controller.enqueue(chunk)
        } catch {
          // The readable side was cancelled; nothing downstream wants more.
          close()
        }
      },
      end() {
        if (ended) return
        ended = true
        close()
        controller.terminate()
      },
      error(error) {
        close()
        controller.error(toFailure(error))
      },
      get full() {
        return (controller.desiredSize ?? 1) <= 0
      },
    }

    super(
      {
        start(c) {
          controller = c
          core.start?.(sink)
        },
        transform(chunk) {
          if (ended) return
          return guard(() => core.transform(chunk, sink))
        },
        async flush() {
          if (ended) return
          await guard(() => core.flush?.(sink))
          close()
        },
        // Newer engines call this when the readable side is cancelled.
        cancel() {
          close()
        },
      } as WebTransformer<I, O>,
      writableStrategy,
      readableStrategy,
    )
  }
}

// `cancel` is newer than some lib.dom versions; widen the transformer type.
type WebTransformer<I, O> = Transformer<I, O> & { cancel?: (reason: unknown) => void }
