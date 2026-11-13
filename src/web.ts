// syke/web: utilities built on Web Streams, for browsers, Deno, Bun and Node.
export { SykeError } from './shared/errors.ts'
export type { Hasher, DigestEncoding } from './digest/core.ts'
export { DigestError } from './digest/core.ts'
export { Digest, type DigestOptions } from './digest/web.ts'
export { LengthCheckError, type LengthCheckOptions } from './lengthCheck/core.ts'
export { LengthCheck } from './lengthCheck/web.ts'
export { LineTooLongError, type LinesOptions } from './lines/core.ts'
export { Lines } from './lines/web.ts'
export {
  type JsonReplacer,
  type JsonReviver,
  NdjsonParseError,
  type NdjsonParseOptions,
  NdjsonStringifyError,
  type NdjsonStringifyOptions,
} from './ndjson/core.ts'
export { NdjsonParse, NdjsonStringify } from './ndjson/web.ts'
export type { BatchOptions } from './batch/core.ts'
export { Batch } from './batch/web.ts'
export type { ParallelMapContext, ParallelMapFn, ParallelMapOptions } from './parallelMap/core.ts'
export { ParallelMap } from './parallelMap/web.ts'
