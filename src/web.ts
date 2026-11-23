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
export type { StreamSource } from './shared/combinator/web.ts'
export { fork, type ForkOptions } from './fork/web.ts'
export { peek, type PeekResult } from './peek/web.ts'
export { concat, type ConcatSource } from './concat/web.ts'
export { merge } from './merge/web.ts'
export { CapError, type CapLimitAction, type CapOptions } from './cap/core.ts'
export { Cap } from './cap/web.ts'
export type { RechunkOptions } from './rechunk/core.ts'
export { Rechunk } from './rechunk/web.ts'
export type { MeterOptions, MeterStats } from './meter/core.ts'
export { Meter } from './meter/web.ts'
export { WatchdogError, type WatchdogOptions } from './watchdog/core.ts'
export { Watchdog } from './watchdog/web.ts'
export type { ThrottleOptions } from './throttle/core.ts'
export { Throttle } from './throttle/web.ts'
export { SpoolLimitError, type MinFreeSpace } from './spool/core/limits.ts'
export { type Codec, jsonCodec, SpoolCodecError } from './spool/core/records.ts'
export {
  Spool,
  type ByteSpoolOptions,
  type ObjectSpoolOptions,
  type SpoolBaseOptions,
  type SpoolOptions,
  type SpoolSource,
} from './spool/web/Spool.ts'
export type { SpoolStorageChoice } from './spool/web/openStore.ts'
export { SpoolStorageError, type SpoolStorage } from './spool/web/store.ts'
