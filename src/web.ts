// syke/web: utilities built on Web Streams, for browsers, Deno, Bun and Node.
export { SykeError } from './shared/errors.ts'
export type { Hasher, DigestEncoding } from './digest/core.ts'
export { DigestError } from './digest/core.ts'
export { Digest, type DigestOptions } from './digest/web.ts'
export { LengthCheckError, type LengthCheckOptions } from './lengthCheck/core.ts'
export { LengthCheck } from './lengthCheck/web.ts'
export { LineTooLongError, type LinesOptions } from './lines/core.ts'
export { Lines } from './lines/web.ts'
