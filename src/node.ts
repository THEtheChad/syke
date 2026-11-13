// syke/node: utilities built on Node.js streams.
export { SykeError } from './shared/errors.ts'
export { LengthCheckError, type LengthCheckOptions } from './lengthCheck/core.ts'
export { LengthCheck } from './lengthCheck/node.ts'
export type { MinFreeSpace } from './spool/node/diskSpace.ts'
export { Spool, SpoolLimitError, type SpoolOptions, type SpoolSource } from './spool/node/Spool.ts'
