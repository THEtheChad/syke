// syke/node: utilities built on Node.js streams.
export { SykeError } from './shared/errors.ts'
export type { MinFreeSpace } from './spool/node/diskSpace.ts'
export { Spool, SpoolLimitError, type SpoolOptions, type SpoolSource } from './spool/node/Spool.ts'
