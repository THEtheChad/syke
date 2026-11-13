/**
 * Base class for every error syke raises on purpose. Each subclass sets a
 * stable `code` (`ERR_SYKE_*`), so callers can branch on `error.code`
 * without importing the class.
 */
export class SykeError extends Error {
  readonly code: string = 'ERR_SYKE'
  override name = 'SykeError'
}
