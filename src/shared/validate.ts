/**
 * A positive integer. One of 2^53 or more stops being exact, but no count ever
 * gets that far, so it means "unlimited" and comes back as
 * `Number.MAX_SAFE_INTEGER`, which keeps comparisons and subtraction exact.
 * `Infinity` is not an integer and is rejected.
 */
export function positiveInt(name: string, value: number): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new RangeError(`syke: ${name} must be a positive integer, got ${value}`)
  }
  return Math.min(value, Number.MAX_SAFE_INTEGER)
}

/** A non-negative integer, with large values clamped as in `positiveInt`. */
export function nonNegativeInt(name: string, value: number): number {
  if (!Number.isInteger(value) || value < 0) {
    throw new RangeError(`syke: ${name} must be a non-negative integer, got ${value}`)
  }
  return Math.min(value, Number.MAX_SAFE_INTEGER)
}

export function positiveNumber(name: string, value: number): number {
  if (!(typeof value === 'number' && value > 0 && Number.isFinite(value))) {
    throw new RangeError(`syke: ${name} must be a positive number, got ${value}`)
  }
  return value
}

export function oneOf<const T extends string>(name: string, value: T, allowed: readonly T[]): T {
  if (!allowed.includes(value)) {
    const list = allowed.map((option) => `'${option}'`).join(' or ')
    throw new RangeError(`syke: ${name} must be ${list}, got ${String(value)}`)
  }
  return value
}
