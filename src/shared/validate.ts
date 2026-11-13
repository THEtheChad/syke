export function positiveInt(name: string, value: number): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new RangeError(`syke: ${name} must be a positive integer, got ${value}`)
  }
  return value
}

export function nonNegativeInt(name: string, value: number): number {
  if (!Number.isInteger(value) || value < 0) {
    throw new RangeError(`syke: ${name} must be a non-negative integer, got ${value}`)
  }
  return value
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
