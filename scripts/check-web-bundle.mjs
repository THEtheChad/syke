// Fails if the built syke/web entry (or any chunk it loads) touches Node.js:
// a `node:` import, a bare built-in like 'fs', or the Buffer/process globals.
import { readFileSync } from 'node:fs'
import { builtinModules } from 'node:module'
import { dirname, join } from 'node:path'

const builtins = new Set(builtinModules.flatMap((name) => [name, `node:${name}`]))
const problems = []

for (const entry of ['dist/web.mjs', 'dist/web.cjs']) {
  const seen = new Set()
  const queue = [entry]
  while (queue.length > 0) {
    const file = queue.pop()
    if (seen.has(file)) continue
    seen.add(file)
    const code = readFileSync(file, 'utf8')
    for (const [, specifier] of code.matchAll(/(?:\bfrom\s*|\bimport\s*\(?\s*|\brequire\s*\(\s*)["']([^"']+)["']/g)) {
      if (specifier.startsWith('.')) queue.push(join(dirname(file), specifier))
      else if (builtins.has(specifier)) problems.push(`${file} imports ${specifier}`)
    }
    for (const global of ['Buffer', 'process']) {
      if (new RegExp(`(?<![\\w.$])${global}\\s*[.(\\[]`).test(code)) problems.push(`${file} uses ${global}`)
    }
  }
}

if (problems.length > 0) {
  console.error(`syke/web must run without Node.js:\n  ${problems.join('\n  ')}`)
  process.exit(1)
}
console.log('syke/web bundle is free of Node.js APIs')
