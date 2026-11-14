// Runs test/browser (syke/web's Spool against real OPFS and IndexedDB) in
// headless browsers. Each page POSTs its results back to this server.
//
//   pnpm test:browser                    every browser found
//   pnpm test:browser chrome firefox     just those
//   pnpm test:browser --serve            only serve; open the printed URL yourself
//
// Browser executables come from SYKE_EDGE, SYKE_CHROME and SYKE_FIREFOX, or
// default to Windows installs reached from WSL (where these tests were run).
// Results are written to .browser-test/results/<browser>.json.
import { execFileSync, spawn } from 'node:child_process'
import { existsSync } from 'node:fs'
import { mkdir, readFile, rm, writeFile } from 'node:fs/promises'
import { createServer } from 'node:http'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { extname, join, resolve } from 'node:path'

const root = resolve(import.meta.dirname, '..')
const out = join(root, '.browser-test')
const PAGE_TIMEOUT_MS = 10 * 60 * 1000

const WINDOWS = '/mnt/c/Windows/System32'
const POWERSHELL = `${WINDOWS}/WindowsPowerShell/v1.0/powershell.exe`
const windowsLocalAppData = () =>
  existsSync(`${WINDOWS}/cmd.exe`)
    ? wslPath(execFileSync(`${WINDOWS}/cmd.exe`, ['/c', 'echo %LOCALAPPDATA%'], { cwd: '/mnt/c', encoding: 'utf8' }).trim())
    : null

const chromiumArgs = (profile, url, extra = []) => [
  '--headless=new',
  `--user-data-dir=${profile}`,
  '--no-first-run',
  '--no-default-browser-check',
  ...extra,
  url,
]

const browsers = {
  edge: {
    env: 'SYKE_EDGE',
    default: '/mnt/c/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',
    args: (profile, url) => chromiumArgs(profile, url),
  },
  chrome: {
    env: 'SYKE_CHROME',
    default: () => `${windowsLocalAppData()}/chrome-for-testing/157.0.8103.78/chrome-win64/chrome.exe`,
    // An unzipped Chrome for Testing lacks the folder permissions this sandbox needs.
    args: (profile, url) => chromiumArgs(profile, url, ['--disable-features=NetworkServiceSandbox']),
  },
  firefox: {
    env: 'SYKE_FIREFOX',
    default: () => `${windowsLocalAppData()}/Firefox/firefox.exe`,
    args: (profile, url) => ['-headless', '-no-remote', '-new-instance', '-profile', profile, url],
    async prepare(profileDir) {
      await writeFile(
        join(profileDir, 'user.js'),
        [
          'user_pref("browser.shell.checkDefaultBrowser", false);',
          'user_pref("datareporting.policy.dataSubmissionEnabled", false);',
          'user_pref("toolkit.telemetry.reportingpolicy.firstRun", false);',
          'user_pref("browser.aboutwelcome.enabled", false);',
        ].join('\n'),
      )
    },
  },
}

function wslPath(windowsPath) {
  return execFileSync('wslpath', ['-u', windowsPath], { encoding: 'utf8' }).trim()
}

function windowsPathOf(path) {
  return execFileSync('wslpath', ['-w', path], { encoding: 'utf8' }).trim()
}

// --- A minified copy of the bundle, to prove the inline worker survives one --

async function buildMinified() {
  const require = createRequire(createRequire(import.meta.url).resolve('tsdown'))
  const { build } = await import(require.resolve('rolldown'))
  await build({
    input: join(root, 'dist/web.mjs'),
    output: { file: join(out, 'min/web.mjs'), format: 'esm', minify: true },
    logLevel: 'silent',
  })
}

// --- Server ------------------------------------------------------------------

const types = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.mjs': 'text/javascript' }
let deliver = null

function serve() {
  const routes = {
    '/': 'test/browser/index.html',
    '/frame.html': 'test/browser/frame.html',
    '/csp.html': 'test/browser/frame.html',
    '/tests.js': 'test/browser/tests.js',
    '/util.js': 'test/browser/util.js',
    '/frame.js': 'test/browser/frame.js',
  }
  const server = createServer(async (req, res) => {
    const { pathname } = new URL(req.url ?? '/', 'http://localhost')
    if (req.method === 'POST' && pathname === '/results') {
      let body = ''
      for await (const chunk of req) body += chunk
      res.end('ok')
      deliver?.(JSON.parse(body))
      return
    }
    let file = routes[pathname]
    if (!file && /^\/dist\/[\w.-]+$/.test(pathname)) file = pathname.slice(1)
    if (!file && /^\/min\/[\w.-]+$/.test(pathname)) file = join('.browser-test', pathname)
    if (!file || !existsSync(join(root, file))) {
      res.statusCode = 404
      return res.end()
    }
    res.setHeader('content-type', types[extname(file)] ?? 'application/octet-stream')
    res.setHeader('cache-control', 'no-store')
    // Like a strict production CSP: workers only from this origin, so no blob: workers.
    if (pathname === '/csp.html') res.setHeader('content-security-policy', "worker-src 'self'")
    res.end(await readFile(join(root, file)))
  })
  return new Promise((done) => server.listen(0, '127.0.0.1', () => done(server)))
}

// --- Browsers ----------------------------------------------------------------

async function runBrowser(name, exe, baseUrl) {
  const browser = browsers[name]
  const onWindows = exe.startsWith('/mnt/')
  const tag = `syke-browser-${name}-${Date.now()}`
  const profileDir = onWindows ? join(windowsLocalAppData(), 'Temp', tag) : join(tmpdir(), tag)
  await mkdir(profileDir, { recursive: true })
  await browser.prepare?.(profileDir)
  const profile = onWindows ? windowsPathOf(profileDir) : profileDir

  const results = new Promise((done, fail) => {
    deliver = done
    setTimeout(() => fail(new Error(`${name}: no results after ${PAGE_TIMEOUT_MS / 1000} s`)), PAGE_TIMEOUT_MS).unref()
  })
  const child = spawn(exe, browser.args(profile, `${baseUrl}/?auto&browser=${name}`), {
    cwd: onWindows ? '/mnt/c' : undefined,
    stdio: 'ignore',
  })
  const exited = new Promise((done) => child.once('exit', done))
  try {
    return await results
  } finally {
    deliver = null
    if (onWindows) {
      // The WSL side of the process doesn't own the browser's Windows processes.
      // (Its own command line holds the profile path too, so skip $PID.)
      const match = profile.replaceAll("'", "''")
      try {
        execFileSync(POWERSHELL, [
          '-NoProfile',
          '-Command',
          `Get-CimInstance Win32_Process | Where-Object { $_.ProcessId -ne $PID -and $_.CommandLine -like '*${match}*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }`,
        ], { cwd: '/mnt/c', stdio: 'ignore' })
      } catch (error) {
        console.warn(`${name}: could not stop the browser: ${error.message}`)
      }
    }
    child.kill()
    await Promise.race([exited, new Promise((done) => setTimeout(done, 5000))])
    for (let attempt = 0; attempt < 10; attempt++) {
      try {
        await rm(profileDir, { recursive: true, force: true })
        break
      } catch {
        await new Promise((done) => setTimeout(done, 500))
      }
    }
  }
}

function report(name, results) {
  console.log(`\n${name}: ${results.env.userAgent}`)
  console.log(`  quota ${results.env.quotaGB} GB, secure context ${results.env.secureContext}, Web Locks ${results.env.webLocks}`)
  for (const test of results.tests) {
    console.log(`  ${test.ok ? '✓' : '✗'} ${test.name} (${test.ms} ms, longest stall ${test.longestStallMs} ms)`)
    if (test.detail) console.log(`      ${test.detail.split('\n').join('\n      ')}`)
  }
}

const args = process.argv.slice(2)
await mkdir(out, { recursive: true })
await buildMinified()
const server = await serve()
const baseUrl = `http://localhost:${server.address().port}`

if (args.includes('--serve')) {
  console.log(`Serving the browser tests at ${baseUrl}/`)
} else {
  const names = args.length > 0 ? args : Object.keys(browsers)
  let failed = false
  for (const name of names) {
    const browser = browsers[name]
    if (!browser) throw new Error(`unknown browser ${name}; expected ${Object.keys(browsers).join(', ')}`)
    const exe = process.env[browser.env] ?? (typeof browser.default === 'function' ? browser.default() : browser.default)
    if (!exe || !existsSync(exe)) {
      console.log(`\n${name}: not found (${exe}); set ${browser.env}`)
      if (args.length > 0) failed = true
      continue
    }
    try {
      const results = await runBrowser(name, exe, baseUrl)
      await mkdir(join(out, 'results'), { recursive: true })
      await writeFile(join(out, 'results', `${name}.json`), `${JSON.stringify(results, null, 2)}\n`)
      report(name, results)
      if (!results.ok) failed = true
    } catch (error) {
      console.log(`\n${name}: ${error.message}`)
      failed = true
    }
  }
  server.close()
  console.log(failed ? '\nBrowser tests FAILED' : '\nBrowser tests passed')
  process.exitCode = failed ? 1 : 0
}
