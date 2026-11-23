// Runs test/browser (syke/web: the Spool against real OPFS and IndexedDB, plus
// every other utility) in headless browsers. Each page POSTs its results back.
//
//   pnpm test:browser                    every browser found
//   pnpm test:browser chrome firefox     just those
//   pnpm test:browser --serve            only serve; open the printed URL yourself
//
// Browser executables come from SYKE_EDGE, SYKE_CHROME and SYKE_FIREFOX, or
// are looked for: Windows installs reached from WSL (where these tests were
// first run), then the usual Linux binaries on PATH, then macOS apps. The run
// fails if no browser is found. Results are written to
// .browser-test/results/<browser>.json.
import { execFileSync, spawn } from 'node:child_process'
import { existsSync } from 'node:fs'
import { mkdir, readFile, rm, writeFile } from 'node:fs/promises'
import { createServer } from 'node:http'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { delimiter, extname, join, resolve } from 'node:path'

const root = resolve(import.meta.dirname, '..')
const out = join(root, '.browser-test')
const PAGE_TIMEOUT_MS = 10 * 60 * 1000
// One host for both listening and the URL the browsers load: `localhost` may
// resolve to ::1 first, which a server on 127.0.0.1 doesn't answer. 127.0.0.1
// is a secure context, as OPFS needs.
const HOST = '127.0.0.1'

const WINDOWS = '/mnt/c/Windows/System32'
const POWERSHELL = `${WINDOWS}/WindowsPowerShell/v1.0/powershell.exe`
let localAppData
/** %LOCALAPPDATA% as a WSL path, or null when not under WSL. */
const windowsLocalAppData = () => {
  if (localAppData !== undefined) return localAppData
  localAppData = null
  if (existsSync(`${WINDOWS}/cmd.exe`)) {
    try {
      const windowsPath = execFileSync(`${WINDOWS}/cmd.exe`, ['/c', 'echo %LOCALAPPDATA%'], {
        cwd: '/mnt/c',
        encoding: 'utf8',
      }).trim()
      localAppData = wslPath(windowsPath)
    } catch {
      localAppData = null
    }
  }
  return localAppData
}
/** A path under %LOCALAPPDATA%, or null when there is none. */
const underLocalAppData = (path) => {
  const base = windowsLocalAppData()
  return base ? `${base}/${path}` : null
}
/** The first of `names` found on PATH, or null. */
const onPath = (...names) => {
  for (const dir of (process.env.PATH ?? '').split(delimiter)) {
    for (const name of names) {
      const candidate = join(dir, name)
      if (dir && existsSync(candidate)) return candidate
    }
  }
  return null
}

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
    candidates: () => [
      '/mnt/c/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',
      onPath('microsoft-edge', 'microsoft-edge-stable'),
      '/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge',
    ],
    args: (profile, url) => chromiumArgs(profile, url),
  },
  chrome: {
    env: 'SYKE_CHROME',
    candidates: () => [
      underLocalAppData('chrome-for-testing/157.0.8103.78/chrome-win64/chrome.exe'),
      onPath('google-chrome', 'google-chrome-stable', 'chromium', 'chromium-browser'),
      '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    ],
    // An unzipped Chrome for Testing lacks the folder permissions this sandbox needs.
    args: (profile, url) => chromiumArgs(profile, url, ['--disable-features=NetworkServiceSandbox']),
  },
  firefox: {
    env: 'SYKE_FIREFOX',
    candidates: () => [
      underLocalAppData('Firefox/firefox.exe'),
      onPath('firefox'),
      '/Applications/Firefox.app/Contents/MacOS/firefox',
    ],
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

/** The browser's executable: its env var if set, else the first candidate that exists, else null. */
function findBrowser(browser) {
  const fromEnv = process.env[browser.env]
  if (fromEnv) return fromEnv
  return browser.candidates().find((candidate) => candidate && existsSync(candidate)) ?? null
}

// --- Server ------------------------------------------------------------------

const types = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.mjs': 'text/javascript' }
/** Settles the results of the browser now running: `{ resolve, reject }`, or null. */
let deliver = null

function serve() {
  const routes = {
    '/': 'test/browser/index.html',
    '/frame.html': 'test/browser/frame.html',
    '/csp.html': 'test/browser/frame.html',
    '/tests.js': 'test/browser/tests.js',
    '/util.js': 'test/browser/util.js',
    '/utilities.js': 'test/browser/utilities.js',
    '/frame.js': 'test/browser/frame.js',
  }
  const server = createServer(async (req, res) => {
    const { pathname } = new URL(req.url ?? '/', `http://${HOST}`)
    if (req.method === 'POST' && pathname === '/results') {
      // Decoded once, whole: a multi-byte character may straddle two chunks.
      const chunks = []
      for await (const chunk of req) chunks.push(chunk)
      const body = Buffer.concat(chunks).toString('utf8')
      let results
      try {
        results = JSON.parse(body)
      } catch (error) {
        res.statusCode = 400
        res.end('bad results')
        deliver?.reject(new Error(`the page posted results that are not JSON: ${error.message}`))
        return
      }
      res.end('ok')
      deliver?.resolve(results)
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
  return new Promise((done, fail) => {
    server.once('error', fail)
    server.listen(0, HOST, () => done(server))
  })
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

  let timer
  const results = new Promise((resolve, reject) => {
    deliver = { resolve, reject }
    timer = setTimeout(() => reject(new Error(`${name}: no results after ${PAGE_TIMEOUT_MS / 1000} s`)), PAGE_TIMEOUT_MS)
    timer.unref()
  })
  const child = spawn(exe, browser.args(profile, `${baseUrl}/?auto&browser=${name}`), {
    cwd: onWindows ? '/mnt/c' : undefined,
    stdio: 'ignore',
  })
  // A browser that can't start, or quits before posting, never will: don't wait out the timeout.
  child.once('error', (error) => deliver?.reject(new Error(`${name}: could not start ${exe}: ${error.message}`)))
  const exited = new Promise((done) => child.once('exit', (code, signal) => done({ code, signal })))
  // On Windows the launcher may exit at once and leave the browser running, so only trust exits elsewhere.
  if (!onWindows) {
    void exited.then(({ code, signal }) =>
      deliver?.reject(new Error(`${name}: the browser exited (${signal ?? `code ${code}`}) before posting results`)),
    )
  }
  try {
    return await results
  } finally {
    clearTimeout(timer)
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
const baseUrl = `http://${HOST}:${server.address().port}`

if (args.includes('--serve')) {
  console.log(`Serving the browser tests at ${baseUrl}/`)
} else {
  const names = args.length > 0 ? args : Object.keys(browsers)
  let failed = false
  let ran = 0
  for (const name of names) {
    const browser = browsers[name]
    if (!browser) throw new Error(`unknown browser ${name}; expected ${Object.keys(browsers).join(', ')}`)
    const exe = findBrowser(browser)
    if (!exe || !existsSync(exe)) {
      console.log(`\n${name}: not found${exe ? ` (${exe})` : ''}; set ${browser.env}`)
      if (args.length > 0) failed = true
      continue
    }
    ran++
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
  if (ran === 0) {
    console.log(`\nNo browser found to test in; set ${Object.values(browsers).map((browser) => browser.env).join(', ')}`)
    failed = true
  }
  console.log(failed ? '\nBrowser tests FAILED' : '\nBrowser tests passed')
  process.exitCode = failed ? 1 : 0
}
