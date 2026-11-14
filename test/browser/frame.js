// Pages loaded in iframes by tests.js, each with its own copy of syke/web
// (a new page, as far as the spool is concerned).
//   ?role=orphan  spills to OPFS and IndexedDB and never finishes (its "tab" is then closed)
//   ?role=fresh   runs one spool, which starts this page's sweep
//   ?role=csp     served with a CSP that blocks blob: workers
import { Spool, SpoolStorageError } from '/dist/web.mjs'
import { randomData, readAll, sleep, spillStalled, waitFor, opfsSpoolFiles, idbRows } from './util.js'

const MiB = 1024 * 1024
const role = new URLSearchParams(location.search).get('role')
const report = (ok, detail = '') => parent.postMessage({ ok, detail }, '*')

function equalBytes(a, b) {
  if (a.byteLength !== b.byteLength) return false
  for (let i = 0; i < a.byteLength; i++) if (a[i] !== b[i]) return false
  return true
}

async function roundTrip(options) {
  const data = randomData(8 * MiB)
  const spool = new Spool(options)
  await spillStalled(spool.writable, data, [65536])
  const out = await readAll(spool.readable)
  const joined = new Uint8Array(data.byteLength)
  let offset = 0
  for (const piece of out) {
    joined.set(piece, offset)
    offset += piece.byteLength
  }
  await spool.closed
  return { spool, intact: offset === data.byteLength && equalBytes(joined, data) }
}

try {
  if (role === 'orphan') {
    for (const storage of ['opfs', 'indexeddb']) {
      const spool = new Spool({ storage })
      const writer = spool.writable.getWriter()
      await spillStalled(writer, randomData(4 * MiB), [65536], { close: false })
      // Kept alive (and never ended) until the iframe is removed.
      ;(globalThis.orphans ??= []).push(spool)
    }
    await waitFor(async () => (await opfsSpoolFiles()).length === 1 && (await idbRows()) > 0)
    report(true)
  } else if (role === 'fresh') {
    const { intact } = await roundTrip({ storage: 'indexeddb' })
    // Let the sweep, started in the background, finish.
    await sleep(500)
    report(intact, intact ? '' : 'data differs')
  } else if (role === 'csp') {
    const { spool, intact } = await roundTrip({})
    const opfsError = await roundTrip({ storage: 'opfs' }).then(
      () => null,
      (error) => error,
    )
    const ok = spool.storage === 'indexeddb' && intact && opfsError instanceof SpoolStorageError
    report(
      ok,
      `auto chose ${spool.storage}, data ${intact ? 'intact' : 'DIFFERS'}; storage: 'opfs' failed with ${opfsError?.name}: ${opfsError?.message}`,
    )
  }
} catch (error) {
  report(false, String(error?.stack ?? error))
}
