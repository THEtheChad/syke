// Helpers shared by the browser test pages.

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/** Random bytes (getRandomValues fills at most 64 KiB per call). */
export function randomData(size) {
  const data = new Uint8Array(size)
  for (let offset = 0; offset < size; offset += 65536) crypto.getRandomValues(data.subarray(offset, offset + 65536))
  return data
}

export async function readAll(readable) {
  const reader = readable.getReader()
  const out = []
  for (;;) {
    const { value, done } = await reader.read()
    if (done) return out
    out.push(value)
  }
}

/**
 * Writes `data` in pieces of the given sizes (cycled) with nothing reading.
 * Takes a writable or a writer; closes it unless `close: false`.
 */
export async function spillStalled(target, data, sizes, { close = true } = {}) {
  const writer = typeof target.getWriter === 'function' ? target.getWriter() : target
  let offset = 0
  for (let i = 0; offset < data.byteLength; i++) {
    const size = sizes[i % sizes.length]
    await writer.write(data.subarray(offset, offset + size))
    offset += size
  }
  if (close) await writer.close()
}

export async function waitFor(check, timeoutMs = 5000) {
  const start = performance.now()
  while (!(await check())) {
    if (performance.now() - start > timeoutMs) throw new Error(`waitFor timed out: ${check}`)
    await sleep(20)
  }
}

/** Names of the spool files in this origin's OPFS. */
export async function opfsSpoolFiles() {
  const root = await navigator.storage.getDirectory()
  const names = []
  for await (const name of root.keys()) if (name.startsWith('syke-spool-')) names.push(name)
  return names
}

/** Records in the spool database, without creating it. */
export async function idbRows() {
  const databases = await indexedDB.databases()
  if (!databases.some((database) => database.name === 'syke-spool')) return 0
  const db = await new Promise((resolve, reject) => {
    const req = indexedDB.open('syke-spool')
    req.onsuccess = () => resolve(req.result)
    req.onerror = () => reject(req.error)
  })
  try {
    if (!db.objectStoreNames.contains('chunks')) return 0
    return await new Promise((resolve, reject) => {
      const req = db.transaction('chunks').objectStore('chunks').count()
      req.onsuccess = () => resolve(req.result)
      req.onerror = () => reject(req.error)
    })
  } finally {
    db.close()
  }
}

/**
 * Tracks the longest the main thread went without running a 4 ms timer.
 * `reset()` returns the longest stall since the last reset.
 */
export function stallMonitor() {
  let last = performance.now()
  let worst = 0
  setInterval(() => {
    const now = performance.now()
    worst = Math.max(worst, now - last - 4)
    last = now
  }, 4)
  return {
    reset() {
      const value = Math.max(0, worst)
      worst = 0
      last = performance.now()
      return value
    },
  }
}

/** Loads `src` in a hidden iframe (a stand-in for another tab); `message` is its first postMessage. */
export function frame(src) {
  const el = document.createElement('iframe')
  el.hidden = true
  el.src = src
  const message = new Promise((resolve) => {
    addEventListener('message', function onMessage(event) {
      if (event.source !== el.contentWindow) return
      removeEventListener('message', onMessage)
      resolve(event.data)
    })
  })
  document.body.append(el)
  return { message, remove: () => el.remove() }
}
