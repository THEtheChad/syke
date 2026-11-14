import { resolveObjectURL } from 'node:buffer'
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb'
import { afterEach, vi } from 'vitest'

/**
 * Just enough of a browser for the web Spool's storage to run in Node:
 * `fake-indexeddb`, an in-memory OPFS with sync access handles, a `Worker`
 * that runs the real worker source (from its Blob URL) against that OPFS,
 * and `navigator.storage.estimate()`. Node already has `navigator.locks`.
 */
export interface FakeBrowserOptions {
  /** No `Worker` global at all (as in Node or Deno). */
  noWorker?: boolean
  /** Worker creation fails the way a CSP `worker-src` without `blob:` makes it. */
  workerBlocked?: boolean
  /** The worker has no `createSyncAccessHandle` (older Firefox/Safari). */
  noSyncHandles?: boolean
  /** No IndexedDB. */
  noIndexedDb?: boolean
  /** The origin's quota, in bytes. OPFS writes past it throw `QuotaExceededError`. */
  quota?: number
  /** Usage by other data, in bytes, on top of OPFS files. */
  usage?: number
}

export interface FakeBrowser {
  readonly root: FakeDirectory
  readonly options: FakeBrowserOptions
  /** Live (not terminated) workers. */
  readonly workers: Set<FakeWorker>
  /** Names of OPFS files. */
  opfsFiles(): string[]
  /** Records in the spool database (0 if it doesn't exist). */
  idbRecords(): Promise<number>
  /** Calls to `navigator.storage.estimate()`. */
  estimates: number
}

let current: FakeBrowser | null = null

export function installFakeBrowser(options: FakeBrowserOptions = {}): FakeBrowser {
  const root = new FakeDirectory(options)
  const factory = new IDBFactory()
  const browser: FakeBrowser = {
    root,
    options,
    workers: new Set(),
    estimates: 0,
    opfsFiles: () => [...root.files.keys()],
    idbRecords: () => countRecords(factory),
  }
  current = browser
  if (!options.noIndexedDb) {
    vi.stubGlobal('indexedDB', factory)
    vi.stubGlobal('IDBKeyRange', IDBKeyRange)
  }
  if (!options.noWorker) vi.stubGlobal('Worker', FakeWorker)
  Object.defineProperty(navigator, 'storage', {
    configurable: true,
    value: {
      getDirectory: async () => root,
      estimate: async () => {
        browser.estimates++
        return { quota: options.quota ?? 10 * 1024 ** 3, usage: (options.usage ?? 0) + root.bytes() }
      },
    },
  })
  return browser
}

/** Installs a fresh fake browser around each test and removes it after. */
export function useFakeBrowser(): void {
  afterEach(() => {
    for (const worker of current?.workers ?? []) worker.terminate()
    current = null
    vi.unstubAllGlobals()
    delete (navigator as { storage?: unknown }).storage
  })
}

/**
 * A fresh copy of `syke/web`, as a new page would load it: no shared worker,
 * database connection, quota snapshot or sweep yet.
 */
export async function freshPage(): Promise<typeof import('../../../src/web.ts')> {
  vi.resetModules()
  return import('../../../src/web.ts')
}

async function countRecords(factory: IDBFactory): Promise<number> {
  const databases = await factory.databases()
  if (!databases.some((database) => database.name === 'syke-spool')) return 0
  const db = await new Promise<IDBDatabase>((resolve, reject) => {
    const req = factory.open('syke-spool')
    req.onsuccess = () => resolve(req.result)
    req.onerror = () => reject(req.error)
  })
  try {
    return await new Promise<number>((resolve, reject) => {
      const req = db.transaction('chunks').objectStore('chunks').count()
      req.onsuccess = () => resolve(req.result)
      req.onerror = () => reject(req.error)
    })
  } finally {
    db.close()
  }
}

export class FakeFile {
  data: Uint8Array = new Uint8Array(0)
  size: number = 0
  locked: boolean = false
}

export class FakeDirectory {
  readonly files: Map<string, FakeFile> = new Map()
  readonly #options: FakeBrowserOptions
  /** Largest any file has been. */
  peakFileSize: number = 0

  constructor(options: FakeBrowserOptions) {
    this.#options = options
  }

  bytes(): number {
    let total = 0
    for (const file of this.files.values()) total += file.size
    return total
  }

  async getFileHandle(
    name: string,
    { create = false }: { create?: boolean } = {},
    owner?: Set<FakeSyncAccessHandle>,
  ): Promise<FakeFileHandle> {
    let file = this.files.get(name)
    if (!file) {
      if (!create) throw new DOMException(`${name} not found`, 'NotFoundError')
      file = new FakeFile()
      this.files.set(name, file)
    }
    return new FakeFileHandle(file, this, owner)
  }

  async removeEntry(name: string): Promise<void> {
    const file = this.files.get(name)
    if (!file) throw new DOMException(`${name} not found`, 'NotFoundError')
    if (file.locked) throw new DOMException(`${name} is open`, 'NoModificationAllowedError')
    this.files.delete(name)
  }

  async *keys(): AsyncGenerator<string> {
    yield* [...this.files.keys()]
  }

  /** Called by a sync handle before growing a file to `end` bytes. */
  checkQuota(file: FakeFile, end: number): void {
    const { quota, usage = 0 } = this.#options
    if (quota === undefined || end <= file.size) return
    if (usage + this.bytes() - file.size + end > quota) {
      throw new DOMException('quota exceeded', 'QuotaExceededError')
    }
  }
}

export class FakeFileHandle {
  readonly #file: FakeFile
  readonly #dir: FakeDirectory
  readonly #owner: Set<FakeSyncAccessHandle> | undefined
  constructor(file: FakeFile, dir: FakeDirectory, owner?: Set<FakeSyncAccessHandle>) {
    this.#file = file
    this.#dir = dir
    this.#owner = owner
  }

  async createSyncAccessHandle(): Promise<FakeSyncAccessHandle> {
    if (this.#file.locked) throw new DOMException('already open', 'NoModificationAllowedError')
    this.#file.locked = true
    const handle = new FakeSyncAccessHandle(this.#file, this.#dir)
    this.#owner?.add(handle)
    return handle
  }
}

export class FakeSyncAccessHandle {
  readonly #file: FakeFile
  readonly #dir: FakeDirectory
  constructor(file: FakeFile, dir: FakeDirectory) {
    this.#file = file
    this.#dir = dir
  }

  read(buffer: Uint8Array, { at }: { at: number }): number {
    const count = Math.max(0, Math.min(buffer.byteLength, this.#file.size - at))
    buffer.set(this.#file.data.subarray(at, at + count))
    return count
  }

  write(buffer: Uint8Array, { at }: { at: number }): number {
    const file = this.#file
    const end = at + buffer.byteLength
    this.#dir.checkQuota(file, end)
    if (end > file.data.byteLength) {
      const grown = new Uint8Array(Math.max(end, file.data.byteLength * 2))
      grown.set(file.data.subarray(0, file.size))
      file.data = grown
    }
    file.data.set(buffer, at)
    file.size = Math.max(file.size, end)
    this.#dir.peakFileSize = Math.max(this.#dir.peakFileSize, file.size)
    return buffer.byteLength
  }

  getSize(): number {
    return this.#file.size
  }

  flush(): void {}

  close(): void {
    this.#file.locked = false
  }
}

/** Runs the worker source from a Blob URL in-process, with a fake `self`. */
export class FakeWorker extends EventTarget {
  #handlers: ((event: { data: unknown }) => void) | null = null
  readonly #handles = new Set<FakeSyncAccessHandle>()
  readonly #started: Promise<void>
  #terminated = false

  constructor(url: string) {
    super()
    const browser = current as FakeBrowser
    browser.workers.add(this)
    if (browser.options.workerBlocked) {
      this.#started = Promise.resolve()
      setTimeout(() => {
        browser.workers.delete(this)
        this.dispatchEvent(Object.assign(new Event('error'), { message: 'blocked by worker-src' }))
      })
      return
    }
    const scope = {
      navigator: {
        storage: {
          // The worker's view of OPFS, so terminate() can close its handles.
          getDirectory: async () => ({
            getFileHandle: (name: string, options?: { create?: boolean }) =>
              browser.root.getFileHandle(name, options, this.#handles),
            removeEntry: (name: string) => browser.root.removeEntry(name),
          }),
        },
      },
      FileSystemFileHandle: browser.options.noSyncHandles ? class {} : FakeFileHandle,
      onmessage: null as ((event: { data: unknown }) => void) | null,
      postMessage: (data: unknown, transfer: Transferable[] = []) => {
        if (this.#terminated) return
        const copy = structuredClone(data, { transfer: transfer as never[] })
        setTimeout(() => {
          if (!this.#terminated) this.dispatchEvent(new MessageEvent('message', { data: copy }))
        })
      },
    }
    const blob = resolveObjectURL(url)
    if (!blob) throw new Error(`FakeWorker: no blob at ${url}`)
    this.#started = blob.text().then((source) => {
      new Function('self', source)(scope)
      this.#handlers = (event) => scope.onmessage?.(event)
    })
  }

  postMessage(data: unknown, transfer: Transferable[] = []): void {
    if (this.#terminated) return
    const copy = structuredClone(data, { transfer: transfer as never[] })
    void this.#started.then(() => {
      if (!this.#terminated) this.#handlers?.({ data: copy })
    })
  }

  /** Like a browser: the worker stops and its sync handles close. */
  terminate(): void {
    this.#terminated = true
    current?.workers.delete(this)
    for (const handle of this.#handles) handle.close()
  }
}

/** Polls `check` until it's true. */
export async function waitFor(check: () => boolean | Promise<boolean>, timeoutMs = 2000): Promise<void> {
  const start = Date.now()
  while (!(await check())) {
    if (Date.now() - start > timeoutMs) throw new Error('waitFor timed out')
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}
