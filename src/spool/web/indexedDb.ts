import type { SpoolStorage, SpoolStore } from './store.ts'

export const DB_NAME = 'syke-spool'
const STORE = 'chunks'
/** Bytes to read back per transaction: transactions are slow (up to ~2.5 ms in Edge). */
const READ_BATCH_BYTES = 1024 * 1024

/**
 * One connection to the spool database and how many spools use it. A
 * connection that is replaced (another tab upgraded the schema, or the
 * browser closed it) keeps its own count, so its users still release it.
 */
interface Connection {
  readonly db: Promise<IDBDatabase>
  users: number
}

let connection: Connection | null = null
const connectionOf = new WeakMap<IDBDatabase, Connection>()

function connect(): Connection {
  let conn!: Connection
  const forget = () => {
    if (connection === conn) connection = null
  }
  const db = new Promise<IDBDatabase>((resolve, reject) => {
    if (typeof indexedDB === 'undefined') throw new Error('syke: this runtime has no IndexedDB')
    let gaveUp = false
    const req = indexedDB.open(DB_NAME, 1)
    // One store shared by every spool, keyed [spoolId, seq]: adding a store
    // per spool would need a version change, which blocks other tabs.
    req.onupgradeneeded = () => req.result.createObjectStore(STORE)
    req.onsuccess = () => {
      const opened = req.result
      // Opened after the open was given up on (blocked): nobody will use or close it.
      if (gaveUp) return opened.close()
      connectionOf.set(opened, conn)
      // Let a newer schema (another tab) take over rather than block it.
      opened.onversionchange = () => {
        opened.close()
        forget()
      }
      // The browser closed it (storage cleared, Safari backgrounding a page):
      // the next spool opens a new one.
      opened.onclose = forget
      resolve(opened)
    }
    req.onerror = () => {
      gaveUp = true
      reject(req.error)
    }
    req.onblocked = () => {
      gaveUp = true
      reject(new Error('syke: the spool database is blocked by another tab'))
    }
  })
  conn = { db, users: 0 }
  db.catch(forget)
  return conn
}

/** The page's connection to the spool database, shared by every spool on it. */
export async function openDb(): Promise<IDBDatabase> {
  const conn = (connection ??= connect())
  conn.users++
  try {
    return await conn.db
  } catch (error) {
    conn.users--
    throw error
  }
}

/** Releases a connection from `openDb()`; the last user of that connection closes it. */
export function closeDb(db: IDBDatabase): void {
  const conn = connectionOf.get(db)
  if (conn && --conn.users > 0) return
  if (conn && connection === conn) connection = null
  db.close()
}

/** Whether the spool database exists, without creating it. `true` if that can't be told. */
export async function dbExists(): Promise<boolean> {
  if (typeof indexedDB === 'undefined') return false
  if (typeof indexedDB.databases !== 'function') return true
  const databases = await indexedDB.databases()
  return databases.some((database) => database.name === DB_NAME)
}

function transact<T>(db: IDBDatabase, mode: IDBTransactionMode, fn: (store: IDBObjectStore) => T): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    // 'relaxed': a spool never needs its data to survive a crash.
    const tx = db.transaction(STORE, mode, { durability: 'relaxed' })
    let result: T
    try {
      result = fn(tx.objectStore(STORE))
    } catch (error) {
      tx.abort()
      reject(error)
      return
    }
    tx.oncomplete = () => resolve(result)
    // Not `onerror`: a failed request's error event reaches the transaction
    // before it aborts, while `tx.error` is still null. Every failure that
    // loses data aborts the transaction, with `tx.error` set by then.
    tx.onabort = () => reject(tx.error ?? new Error('syke: IndexedDB transaction aborted'))
  })
}

/** Every key of spool `id`. */
function spoolRange(id: string): IDBKeyRange {
  // Arrays sort after numbers, so [id, []] is past every [id, seq].
  return IDBKeyRange.bound([id], [id, []])
}

/** Ids of every spool with records in the database. */
export async function spoolIdsInDb(db: IDBDatabase): Promise<string[]> {
  const ids: string[] = []
  await transact(db, 'readonly', (store) => {
    const req = store.openKeyCursor()
    req.onsuccess = () => {
      const cursor = req.result
      if (!cursor) return
      const id = (cursor.key as [string, number])[0]
      ids.push(id)
      cursor.continue([id, []])
    }
  })
  return ids
}

/** Deletes every record of spool `id`. */
export function deleteSpoolRecords(db: IDBDatabase, id: string): Promise<void> {
  return transact(db, 'readwrite', (store) => void store.delete(spoolRange(id)))
}

/**
 * Spool storage as records in IndexedDB: the fallback when OPFS is
 * unavailable. Each write is one transaction holding one record (a batch of
 * up to 1 MiB, since transactions are slow); reads take about 1 MiB of the
 * oldest records per transaction and delete them as they go.
 */
export class IdbStore implements SpoolStore {
  readonly kind: SpoolStorage = 'indexeddb'
  readonly #db: IDBDatabase
  readonly #id: string
  readonly #chunkSize: number
  readonly #maxBytes: number
  readonly #release: () => void
  // Sizes of the stored records, oldest first; `#head` is the oldest's seq.
  readonly #sizes: number[] = []
  #head = 0
  #stored = 0
  #closing: Promise<void> | null = null

  static async open(id: string, chunkSize: number, maxBytes: number, release: () => void): Promise<IdbStore> {
    return new IdbStore(await openDb(), id, chunkSize, maxBytes, release)
  }

  private constructor(db: IDBDatabase, id: string, chunkSize: number, maxBytes: number, release: () => void) {
    this.#db = db
    this.#id = id
    this.#chunkSize = chunkSize
    this.#maxBytes = maxBytes
    this.#release = release
  }

  get empty(): boolean {
    return this.#sizes.length === 0
  }

  get atMax(): boolean {
    return this.#stored >= this.#maxBytes
  }

  room(want: number, growth: number, anySize: boolean): number {
    // Every stored byte is new usage: deleted records free theirs.
    const room = Math.min(want, this.#maxBytes - this.#stored, Math.max(0, growth))
    if (room <= 0) return 0
    return anySize || room >= Math.min(want, this.#chunkSize) ? room : 0
  }

  wouldReachMax(growth: number): boolean {
    return this.#stored + Math.max(0, growth) >= this.#maxBytes
  }

  async write(bytes: Uint8Array): Promise<number> {
    // Structured clone copies a view's whole buffer, so store just the bytes.
    const record =
      bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength ? bytes : bytes.slice()
    const seq = this.#head + this.#sizes.length
    await transact(this.#db, 'readwrite', (store) => void store.put(record, [this.#id, seq]))
    this.#sizes.push(record.byteLength)
    this.#stored += record.byteLength
    return record.byteLength
  }

  async read(): Promise<Uint8Array[]> {
    let count = 0
    let bytes = 0
    // At least one record, then as many more as fit in READ_BATCH_BYTES.
    while (count < this.#sizes.length) {
      const size = this.#sizes[count] as number
      if (count > 0 && bytes + size > READ_BATCH_BYTES) break
      bytes += size
      count++
    }
    const range = IDBKeyRange.bound([this.#id, this.#head], [this.#id, this.#head + count - 1])
    // The request's result is read once the transaction commits; a failed
    // request aborts the transaction, which rejects.
    const got = await transact(this.#db, 'readwrite', (store) => {
      const req = store.getAll(range, count)
      store.delete(range)
      return req
    })
    const records = got.result
    if (records.length !== count) throw new Error('syke: spool records went missing from IndexedDB')
    this.#sizes.splice(0, count)
    this.#head += count
    this.#stored -= bytes
    // Not given back to the quota budget: browsers reclaim deleted records
    // lazily (Chrome on compaction), so the next estimate is the authority.
    return records as Uint8Array[]
  }

  close(): Promise<void> {
    this.#closing ??= deleteSpoolRecords(this.#db, this.#id).finally(() => {
      closeDb(this.#db)
      this.#release()
    })
    return this.#closing
  }
}
