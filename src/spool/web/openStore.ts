import { closeDb, dbExists, deleteSpoolRecords, IdbStore, openDb, spoolIdsInDb } from './indexedDb.ts'
import { OpfsStore } from './opfs.ts'
import {
  canSweep,
  FILE_PREFIX,
  holdLock,
  ifAbandoned,
  NoStore,
  SpoolStorageError,
  type SpoolStore,
  spoolId,
} from './store.ts'

/** Which storage a spool may use: `'auto'` tries OPFS, then IndexedDB. */
export type SpoolStorageChoice = 'auto' | 'opfs' | 'indexeddb'

/**
 * Opens storage for one spool. Takes the spool's Web Lock first, so the
 * sweep can never mistake it for abandoned. With `'auto'`, falls back from
 * OPFS to IndexedDB to no storage at all (a `NoStore`, so `onLimit` applies);
 * a backend asked for by name fails with a `SpoolStorageError` instead.
 */
export async function openStore(choice: SpoolStorageChoice, chunkSize: number, maxBytes: number): Promise<SpoolStore> {
  sweepOnce()
  const id = spoolId()
  const release = await holdLock(id)
  const failures: string[] = []

  if (choice !== 'indexeddb') {
    try {
      return await OpfsStore.open(id, chunkSize, maxBytes, release)
    } catch (error) {
      if (choice === 'opfs') {
        release()
        throw new SpoolStorageError(`syke: OPFS storage is unavailable (${describe(error)})`, { cause: error })
      }
      failures.push(`OPFS: ${describe(error)}`)
    }
  }

  try {
    return await IdbStore.open(id, chunkSize, maxBytes, release)
  } catch (error) {
    if (choice === 'indexeddb') {
      release()
      throw new SpoolStorageError(`syke: IndexedDB storage is unavailable (${describe(error)})`, { cause: error })
    }
    failures.push(`IndexedDB: ${describe(error)}`)
  }

  release()
  return new NoStore(failures.join('; '))
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message.replace(/^syke: /, '') : String(error)
}

let swept: Promise<void> | null = null

/** Starts the sweep once per page, in the background. */
function sweepOnce(): void {
  swept ??= sweep().catch(() => {})
}

/**
 * Deletes the OPFS files and IndexedDB records of spools whose page is gone,
 * found by their Web Lock being free. Taking the lock before deleting means a
 * live spool, in this tab or another, is never touched.
 */
export async function sweep(): Promise<void> {
  if (!canSweep()) return
  const ids = new Set<string>()

  let root: FileSystemDirectoryHandle | null = null
  try {
    const storage = globalThis.navigator?.storage
    if (typeof storage?.getDirectory === 'function') {
      root = await storage.getDirectory()
      for await (const name of (root as unknown as { keys(): AsyncIterable<string> }).keys()) {
        if (name.startsWith(FILE_PREFIX)) ids.add(name.slice(FILE_PREFIX.length))
      }
    }
  } catch {
    root = null
  }

  let db: IDBDatabase | null = null
  try {
    if (await dbExists()) {
      db = await openDb()
      for (const id of await spoolIdsInDb(db)) ids.add(id)
    }
  } catch {
    // No IndexedDB here; nothing to sweep in it.
  }

  try {
    for (const id of ids) {
      await ifAbandoned(id, async () => {
        await root?.removeEntry(FILE_PREFIX + id).catch(() => {})
        if (db) await deleteSpoolRecords(db, id).catch(() => {})
      })
    }
  } finally {
    if (db) closeDb(db)
  }
}
