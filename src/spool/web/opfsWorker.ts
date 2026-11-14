/**
 * Source of the dedicated worker that owns OPFS spool files through sync
 * access handles (which only exist in workers). syke is a library and can't
 * ship a separate worker file, so this runs from a Blob URL.
 *
 * It's a plain string, not a function's `.toString()`, so no bundler,
 * minifier or transpiler can rename, wrap or inject helpers into it. It only
 * uses `self`, and plain ES2017 that every browser with sync access handles
 * runs.
 *
 * Protocol (every request has a numeric `id`; replies are `{ id, value }` or
 * `{ id, error: { name, message } }`):
 *
 *   { op: 'open', name }                       creates the file, takes a sync handle
 *   { op: 'write', name, at, buffer, offset, length }   writes; `buffer` is transferred
 *   { op: 'read', name, at, length }           replies `{ buffer, length }`, transferred
 *   { op: 'close', name }                      closes the handle and deletes the file
 *
 * On start it posts `{ ready: boolean }`: whether sync access handles exist.
 * Requests run one at a time, in order.
 */
export const OPFS_WORKER_SOURCE: string = `'use strict';
var handles = new Map();
var rootPromise = null;
var queue = Promise.resolve();

function supported() {
  try {
    var FileHandle = self.FileSystemFileHandle;
    var storage = self.navigator && self.navigator.storage;
    return typeof FileHandle === 'function' &&
      typeof FileHandle.prototype.createSyncAccessHandle === 'function' &&
      !!storage && typeof storage.getDirectory === 'function';
  } catch (error) {
    return false;
  }
}

function root() {
  if (!rootPromise) rootPromise = self.navigator.storage.getDirectory();
  return rootPromise;
}

function handleFor(name) {
  var handle = handles.get(name);
  if (!handle) throw new Error('no open spool file ' + name);
  return handle;
}

async function run(message) {
  var name = message.name;
  if (message.op === 'open') {
    var dir = await root();
    var file = await dir.getFileHandle(name, { create: true });
    var created = await file.createSyncAccessHandle();
    handles.set(name, created);
    return [undefined, []];
  }
  if (message.op === 'write') {
    var target = handleFor(name);
    var bytes = new Uint8Array(message.buffer, message.offset, message.length);
    var written = 0;
    while (written < bytes.length) {
      var wrote = target.write(bytes.subarray(written), { at: message.at + written });
      if (!(wrote > 0)) throw new Error('spool file write made no progress');
      written += wrote;
    }
    return [undefined, []];
  }
  if (message.op === 'read') {
    var source = handleFor(name);
    var out = new Uint8Array(message.length);
    var got = 0;
    while (got < out.length) {
      var count = source.read(out.subarray(got), { at: message.at + got });
      if (!(count > 0)) break;
      got += count;
    }
    return [{ buffer: out.buffer, length: got }, [out.buffer]];
  }
  if (message.op === 'close') {
    var open = handles.get(name);
    handles.delete(name);
    if (open) await open.close();
    var parent = await root();
    try {
      await parent.removeEntry(name);
    } catch (error) {
      if (!error || error.name !== 'NotFoundError') throw error;
    }
    return [undefined, []];
  }
  throw new Error('unknown op ' + message.op);
}

self.onmessage = function (event) {
  var message = event.data;
  queue = queue.then(function () {
    return run(message).then(
      function (result) {
        self.postMessage({ id: message.id, value: result[0] }, result[1]);
      },
      function (error) {
        // A failed write hands its bytes back, so a write refused for quota can be retried.
        var back = message.op === 'write' && message.buffer && message.buffer.byteLength ? message.buffer : undefined;
        self.postMessage({
          id: message.id,
          value: back,
          error: { name: (error && error.name) || 'Error', message: String((error && error.message) || error) },
        }, back ? [back] : []);
      }
    );
  });
};

self.postMessage({ ready: supported() });
`
