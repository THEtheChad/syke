# Contributing to syke

## Setup

Node via fnm (`.node-version`), pnpm only (version pinned in `package.json`).

```sh
pnpm install
pnpm check   # typecheck, tests, build, publint, attw, web-bundle guard
```

`pnpm test:browser` runs `test/browser/` (every `syke/web` utility, and the web `Spool` in depth against real OPFS
and IndexedDB) in headless Edge, Chrome and Firefox; see
`scripts/test-browser.mjs` for where it looks for them.

Commits follow [Conventional Commits](https://www.conventionalcommits.org/):
`feat: add Rechunk`, `fix(spool): ...`, `docs: ...`, `test: ...`.

## Layout

```
src/
  node.ts, web.ts          entry points (syke/node, syke/web): re-exports only
  shared/
    transformer.ts         Transformer + Sink: the runtime-neutral contract
    node.ts                CoreTransform: a Transformer as a Node Transform
    web.ts                 CoreTransformStream: a Transformer as a TransformStream
    errors.ts              SykeError, the base of every deliberate error
    validate.ts            option validators (positiveInt, oneOf, ...)
    bytes.ts               hex/base64/compare helpers (no Buffer)
  <utility>/
    core.ts                logic, options type, error class: runtime-neutral
    node.ts                Node wrapper (thin)
    web.ts                 web wrapper (thin)
  spool/core, spool/node, spool/web   Spool's shared allocator/codec and per-runtime code
test/
  harness.ts               run the same tests against node and web versions
  nodeHelpers.ts           Node-only helpers (temp dirs, pausable sinks)
  <utility>/<utility>.test.ts
docs/<utility>.md          one page per utility, linked from the README table
```

Directories are camelCase (`lengthCheck`), doc pages kebab-case (`length-check.md`).

## Writing a utility

1. **Logic once, in `core.ts`.** If the utility is a transform, implement
   `Transformer<I, O>` (see `src/shared/transformer.ts`): `transform` and
   `flush` push output through the `Sink`; return a promise to hold back the
   source; throw to fail the stream; `start` gets the sink for timers;
   `close` releases timers and is called exactly once however the stream
   ends. Combinators that aren't transforms (`fork`, `concat`, ...) share what
   logic they can and implement each runtime directly.
2. **Core and web code must not touch Node.js:** no `node:*` imports, no
   `Buffer`, no `process`. Bytes are `Uint8Array`. `pnpm lint:package` checks
   the built `syke/web` bundle.
3. **Thin wrappers.** `node.ts` subclasses `CoreTransform` and takes
   `NodeStreamOptions` (`highWaterMark`, `signal`) besides the utility's own
   options; `web.ts` subclasses `CoreTransformStream`. Same class name, same
   options, same getters/promises in both. Byte-oriented utilities take
   `Uint8Array` (Node also accepts strings, via `decodeStrings`); record-oriented
   ones use object mode in Node.
4. **Errors.** Deliberate failures throw a subclass of `SykeError` with a
   stable `code` (`ERR_SYKE_<NAME>`), defined in `core.ts` and exported from
   both entries. Bad options throw `TypeError`/`RangeError` from the
   constructor. Every message starts with `syke:`.
5. **Promises on the instance** (like `Digest#digest`) must never cause an
   unhandled rejection: attach a no-op `catch` internally.
6. **Exports.** Add the class, options type and error to `src/node.ts` and
   `src/web.ts`.
7. **Tests** run against both runtimes with `describe.each(runtimes)` and
   `pick()` from `test/harness.ts`; see `test/lengthCheck/` and `test/digest/`.
   Cover errors, empty streams, chunk boundaries, early termination and
   option validation. Use fake timers (`vi.useFakeTimers()`) for time-based
   utilities rather than real sleeps where possible.
8. **Docs.** A `docs/<utility>.md` page (see `docs/length-check.md` for the
   shape: one-paragraph purpose, examples for each runtime, options table,
   errors), a row in the README's utilities table, and an entry in
   `CHANGELOG.md`.

## Style

TypeScript with `isolatedDeclarations`: annotate exported return types. No
default exports. Private state uses `#fields`. Prefer short, plain doc
comments that say why.

## Releasing

Releases are automatic. On every push to `main`, the release workflow
(`.github/workflows/release.yml`) runs `pnpm check` and then
[semantic-release](https://semantic-release.gitbook.io), which reads the
Conventional Commits since the last tag:

| Commits since the last release | Release |
| --- | --- |
| `fix:` or `perf:` | patch (0.1.0 → 0.1.1) |
| `feat:` | minor (0.1.0 → 0.2.0) |
| `!` after the type, or a `BREAKING CHANGE:` footer | major (0.x → 1.0.0) |
| only `docs`, `test`, `ci`, `chore`, `build`, `refactor`, `style` | none |

It then publishes to npm, tags `vX.Y.Z` and writes the release notes on
GitHub Releases. Nothing is committed back: `version` in `package.json` stays
a placeholder, and the real version only exists in the tag and the published
package. So the commit type is the release decision; there is no manual
version bump or changelog step.

npm authenticates through trusted publishing, tied to this repository, the
`release.yml` workflow and the `npm` environment (which only `main` may
use). Renaming any of them breaks publishing until npm's settings are
updated to match.
