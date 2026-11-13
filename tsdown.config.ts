import { defineConfig, type UserConfig } from 'tsdown'

const config: UserConfig = defineConfig({
  // `syke/node` (Node streams) and `syke/web` (Web Streams). They share
  // chunks, so an error class is the same object whichever entry it came from.
  entry: { node: 'src/node.ts', web: 'src/web.ts' },
  format: ['esm', 'cjs'],
  // Neutral so nothing browser-hostile is assumed; scripts/check-web-bundle.mjs
  // verifies syke/web never reaches a Node built-in.
  platform: 'neutral',
  deps: { neverBundle: [/^node:/] },
  fixedExtension: true,
  target: 'es2022',
  dts: true,
  clean: true,
})

// tsdown loads its config through a default export.
export default config
