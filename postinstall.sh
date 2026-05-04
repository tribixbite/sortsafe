#!/bin/sh
# Fix native module resolution on Android/Termux
# Rollup and esbuild don't ship android-arm64 binaries;
# the linux-arm64 builds work fine on Termux.

if [ "$(uname -o 2>/dev/null)" = "Android" ] || [ -d "/data/data/com.termux" ]; then
  # esbuild
  if [ -d "node_modules/@esbuild/linux-arm64" ] && [ ! -e "node_modules/@esbuild/android-arm64" ]; then
    ln -sf linux-arm64 node_modules/@esbuild/android-arm64
  fi

  # rollup: replace native with wasm build
  if [ -d "node_modules/@rollup/wasm-node/dist" ]; then
    cp -rf node_modules/@rollup/wasm-node/dist/* node_modules/rollup/dist/
  fi

  # NOTE: lightningcss glibc native binaries need libgcc_s.so.1 which
  # Termux's bionic-linked node can't load. The fix is to run vite
  # through bun (scripts/vite-cli.ts) which uses glibc and reports
  # process.platform === 'linux', loading linux-arm64-gnu binaries.
fi
