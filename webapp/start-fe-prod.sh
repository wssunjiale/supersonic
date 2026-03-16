#!/bin/bash

set -euo pipefail

start=$(date +%s)
script_dir=$(cd "$(dirname "$0")"; pwd)
cd "$script_dir"

build_tmp_root="${S2_BUILD_TMP_ROOT:-${TMPDIR:-/tmp}/supersonic-build}"
export COREPACK_HOME="${COREPACK_HOME:-$build_tmp_root/corepack}"
export npm_config_store_dir="${npm_config_store_dir:-$build_tmp_root/pnpm-store}"
export NPM_CONFIG_CACHE="${NPM_CONFIG_CACHE:-$build_tmp_root/npm-cache}"
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-$build_tmp_root/xdg-cache}"
export COREPACK_ENABLE_DOWNLOAD_PROMPT=0
export npm_config_fetch_retries="${npm_config_fetch_retries:-5}"
export npm_config_fetch_retry_factor="${npm_config_fetch_retry_factor:-2}"
export npm_config_fetch_retry_mintimeout="${npm_config_fetch_retry_mintimeout:-10000}"
export npm_config_fetch_retry_maxtimeout="${npm_config_fetch_retry_maxtimeout:-120000}"
export npm_config_fetch_timeout="${npm_config_fetch_timeout:-300000}"

default_registry="${S2_NPM_REGISTRY:-https://registry.npmjs.org/}"
export NPM_CONFIG_REGISTRY="$default_registry"
export npm_config_registry="$default_registry"

mkdir -p "$COREPACK_HOME" "$npm_config_store_dir" "$NPM_CONFIG_CACHE" "$XDG_CACHE_HOME"

function resolvePnpmVersion {
  node -p "const candidates = ['./packages/supersonic-fe/package.json', './packages/chat-sdk/package.json']; for (const file of candidates) { const packageManager = require(file).packageManager || ''; const match = /pnpm@([^+]+)/.exec(packageManager); if (match) { console.log(match[1]); process.exit(0); } } process.exit(1);"
}

function resolvePnpmFromRegistry {
  local pnpm_version=$1
  local registry=${npm_config_registry:-${NPM_CONFIG_REGISTRY:-https://registry.npmjs.org/}}
  local normalized_registry=${registry%/}
  local pnpm_root="$build_tmp_root/pnpm-runtime/pnpm-$pnpm_version"
  local pnpm_archive="$pnpm_root.tgz"
  local pnpm_bin="$pnpm_root/package/bin/pnpm.cjs"

  if [ ! -f "$pnpm_bin" ]; then
    rm -rf "$pnpm_root"
    mkdir -p "$pnpm_root"

    if command -v curl >/dev/null 2>&1; then
      curl -fsSL "$normalized_registry/pnpm/-/pnpm-$pnpm_version.tgz" -o "$pnpm_archive"
    elif command -v wget >/dev/null 2>&1; then
      wget -qO "$pnpm_archive" "$normalized_registry/pnpm/-/pnpm-$pnpm_version.tgz"
    else
      echo "Neither curl nor wget is available to download pnpm."
      exit 1
    fi

    tar -xzf "$pnpm_archive" -C "$pnpm_root"
  fi

  pnpm_cmd=(node "$pnpm_bin")
}

if [ -n "${S2_PNPM_BIN:-}" ]; then
  pnpm_cmd=("$S2_PNPM_BIN")
else
  pnpm_version=$(resolvePnpmVersion)

  if command -v corepack >/dev/null 2>&1 && corepack pnpm@"$pnpm_version" --version >/dev/null 2>&1; then
    pnpm_cmd=(corepack pnpm@"$pnpm_version")
  else
    resolvePnpmFromRegistry "$pnpm_version"
  fi
fi

pnpm_global_args=(--store-dir "$npm_config_store_dir")

node_version=$(node -v)
major_version=$(echo "$node_version" | cut -d'.' -f1 | tr -d 'v')
node_options="${NODE_OPTIONS:-}"

if [ "$major_version" -ge 17 ] && [[ "$node_options" != *"--openssl-legacy-provider"* ]]; then
  node_options="${node_options:+$node_options }--openssl-legacy-provider"
fi

if [[ "$node_options" != *"--max-old-space-size="* ]]; then
  node_options="${node_options:+$node_options }--max-old-space-size=${S2_WEBAPP_BUILD_MAX_OLD_SPACE_SIZE:-4096}"
fi

export NODE_OPTIONS="$node_options"
export CI="${CI:-1}"

rm -rf \
  "supersonic-webapp.tar.gz" \
  "./packages/chat-sdk/dist" \
  "./packages/supersonic-fe/src/.umi" \
  "./packages/supersonic-fe/src/.umi-production" \
  "./packages/supersonic-fe/supersonic-webapp"

"${pnpm_cmd[@]}" "${pnpm_global_args[@]}" -r install --frozen-lockfile --ignore-scripts

"${pnpm_cmd[@]}" -C "./packages/chat-sdk" run build-es
"${pnpm_cmd[@]}" -C "./packages/supersonic-fe" run build:os-local

tar -zcvf "supersonic-webapp.tar.gz" -C "./packages/supersonic-fe" "supersonic-webapp"

end=$(date +%s)
take=$(( end - start ))

echo "Time taken to execute commands is ${take} seconds."
