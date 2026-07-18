#!/bin/sh

set -eu

repository="Rionlyu/spoold"
install_dir="${SPOOLD_INSTALL_DIR:-/usr/local/bin}"
version="${SPOOLD_VERSION:-}"

command -v curl >/dev/null 2>&1 || {
  echo "spoold installer: curl is required" >&2
  exit 1
}

case "$(uname -s)" in
  Linux) os="linux" ;;
  Darwin) os="darwin" ;;
  *)
    echo "spoold installer: only Linux and macOS are supported" >&2
    exit 1
    ;;
esac

case "$(uname -m)" in
  x86_64 | amd64) arch="amd64" ;;
  arm64 | aarch64) arch="arm64" ;;
  *)
    echo "spoold installer: unsupported architecture $(uname -m)" >&2
    exit 1
    ;;
esac

if [ -z "$version" ]; then
  release_url="$(curl -fsSL -o /dev/null -w '%{url_effective}' "https://github.com/${repository}/releases/latest")"
  version="${release_url##*/}"
fi
case "$version" in
  v*) ;;
  *) version="v${version}" ;;
esac

release_version="${version#v}"
asset="spoold_${release_version}_${os}_${arch}.tar.gz"
base_url="${SPOOLD_RELEASE_BASE_URL:-https://github.com/${repository}/releases/download/${version}}"
temporary="$(mktemp -d)"
trap 'rm -rf "$temporary"' EXIT HUP INT TERM

curl -fsSL "${base_url}/${asset}" -o "${temporary}/${asset}"
curl -fsSL "${base_url}/checksums.txt" -o "${temporary}/checksums.txt"

checksum_line="$(
  awk -v asset="$asset" '$2 == asset { print }' "${temporary}/checksums.txt"
)"
if [ -z "$checksum_line" ] || [ "$(printf '%s\n' "$checksum_line" | wc -l | tr -d ' ')" -ne 1 ]; then
  echo "spoold installer: release checksum for ${asset} is missing or ambiguous" >&2
  exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
  (
    cd "$temporary"
    printf '%s\n' "$checksum_line" | sha256sum --check -
  )
else
  (
    cd "$temporary"
    printf '%s\n' "$checksum_line" | shasum -a 256 --check -
  )
fi

tar -xzf "${temporary}/${asset}" -C "$temporary"
archive_dir="${temporary}/spoold_${release_version}_${os}_${arch}"

mkdir -p "$install_dir"
install -m 0755 "${archive_dir}/spoold" "${install_dir}/spoold"
install -m 0755 "${archive_dir}/spoolctl" "${install_dir}/spoolctl"

"${install_dir}/spoold" -version >/dev/null
"${install_dir}/spoolctl" version >/dev/null

echo "installed spoold and spoolctl ${version} to ${install_dir}"
