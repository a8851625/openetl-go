FROM ubuntu:24.04

ARG EXTISM_JS_VERSION=1.6.0
ARG ESBUILD_VERSION=0.25.6
ARG BINARYEN_VERSION=130

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gzip nodejs npm \
    && rm -rf /var/lib/apt/lists/*

RUN set -eu; \
    arch="$(uname -m)"; \
    case "$arch" in \
      x86_64) asset_arch="x86_64"; expected="0a18362361ad05465118cd8eeb72edaeec89de6894bc283576ef4e07aa3babcc" ;; \
      aarch64|arm64) asset_arch="aarch64"; expected="e6ae6e09ac40f4e14bc5be6f687c58e2995c84170013975fa641809dd3b480a0" ;; \
      *) echo "unsupported compiler architecture: $arch" >&2; exit 1 ;; \
    esac; \
    archive="binaryen-version_${BINARYEN_VERSION}-${asset_arch}-linux.tar.gz"; \
    url="https://github.com/WebAssembly/binaryen/releases/download/version_${BINARYEN_VERSION}/${archive}"; \
    curl -fsSL "$url" -o "/tmp/$archive"; \
    echo "$expected  /tmp/$archive" | sha256sum -c -; \
    mkdir -p /opt/binaryen; \
    tar -xzf "/tmp/$archive" --strip-components=1 -C /opt/binaryen; \
    for tool in /opt/binaryen/bin/*; do ln -s "$tool" "/usr/local/bin/$(basename "$tool")"; done; \
    rm "/tmp/$archive"; \
    wasm-merge --version; \
    wasm-opt --version

RUN set -eu; \
    arch="$(uname -m)"; \
    case "$arch" in \
      x86_64) asset_arch="x86_64"; expected="4ded271ccf465031ccd0dc35e7a140e134d7f30721671cc4a8e1ff805d4aad68" ;; \
      aarch64|arm64) asset_arch="aarch64"; expected="15a186250e68d6bff4ec839fff275d45a90e383a69209dcc1239eb9e3aee6e1b" ;; \
      *) echo "unsupported compiler architecture: $arch" >&2; exit 1 ;; \
    esac; \
    url="https://github.com/extism/js-pdk/releases/download/v${EXTISM_JS_VERSION}/extism-js-${asset_arch}-linux-v${EXTISM_JS_VERSION}.gz"; \
    curl -fsSL "$url" -o /tmp/extism-js.gz; \
    echo "$expected  /tmp/extism-js.gz" | sha256sum -c -; \
    gzip -dc /tmp/extism-js.gz > /usr/local/bin/extism-js; \
    chmod 0755 /usr/local/bin/extism-js; \
    rm /tmp/extism-js.gz

RUN npm install --global "esbuild@${ESBUILD_VERSION}"

WORKDIR /workspace
