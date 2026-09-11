#!/usr/bin/env bash
# Scan a real portable backup or SQL dump for known plaintext credentials.
# Usage: check-plaintext-secrets.sh ARTIFACT --needles-file PRIVATE_NEEDLES_FILE
#        check-plaintext-secrets.sh ARTIFACT NEEDLE [NEEDLE ...]
# Exit 0 = clean, 1 = plaintext found, 2 = invalid input or scan failure.
set -euo pipefail
exec python3 - "$@" <<'PY'
import json
import pathlib
import sys

try:
    args = sys.argv[1:]
    if len(args) < 2:
        raise ValueError('usage: ARTIFACT --needles-file FILE, or ARTIFACT NEEDLE [NEEDLE ...]')
    artifact = pathlib.Path(args[0])
    if args[1] == '--needles-file':
        if len(args) != 3:
            raise ValueError('--needles-file requires exactly one file')
        needles = pathlib.Path(args[2]).read_text(encoding='utf-8').splitlines()
    else:
        needles = args[1:]
    if not needles or any(not value for value in needles):
        raise ValueError('at least one nonempty known plaintext needle is required')

    def mysql_escape(value):
        return (value.replace('\\', '\\\\').replace('\0', '\\0')
                .replace('\n', '\\n').replace('\r', '\\r').replace('\x1a', '\\Z')
                .replace("'", "\\'").replace('"', '\\"'))

    variants = set(needles)
    # SQL dumps can contain JSON-encoded config strings, so account for both
    # serialization layers as well as literal values. Never decode ciphertext.
    for _ in range(2):
        for value in tuple(variants):
            encoded = json.dumps(value, ensure_ascii=False)[1:-1]
            variants.update((encoded, json.dumps(value, ensure_ascii=True)[1:-1],
                             encoded.replace('<', '\\u003c').replace('>', '\\u003e').replace('&', '\\u0026'),
                             value.replace("'", "''"), mysql_escape(value)))
    patterns = [value.encode('utf-8') for value in variants]
    overlap = max(map(len, patterns)) - 1
    tail = b''
    found = False
    with artifact.open('rb') as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            block = tail + chunk
            if any(pattern in block for pattern in patterns):
                found = True
                break
            tail = block[-overlap:] if overlap else b''
    if found:
        print(f'FAIL: known plaintext credential found in {artifact}', file=sys.stderr)
        sys.exit(1)
    print(f'OK: no known plaintext credentials in {artifact}')
except (OSError, ValueError, UnicodeError) as error:
    print(f'ERROR: secret scan could not complete: {error}', file=sys.stderr)
    sys.exit(2)
PY
