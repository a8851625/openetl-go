#!/usr/bin/env python3
"""Exercise the scanner through its public CLI, including dump encodings."""

import json
from pathlib import Path
import subprocess
import tempfile
import unittest


SCANNER = Path(__file__).resolve().parent / "check-plaintext-secrets.sh"


class SecretScannerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="openetl-scan-test-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.artifact = self.root / "artifact"
        self.needles = self.root / "needles"

    def scan(self, content, needle, expected, *, use_file=True):
        self.artifact.write_bytes(content)
        self.needles.write_text(needle + "\n", encoding="utf-8")
        args = ["--needles-file", str(self.needles)] if use_file else [needle]
        result = subprocess.run(
            ["bash", str(SCANNER), str(self.artifact), *args],
            capture_output=True, text=True, check=False,
        )
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        self.assertNotIn(needle, result.stdout + result.stderr)

    def test_literal_and_serialized_credentials(self):
        needle = "scan-probe:'quote'\\slash/密钥<>&"
        json_value = json.dumps(needle, ensure_ascii=False)
        cases = {
            "literal": needle,
            "json": json_value,
            "json_ascii": json.dumps(needle, ensure_ascii=True),
            "go_json_html": json_value.replace("<", r"\u003c").replace(
                ">", r"\u003e").replace("&", r"\u0026"),
            "sql": "INSERT INTO settings VALUES ('" + needle.replace("'", "''") + "');",
            # mysqldump escapes JSON backslashes and SQL quotes in config_json.
            "mysql_json": json_value.replace("\\", "\\\\").replace(
                "'", "\\'").replace('"', '\\"'),
            # pg_dump COPY escapes the backslashes in JSON text.
            "postgres_json": json_value.replace("\\", "\\\\"),
        }
        for name, content in cases.items():
            with self.subTest(name=name):
                self.scan(content.encode(), needle, 1)

    def test_chunk_boundary(self):
        needle = "boundary-credential-scan-marker"
        self.scan(b"x" * (1024 * 1024 - 9) + needle.encode() + b"x", needle, 1)

    def test_control_character_in_legacy_cli_argument(self):
        needle = "legacy-tab\tline\ncredential"
        self.scan(json.dumps(needle).encode(), needle, 1, use_file=False)

    def test_clean_artifact(self):
        self.scan(b'{"dsn":"enc:v1:opaque-ciphertext"}', "known-test-credential", 0)

    def test_invalid_inputs_fail_instead_of_passing(self):
        self.artifact.write_text("{}")
        self.needles.write_text("")
        cases = [
            [], [str(self.artifact)], [str(self.artifact), ""],
            [str(self.artifact), "--needles-file", str(self.needles)],
            [str(self.artifact), "--needles-file"],
            [str(self.root / "missing"), "known-test-credential"],
            [str(self.root), "known-test-credential"],
            [str(self.artifact), "--needles-file", str(self.root / "missing")],
        ]
        for args in cases:
            with self.subTest(args=args):
                result = subprocess.run(
                    ["bash", str(SCANNER), *args], capture_output=True, check=False,
                )
                self.assertEqual(result.returncode, 2, result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
