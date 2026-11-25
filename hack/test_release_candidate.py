"""Candidate gates reject incomplete evidence and changed package bytes."""
import copy
import datetime as dt
import hashlib
import io
import json
from pathlib import Path
import struct
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import release_candidate as rc


def header(goos, arch):
    data = bytearray(1024)
    if goos == "linux":
        data[:6] = b"\x7fELF\x02\x01"
        data[18:20] = struct.pack("<H", {"amd64": 62, "arm64": 183}[arch])
    elif goos == "darwin":
        data[:4] = b"\xcf\xfa\xed\xfe"
        data[4:8] = struct.pack("<I", {"amd64": 0x1000007, "arm64": 0x100000C}[arch])
    else:
        data[:2] = b"MZ"
        data[60:64] = struct.pack("<I", 128)
        data[128:132] = b"PE\0\0"
        data[132:134] = struct.pack("<H", 0x8664)
    return bytes(data)


class CandidateGateTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        binaries, archives = [], []
        for variant in ("default", "extism"):
            for goos, arch in sorted(rc.PLATFORMS):
                member = "openetl-go" + ("-extism" if variant == "extism" else "") + (".exe" if goos == "windows" else "")
                name = variant + "_" + goos + "_" + arch + ".tar.gz"
                body = header(goos, arch) + variant.encode()
                with tarfile.open(self.base / name, "w:gz") as t:
                    for filename, content in [(member, body), ("manifest/config/config.yaml", b"etl: {}"), ("LICENSE", b"fixture")]:
                        info = tarfile.TarInfo(filename)
                        info.size = len(content)
                        t.addfile(info, io.BytesIO(content))
                archives.append({"path": name, "sha256": rc.sha(self.base / name)})
                binaries.append({"variant": variant, "goos": goos, "goarch": arch,
                                 "archive": name, "member": member, "sha256": hashlib.sha256(body).hexdigest()})
        (self.base / "candidate-image.tar").write_bytes(b"isolated image fixture")
        (self.base / "checksums.txt").write_text("".join(a["sha256"] + "  " + a["path"] + "\n" for a in archives))
        selected = next(b for b in binaries if (b["variant"], b["goos"], b["goarch"]) == ("default", "linux", "amd64"))
        build_end = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=5)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        self.data = {"format": "openetl-candidate/v1", "source": {"commit": "a" * 40, "dirty": False},
                     "build": {"finished_at": build_end}, "binaries": binaries, "archives": archives,
                     "checksum_file": {"path": "checksums.txt", "sha256": rc.sha(self.base / "checksums.txt")},
                     "image": {"id": "sha256:" + "b" * 64, "architecture": "amd64", "archive": "candidate-image.tar",
                               "archive_sha256": rc.sha(self.base / "candidate-image.tar"), "binary_sha256": selected["sha256"]}}
        rc.save(self.base / "candidate.json", self.data)
        (self.base / "connector-evidence.json").write_text("{}")
        values = []
        for name in rc.REQUIRED:
            folder = rc.case_dir(self.base, name)
            folder.mkdir(parents=True)
            (folder / "command.log").write_text("synthetic test observation\n")
            if name == "tagged":
                (folder / "paths").mkdir()
                for path_id in rc.PATHS:
                    (folder / "paths" / (path_id + ".json")).write_text("{}")
            elif name in rc.P1 or name == "readiness":
                (folder / "probe").mkdir()
                (folder / "probe/result.json").write_text("{}")
            value = {"name": name, "source_commit": "a" * 40, "image_id": self.data["image"]["id"],
                     "binary_sha256": selected["sha256"], "started_at": rc.utc(), "finished_at": rc.utc(),
                     "result": "passed", "exit_code": 0, "log_sha256": rc.sha(folder / "command.log"),
                     "artifact_sha256": rc.case_artifact_hashes(folder, name)}
            rc.save(folder / "result.json", value)
            values.append(value)
        self.report = {"format": "openetl-candidate-certification/v1", "result": "passed",
                       "source_commit": "a" * 40, "candidate_sha256": rc.sha(self.base / "candidate.json"),
                       "image_id": self.data["image"]["id"], "image_binary_sha256": selected["sha256"],
                       "connector_evidence_sha256": rc.sha(self.base / "connector-evidence.json"),
                       "finished_at": rc.utc(), "cases": values}
        rc.save(self.base / "certification.json", self.report)

    def test_changed_archive_cannot_be_promoted(self):
        rc.verify(self.base)
        (self.base / self.data["archives"][0]["path"]).write_bytes(b"changed package")
        with self.assertRaisesRegex(ValueError, "archive hash"):
            rc.verify(self.base)

    def test_changed_or_inconsistent_published_checksums_are_rejected(self):
        (self.base / "checksums.txt").write_text("0" * 64 + "  unrelated.tar.gz\n")
        with self.assertRaisesRegex(ValueError, "checksum file hash"):
            rc.verify(self.base)
        self.data["checksum_file"]["sha256"] = rc.sha(self.base / "checksums.txt")
        rc.save(self.base / "candidate.json", self.data)
        with self.assertRaisesRegex(ValueError, "checksum file does not match"):
            rc.candidate(self.base)

    def test_missing_or_skipped_mandatory_case_blocks(self):
        for result in ("skipped", "failed"):
            value = copy.deepcopy(self.report["cases"][0])
            value["result"] = result
            rc.save(rc.case_dir(self.base, value["name"]) / "result.json", value)
            with self.assertRaises(ValueError):
                rc.verify(self.base)
        (rc.case_dir(self.base, rc.REQUIRED[0]) / "result.json").unlink()
        with self.assertRaises(FileNotFoundError):
            rc.verify(self.base)

    def test_changed_logs_and_wrong_binary_fail_closed(self):
        (rc.case_dir(self.base, rc.REQUIRED[0]) / "command.log").write_text("replacement claim")
        with self.assertRaises(ValueError):
            rc.verify(self.base)
        data = copy.deepcopy(self.data)
        data["image"]["binary_sha256"] = "0" * 64
        rc.save(self.base / "candidate.json", data)
        with self.assertRaisesRegex(ValueError, "identities disagree"):
            rc.candidate(self.base)

    def test_unsafe_archive_member_is_rejected(self):
        path = self.base / "unsafe.tar.gz"
        with tarfile.open(path, "w:gz") as t:
            item = tarfile.TarInfo("../escape")
            item.size = 1
            t.addfile(item, io.BytesIO(b"x"))
        with self.assertRaisesRegex(ValueError, "unsafe"):
            rc.archive_binary(path, "openetl-go")

    def test_changed_path_or_probe_artifact_blocks_promotion(self):
        for name, relative in (("tagged", "paths/" + rc.PATHS[0] + ".json"),
                               ("p1-recovery", "probe/result.json")):
            with self.subTest(name=name):
                path = rc.case_dir(self.base, name) / relative
                original = path.read_bytes()
                path.write_text('{"result":"replacement"}')
                with self.assertRaisesRegex(ValueError, "mandatory evidence"):
                    rc.verify(self.base)
                path.write_bytes(original)

    def test_collect_time_cannot_renew_expired_case_evidence(self):
        name = rc.REQUIRED[0]
        value = copy.deepcopy(self.report["cases"][0])
        # A fresh aggregate timestamp cannot make an old execution current.
        value["started_at"] = value["finished_at"] = "2025-01-01T00:00:00Z"
        self.data["build"]["finished_at"] = "2024-12-31T00:00:00Z"
        rc.save(self.base / "candidate.json", self.data)
        rc.save(rc.case_dir(self.base, name) / "result.json", value)
        with self.assertRaisesRegex(ValueError, "evidence expired"):
            rc.reports(self.base, [name])

    def test_wrong_release_source_is_rejected_before_push(self):
        with patch.object(rc, "command") as command:
            command.return_value.stdout = "c" * 40
            with self.assertRaisesRegex(ValueError, "release tag"):
                rc.promote(self.base, "unused.example/fixture:v1", release_tag="v1")
            self.assertEqual(command.call_count, 1)
            self.assertEqual(command.call_args.args[0][:2], ["git", "rev-parse"])

    def test_expired_certificate_does_not_refresh_itself(self):
        self.report["finished_at"] = "2025-01-01T00:00:00Z"
        rc.save(self.base / "certification.json", self.report)
        with self.assertRaisesRegex(ValueError, "expired"):
            rc.verify(self.base)


if __name__ == "__main__":
    unittest.main()
