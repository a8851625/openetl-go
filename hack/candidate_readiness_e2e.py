"""A read-only post-build certificate is bound to this actual executable."""
import argparse
import json
from pathlib import Path
import shutil

from health_ui_e2e import Probe


class EvidenceProbe(Probe):
    def __init__(self, cli, image, output, manifest):
        super().__init__(cli, image, output)
        self.manifest = json.loads(Path(manifest).read_text())
        shutil.copy2(manifest, self.data / "connector-evidence.json")
        self.app_env = {"ETL_CONNECTOR_EVIDENCE_FILE": "/app/data/connector-evidence.json"}

    def start(self):
        # This is an API binding check; it needs neither a browser nor source,
        # sink or SQLite-client fixtures in the certification aggregation job.
        self.container("network", "create", self.network)
        self.container("run", "-d", "--name", self.app, "--network", self.network,
                       "-p", "127.0.0.1::8000", "-p", "127.0.0.1::8001",
                       "-v", str(self.data) + ":/app/data", "-v", str(self.pipes) + ":/app/pipes:ro",
                       "-e", "ETL_PROFILE=development", "-e", "ETL_INSECURE_DEV=true",
                       "-e", "ETL_API_TOKEN=", "-e", "ETL_SPEC_ENCRYPTION_KEY=",
                       "-e", "ETL_CONNECTOR_EVIDENCE_FILE=" + self.app_env["ETL_CONNECTOR_EVIDENCE_FILE"],
                       self.image)
        self.refresh_addresses()
        self.wait("candidate API", lambda: self.api("GET", "/api/v2/pipelines"))

    def close(self):
        logs = self.container("logs", self.app, check=False)
        (self.out / (self.app + ".log")).write_text(logs)
        self.container("rm", "-f", self.app, check=False)
        self.container("network", "rm", self.network, check=False)

    def gate(self):
        descriptors = self.api("GET", "/api/v2/connectors/descriptors")["descriptors"]
        mysql = next(d for d in descriptors if d["kind"] == "sink" and d["type"] == "mysql")
        return next(g for g in mysql["readiness"]["gates"] if g["code"] == "e2e_evidence")

    def verify(self):
        gate = self.gate()
        metadata = gate.get("evidence_metadata", {})
        if gate["status"] != "pass" or not metadata.get("verified") \
                or metadata.get("commit") != self.manifest["certified_commit"] \
                or metadata.get("image") != self.manifest["certified_image"] \
                or metadata.get("binary_sha256") != self.manifest["certified_binary_sha256"]:
            raise RuntimeError("current executable did not accept its actual post-build evidence")
        self.record("source/executable-bound external certification becomes visible without a rebuild")
        wrong = {**self.manifest, "certified_binary_sha256": "0" * 64,
                 "records": [{**r, "binary_sha256": "0" * 64} for r in self.manifest["records"]]}
        (self.data / "connector-evidence.json").write_text(json.dumps(wrong))
        if self.gate()["status"] == "pass":
            raise RuntimeError("wrong executable certificate silently passed")
        self.record("a certificate for another executable fails closed")
        (self.data / "connector-evidence.json").write_text(json.dumps(self.manifest))
        if self.gate()["status"] != "pass":
            raise RuntimeError("correct evidence did not recover")
        self.record("correct evidence restores readiness on the unchanged image")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args()
    probe = EvidenceProbe(args.cli, args.image, args.out, args.manifest)
    result = "failed"
    try:
        probe.start()
        probe.verify()
        result = "passed"
    finally:
        probe.close()
        Path(args.out, "result.json").write_text(json.dumps({
            "image": args.image, "image_id": probe.container("image", "inspect", "--format", "{{.Id}}", args.image),
            "checks": probe.checks, "result": result}, indent=2))


if __name__ == "__main__":
    main()
