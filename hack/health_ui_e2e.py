"""Real API and browser checks for current failure/unknown health."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


class Probe:
    def __init__(self, cli, image, output):
        self.cli = cli
        self.image = image
        self.out = Path(output or tempfile.mkdtemp(prefix="openetl-health-ui-")).resolve()
        self.out.mkdir(parents=True, exist_ok=True)
        self.data = self.out / "data"
        self.pipes = self.out / "pipes"
        for directory in (self.data, self.pipes, self.data / "output"):
            directory.mkdir(parents=True, exist_ok=True)
            directory.chmod(0o777)
        self.prefix = "openetl-health-" + str(os.getpid())
        self.network = self.prefix + "-net"
        self.app = self.prefix + "-app"
        self.mysql = self.prefix + "-mysql"
        self.sqlite = self.prefix + "-sqlite"
        self.session = self.prefix
        self.checks = []
        self.api_url = ""
        self.base = ""
        self.app_env = {}

    def run(self, args, timeout=180, check=True):
        result = subprocess.run(args, cwd=self.out, capture_output=True, text=True, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError("command failed: " + " ".join(args[:3]) + "\n" + result.stderr + result.stdout)
        return result.stdout.strip()

    def container(self, *args, **kwargs):
        return self.run([self.cli, *args], **kwargs)

    def browser(self, *args):
        return self.run(["playwright-cli", "-s=" + self.session, "--raw", *args], timeout=90)

    def api(self, method, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(self.api_url + path, data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"{method} {path}: {error.code} {error.read().decode()}") from error

    def wait(self, description, predicate, timeout=120):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                last = predicate()
                if last:
                    return last
            except (OSError, RuntimeError) as error:
                last = str(error)
            time.sleep(0.5)
        raise RuntimeError(f"timeout waiting for {description}: {last}")

    def metric(self):
        metrics = self.api("GET", "/api/v2/metrics")["pipelines"]
        return next(item for item in metrics if item["id"] == self.pipeline_id)

    def mysql_sql(self, statement):
        return self.container("exec", self.mysql, "mysql", "-uroot", "-phealth_fixture_only",
                              "openetl_health", "-e", statement)

    def sqlite_sql(self, statement):
        # Both database clients must run in the same container host kernel.
        # Editing an active WAL from macOS across a VM bind mount bypasses its
        # advisory locks and can overwrite concurrent application writes.
        return self.container("exec", "--user", "1001", self.sqlite,
                              "sqlite3", "/app/data/etl.db", statement)

    def record(self, name):
        self.checks.append({"name": name, "result": "passed"})
        print("PASS " + name, flush=True)

    def browser_views(self, state, issue_text):
        routes = [
            ("overview", "#/overview"),
            ("list", "#/pipelines"),
            ("detail", "#/pipelines/" + self.pipeline_id + "/overview"),
            ("issues", "#/issues"),
        ]
        for view, route in routes:
            code = "async (page) => {\n" \
                + "await page.goto(" + json.dumps(self.base + "/" + route) + ");\n" \
                + "await page.getByText('p1-health-probe', {exact:false}).first().waitFor();\n"
            if view in ("list", "detail"):
                code += "await page.locator(" + json.dumps('[data-health="' + state + '"]') \
                    + ").first().waitFor({state:'visible',timeout:15000});\n"
            else:
                code += "await page.getByText(" + json.dumps(issue_text) \
                    + ", {exact:false}).first().waitFor({state:'visible',timeout:15000});\n"
            code += "return true;\n}"
            filename = self.out / (state + "-" + view + ".js")
            filename.write_text(code)
            result = self.browser("run-code", "--filename", str(filename))
            if result != "true":
                raise RuntimeError(f"{state} {view} browser assertion failed: {result}")
            self.record(state + " " + view)

    def start(self):
        self.container("network", "create", self.network)
        self.container("run", "-d", "--name", self.mysql, "--network", self.network,
                       "--tmpfs", "/var/lib/mysql:rw,size=1g",
                       "-e", "MYSQL_ROOT_PASSWORD=health_fixture_only", "-e", "MYSQL_ROOT_HOST=%",
                       "-e", "MYSQL_DATABASE=openetl_health", "docker.io/library/mysql:8.0")
        self.wait("MySQL", lambda: self.mysql_sql("SELECT 1;"))
        self.container("run", "-d", "--name", self.app, "--network", self.network,
                       "-p", "127.0.0.1::8000", "-p", "127.0.0.1::8001",
                       "-v", str(self.data) + ":/app/data", "-v", str(self.pipes) + ":/app/pipes:ro",
                       "-e", "ETL_PROFILE=development", "-e", "ETL_INSECURE_DEV=true",
                       "-e", "ETL_RESTORE_STRICT=false", "-e", "ETL_API_TOKEN=",
                       "-e", "ETL_SPEC_ENCRYPTION_KEY=",
                       *[part for key, value in self.app_env.items() for part in ("-e", key + "=" + value)],
                       self.image)
        front = self.container("port", self.app, "8000/tcp").splitlines()[0].rsplit(":", 1)[1]
        api = self.container("port", self.app, "8001/tcp").splitlines()[0].rsplit(":", 1)[1]
        self.base = "http://127.0.0.1:" + front
        self.api_url = "http://127.0.0.1:" + api
        self.wait("API", lambda: self.api("GET", "/api/v2/pipelines"))
        self.container("run", "-d", "--name", self.sqlite, "--volumes-from", self.app,
                       "-v", str(self.pipes) + ":/fixture-pipes",
                       "docker.io/library/alpine:3.19", "sh", "-c",
                       "apk add --no-cache sqlite && tail -f /dev/null")
        self.wait("SQLite fixture client", lambda: self.sqlite_sql("SELECT 1;"))
        self.browser("open", self.base)
        self.browser("localstorage-set", "etl_lang", "en")
        self.browser("reload")

    def verify(self):
        self.spec = {
            "name": "p1-health-probe", "batch_size": 1,
            "source": {"type": "demo", "config": {"count": 0, "interval_ms": 500,
                "fields": [{"name": "id", "type": "counter"}, {"name": "name", "type": "static:ok"}]}},
            "sink": {"type": "mysql", "config": {"host": self.mysql, "port": 3306, "user": "root",
                "password": "health_fixture_only", "database": "openetl_health", "table": "health_probe",
                "batch_mode": "upsert", "pk_columns": ["id"], "auto_create": True}},
            "retry": {"max_attempts": 1, "initial_interval_ms": 100, "max_interval_ms": 100},
            "circuit_breaker": {"max_failures": 2, "window_sec": 60, "cooldown_sec": 30},
        }
        created = self.api("POST", "/api/v2/pipelines", {"spec": self.spec})
        self.pipeline_id = created["id"]
        self.api("POST", "/api/v2/pipelines/" + self.pipeline_id + "/start")
        self.wait("initial writes", lambda: self.metric()["records_written"] >= 3)
        self.mysql_sql("UPDATE health_probe SET name='x'; ALTER TABLE health_probe MODIFY name VARCHAR(1);")
        self.wait("open write circuit", lambda: self.metric()["circuit_breaker_state"] == 1)
        self.browser_views("degraded", "write circuit open")
        self.mysql_sql("ALTER TABLE health_probe MODIFY name VARCHAR(255);")
        self.wait("write circuit recovery", lambda: self.metric()["circuit_breaker_state"] == 0)
        self.api("POST", "/api/v2/dlq/" + self.pipeline_id + "/replay")
        self.wait("healthy after replay", lambda: self.metric()["health"] == "healthy")
        self.record("healthy after circuit recovery and replay")

        self.sqlite_sql("ALTER TABLE dead_letters RENAME TO health_hidden_dlq;")
        self.wait("unknown backlog", lambda: self.metric()["health"] == "unknown")
        self.browser_views("unknown", "health unavailable")
        self.sqlite_sql("ALTER TABLE health_hidden_dlq RENAME TO dead_letters;")
        self.wait("known backlog recovery", lambda: self.metric()["health"] == "healthy")
        self.record("healthy after DLQ count recovery")

        self.api("POST", "/api/v2/pipelines/" + self.pipeline_id + "/stop")
        self.container("stop", self.app)
        broken = dict(self.spec)
        broken["source"] = {**self.spec["source"], "connection": "deleted-health-fixture"}
        stored = json.dumps(broken).replace("'", "''")
        self.sqlite_sql("UPDATE pipelines SET spec_yaml='" + stored
                        + "' WHERE id='" + self.pipeline_id + "';")
        self.container("start", self.app)
        self.wait("restoration failure", lambda: any(
            item["id"] == self.pipeline_id and item["status"] == "restore_failed"
            for item in self.api("GET", "/api/v2/pipelines")["pipelines"]))
        self.browser_views("failed", "restoration failed")
        self.browser("screenshot", "--filename", str(self.out / "restoration-failure.png"))

    def close(self):
        self.run(["playwright-cli", "-s=" + self.session, "close"], check=False)
        for name in (self.app, self.mysql, self.sqlite):
            logs = self.container("logs", name, check=False)
            (self.out / (name + ".log")).write_text(logs)
            self.container("rm", "-f", name, check=False)
        self.container("network", "rm", self.network, check=False)


def main(probe_type=Probe):
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--out", default="")
    args = parser.parse_args()
    probe = probe_type(args.cli, args.image, args.out)
    started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    result = "failed"
    try:
        probe.start()
        probe.verify()
        result = "passed"
    finally:
        probe.close()
        (probe.out / "result.json").write_text(json.dumps({
            "image": probe.image, "started_at": started,
            "image_id": probe.container("image", "inspect", "--format", "{{.Id}}", probe.image),
            "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "checks": probe.checks, "result": result,
        }, indent=2))
        print("Evidence: " + str(probe.out), flush=True)


if __name__ == "__main__":
    main()
