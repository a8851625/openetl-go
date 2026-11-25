"""Persisted connection references, encrypted restart and real UI protection."""
import base64
import json
import urllib.error
import urllib.request

from health_ui_e2e import Probe, main


class ConnectionProbe(Probe):
    def __init__(self, cli, image, output):
        super().__init__(cli, image, output)
        self.app_env = {"ETL_SPEC_ENCRYPTION_KEY": base64.b64encode(bytes(32)).decode(),
                        "ETL_SPEC_ENCRYPTION_KEY_ID": "fixture"}

    def status_api(self, method, path, body=None):
        request = urllib.request.Request(self.api_url + path, method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, json.load(error)

    def report(self):
        return self.api("GET", "/api/v2/connections/p1-target/references")

    def ui_check(self, expected, can_delete=False):
        code = "async (page) => {\n" \
            + "await page.goto(" + json.dumps(self.base + "/#/connections") + ");\n" \
            + "await page.getByTestId('connection-delete-p1-target').click();\n" \
            + "const dialog=page.getByTestId('confirm-dialog');\n" \
            + "await dialog.getByText(" + json.dumps(expected) + ", {exact:false}).first().waitFor();\n" \
            + "if ((await dialog.getByTestId('confirm-dialog-confirm').isDisabled()) === " \
            + json.dumps(can_delete) + ") throw new Error('incorrect delete admission');\n"
        if can_delete:
            code += "await dialog.getByTestId('confirm-dialog-confirm').click();\n" \
                + "await page.getByTestId('connection-delete-p1-target').waitFor({state:'hidden'});\n"
        else:
            code += "await page.screenshot({path:" + json.dumps(str(self.out / "blocked-reference.png")) + "});\n" \
                + "await dialog.getByTestId('confirm-dialog-cancel').click();\n"
        code += "return true;\n}"
        path = self.out / ("references-clear.js" if can_delete else "references-blocked.js")
        path.write_text(code)
        if self.browser("run-code", "--filename", str(path)) != "true":
            raise RuntimeError("connection UI assertion failed")

    def verify(self):
        source_config = {"count": 0, "interval_ms": 200,
            "fields": [{"name": "id", "type": "counter"}, {"name": "name", "type": "static:ok"}]}
        sink_config = {"host": self.mysql, "port": 3306, "user": "root",
            "password": "health_fixture_only", "database": "openetl_health"}
        for name, kind, typ, config in [
            ("p1-source", "source", "demo", source_config),
            ("p1-target", "sink", "mysql", sink_config),
            ("p1-transform", "transform", "identity", {}),
        ]:
            self.api("POST", "/api/v2/connections", {"name": name, "kind": kind, "type": typ, "config": config})
        behavior = {"table": "connection_probe", "batch_mode": "upsert", "pk_columns": ["id"], "auto_create": True}
        self.spec = {"name": "p1-connections-linear", "batch_size": 1,
            "source": {"type": "demo", "connection": "p1-source"},
            "transforms": [{"type": "identity", "connection_ref": "p1-transform"}],
            "sink": {"type": "mysql", "connection_ref": "p1-target", "config": behavior}}
        self.pipeline_id = self.api("POST", "/api/v2/pipelines", {"spec": self.spec})["id"]
        self.api("POST", "/api/v2/pipelines/" + self.pipeline_id + "/start")
        self.wait("connection runtime writes", lambda: self.metric()["records_written"] >= 3)
        self.record("connection references resolve through a real MySQL write")

        dag = {"name": "p1-connections-dag", "dag": {"nodes": [
            {"id": "read", "kind": "source", "plugin": "demo", "connection": "p1-source"},
            {"id": "shape", "kind": "transform", "plugin": "identity", "connection": "p1-transform"},
            {"id": "write", "kind": "sink", "plugin": "mysql", "connection": "p1-target", "config": behavior},
        ], "edges": [{"from": "read", "to": "shape"}, {"from": "shape", "to": "write"}]}}
        dag_id = self.api("POST", "/api/v2/pipelines", {"spec": dag})["id"]
        report = self.report()
        if not report["known"] or len(report["references"]) != 2 or not any(
                ref["pipeline_id"] == dag_id and ref.get("node_id") == "write" for ref in report["references"]):
            raise RuntimeError("linear/DAG persisted references missing")
        if "health_fixture_only" in json.dumps(report):
            raise RuntimeError("reference response exposed secret")
        self.record("linear and DAG stable identities with no config secrets")

        hot = {**self.spec, "name": "p1-connections-hot"}
        hot_file = self.out / "reference-hot.yaml"
        hot_file.write_text(json.dumps(hot))
        # Emit the filesystem event inside the VM kernel, where the runtime's
        # fsnotify watch lives. Host writes to a VM mount may not emit inotify.
        self.container("cp", str(hot_file), self.sqlite + ":/fixture-pipes/reference-hot.yaml")
        self.wait("YAML hot reference", lambda: len(self.report()["references"]) == 3)
        pipelines = self.api("GET", "/api/v2/pipelines")["pipelines"]
        hot_id = next(item["id"] for item in pipelines if item["name"] == hot["name"])
        self.record("YAML hot load participates in persisted reference protection")

        status, conflict = self.status_api("DELETE", "/api/v2/connections/p1-target")
        if status != 409 or len(conflict["references"]) != 3:
            raise RuntimeError("referenced delete did not return authoritative conflict")
        self.record("DELETE 409 returns current affected endpoints")
        self.ui_check("Remove these current pipeline references")
        self.record("UI lists affected pipelines and disables referenced deletion")

        if not self.sqlite_sql("SELECT substr(spec_yaml,1,7) FROM pipelines WHERE id='" + self.pipeline_id + "';").startswith("enc:v1:"):
            raise RuntimeError("fixture spec was not encrypted")
        self.container("restart", self.app)
        self.wait("encrypted reference restart", lambda: self.report()["known"] and len(self.report()["references"]) == 3)
        self.record("encrypted current references survive application restart")

        self.api("POST", "/api/v2/pipelines/" + self.pipeline_id + "/stop")
        inline = {**self.spec,
            "source": {"type": "demo", "config": source_config},
            "transforms": [{"type": "identity", "config": {}}],
            "sink": {"type": "mysql", "config": {**sink_config, **behavior}}}
        self.api("PUT", "/api/v2/pipelines", {"id": self.pipeline_id, "spec": inline})
        self.wait("current spec dereference", lambda: len(self.report()["references"]) == 2)
        self.record("current update releases references while retaining old spec versions")
        self.container("exec", "--user", "1001", self.sqlite, "rm", "/fixture-pipes/reference-hot.yaml")
        for pipeline_id in (dag_id, hot_id):
            self.api("POST", "/api/v2/pipelines/" + pipeline_id + "/stop")
            self.api("DELETE", "/api/v2/pipelines/" + pipeline_id)
        self.wait("all current references removed", lambda: self.report()["known"] and not self.report()["references"])

        legacy_id = "12345678-1234-4234-8234-123456789012"
        self.sqlite_sql("INSERT INTO pipelines (id,name,spec_yaml,status,desired_state,observed_state) VALUES ('"
            + legacy_id + "','p1-unreadable-legacy','broken: [','restore_failed','stopped','restore_failed');")
        self.container("restart", self.app)
        self.wait("legacy unknown reference", lambda: not self.report()["known"])
        status, conflict = self.status_api("DELETE", "/api/v2/connections/p1-target")
        if status != 409 or conflict["known"] or conflict["unreadable_specs"][0]["pipeline_id"] != legacy_id:
            raise RuntimeError("unknown legacy references did not fail closed")
        self.ui_check("Some saved specs cannot be read")
        self.record("unreadable legacy spec blocks deletion with a stable repair identity")
        self.api("DELETE", "/api/v2/pipelines/" + legacy_id)
        self.ui_check("No current pipeline references", can_delete=True)
        self.record("UI permits atomic deletion only after references and unknown specs are removed")


if __name__ == "__main__":
    main(ConnectionProbe)
