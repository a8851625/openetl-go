"""Durable recovery facts under a real sink outage and metadata failures."""
import hashlib
import json
from urllib.parse import urlencode

from connection_reference_ui_e2e import ConnectionProbe
from health_ui_e2e import main


class RecoveryProbe(ConnectionProbe):
    def container(self, *args, **kwargs):
        # Unlike the health-only probe, a service stop/restart must retain
        # MySQL's data. This named fixture volume is unique to this run.
        args = list(args)
        if args and args[0] == "run" and "--name" in args \
                and args[args.index("--name") + 1] == self.mysql and "--tmpfs" in args:
            index = args.index("--tmpfs")
            args[index:index + 2] = ["-v", self.prefix + "-mysql-data:/var/lib/mysql"]
        return super().container(*args, **kwargs)

    def close(self):
        super().close()
        self.container("volume", "rm", self.prefix + "-mysql-data", check=False)

    def detail(self, event_id):
        return self.api("GET", "/api/v2/recovery-events/" + event_id)

    def events(self, pipeline_id=None):
        return self.api("GET", "/api/v2/recovery-events?" + urlencode(
            {"pipeline_id": pipeline_id or self.pipeline_id}))

    def rows(self, pipeline_id=None):
        return self.api("GET", "/api/v2/dlq/" + (pipeline_id or self.pipeline_id))["items"]

    def ui(self, name, route, assertion):
        code = "async (page) => {\nawait page.goto(" + json.dumps(self.base + "/" + route) + ");\n" \
            + assertion + "\nif ((await page.locator('body').innerText()).includes('ui.refresh')) throw new Error('untranslated refresh label');\nreturn true;\n}"
        file = self.out / (name + ".js")
        file.write_text(code)
        result = self.browser("run-code", "--filename", str(file))
        if result != "true":
            raise RuntimeError(name + " browser assertion failed: " + result)
        self.record(name)

    def verify(self):
        self.api("POST", "/api/v2/connections", {
            "name": "p1-recovery-target", "kind": "sink", "type": "mysql",
            "config": {"host": self.mysql, "port": 3306, "user": "root",
                       "password": "health_fixture_only", "database": "openetl_health"}})
        self.spec = {
            "name": "p1-recovery-probe", "batch_size": 1,
            "checkpoint_interval_sec": 1,
            "source": {"type": "demo", "config": {"count": 0, "interval_ms": 200,
                "fields": [{"name": "id", "type": "counter"}, {"name": "name", "type": "static:ok"}]}},
            "sink": {"type": "mysql", "connection_ref": "p1-recovery-target",
                     "config": {"table": "recovery_probe", "batch_mode": "upsert",
                                "pk_columns": ["id"], "auto_create": True}},
            "retry": {"max_attempts": 1, "initial_interval_ms": 100, "max_interval_ms": 100},
            "circuit_breaker": {"max_failures": 1, "window_sec": 60, "cooldown_sec": 2},
        }
        self.pipeline_id = self.api("POST", "/api/v2/pipelines", {"spec": self.spec})["id"]
        self.api("POST", "/api/v2/pipelines/" + self.pipeline_id + "/start")
        self.wait("initial writes", lambda: self.metric()["records_written"] >= 3)
        self.container("stop", "--time", "1", self.mysql)
        self.wait("durable outage recovery record", lambda: self.events()["events"])
        self.api("POST", "/api/v2/pipelines/" + self.pipeline_id + "/stop")
        event = self.events()["events"][0]
        event_id = event["id"]
        if event["pipeline_id"] != self.pipeline_id or event["endpoint"] != "sink" \
                or event.get("connection_name") != "p1-recovery-target" or not event["connection_known"]:
            raise RuntimeError("outage lost explicit capture associations")
        before = self.rows()
        if not before or any(row.get("recovery_event_id") != event_id for row in before):
            raise RuntimeError("outage DLQ association is missing")
        self.record("real MySQL service outage creates a stable recovery identity")

        preview = self.api("POST", "/api/v2/dlq/" + self.pipeline_id + "/replay?dry_run=1")
        current = self.detail(event_id)
        if preview["replayable"] != len(before) or self.rows() != before \
                or current["event"]["replayed_rows"] != 0 \
                or not any(op["state"] == "preview_passed" for op in current["operations"]):
            raise RuntimeError("dry-run did not retain facts without delivery mutation")
        self.record("identity dry-run persists preview without changing DLQ or delivery")
        status, response = self.status_api("POST", "/api/v2/dlq/" + self.pipeline_id + "/replay")
        failed = self.detail(event_id)
        if status < 400 or not self.rows() or not any(
                op["state"] in ("failed", "not_attempted") and op.get("error")
                for op in failed["operations"]):
            raise RuntimeError("failed replay outcome was lost")
        self.record("failed replay persists its error while retaining the backlog")

        context = {"pipeline_id": self.pipeline_id, "recovery_id": event_id,
                   "connection_name": "p1-recovery-target"}
        query = urlencode(context)
        record_selector = "[data-recovery-id='" + event_id + "']"
        for page in ("issues", "dlq", "connections"):
            extra = ""
            if page == "connections":
                extra = "await page.locator('[data-context-connection=\"p1-recovery-target\"]').waitFor();\n"
            self.ui("open-" + page + "-recovery-context", "#/" + page + "?" + query,
                    "await page.locator(" + json.dumps(record_selector) + ").waitFor();\n"
                    + extra + "await page.reload();\nawait page.locator("
                    + json.dumps(record_selector) + ").waitFor();")
        self.ui("detail-tabs-preserve-context",
                "#/pipelines/" + self.pipeline_id + "/issues?" + query,
                "await page.locator(" + json.dumps(record_selector) + ").waitFor();\n"
                "await page.getByRole('tab',{name:'Logs',exact:true}).click();\n"
                "if (!page.url().includes('recovery_id=" + event_id + "')) throw new Error('lost task context');")

        self.container("start", self.mysql)
        self.wait("restored MySQL", lambda: self.mysql_sql("SELECT 1;"))
        self.mysql_sql("CREATE TABLE recovery_sink_writes(id INT PRIMARY KEY,n INT NOT NULL); "
                       "INSERT INTO recovery_sink_writes VALUES(1,0); "
                       "CREATE TRIGGER recovery_insert_count AFTER INSERT ON recovery_probe FOR EACH ROW "
                       "UPDATE recovery_sink_writes SET n=n+1 WHERE id=1; "
                       "CREATE TRIGGER recovery_update_count AFTER UPDATE ON recovery_probe FOR EACH ROW "
                       "UPDATE recovery_sink_writes SET n=n+1 WHERE id=1;")
        row = before[0]
        row_id = row["row_id"]
        self.sqlite_sql("CREATE TRIGGER recovery_result_fault BEFORE UPDATE ON recovery_operations "
                        "WHEN NEW.state='completed' BEGIN SELECT RAISE(ABORT,'fixture result store outage'); END;")
        status, response = self.status_api("POST", "/api/v2/dlq/" + self.pipeline_id + "/" + row_id + "/replay")
        retained = next(item for item in self.rows() if item["row_id"] == row_id)
        if status != 503 or response.get("code") != "recovery_metadata_unavailable" \
                or retained["identity_context"]["replay_state"] != "sink_acked":
            raise RuntimeError("result failure did not retain the durable sink ACK: " + str(response))
        writes_after_ack = self.mysql_sql("SELECT n FROM recovery_sink_writes WHERE id=1;")
        self.sqlite_sql("DROP TRIGGER recovery_result_fault;")
        self.container("restart", self.app)
        self.wait("restarted API", lambda: self.api("GET", "/api/v2/pipelines"))
        if self.detail(event_id)["event"]["id"] != event_id:
            raise RuntimeError("restart changed identity")
        result = self.api("POST", "/api/v2/dlq/" + self.pipeline_id + "/" + row_id + "/replay")
        if result["replayed"] != 1 or self.mysql_sql("SELECT n FROM recovery_sink_writes WHERE id=1;") != writes_after_ack:
            raise RuntimeError("restart rewrote an acknowledged record")
        self.record("result persistence failure and restart finalize without a second sink write")
        while self.rows():
            self.api("POST", "/api/v2/dlq/" + self.pipeline_id + "/replay")
        final = self.detail(event_id)
        if final["event"]["state"] != "resolved" or final["event"]["replayed_rows"] != len(before) \
                or not any(op["state"] == "completed" for op in final["operations"]):
            raise RuntimeError("final durable replay result did not reconcile")
        self.container("restart", self.app)
        self.wait("final API restart", lambda: self.api("GET", "/api/v2/pipelines"))
        self.ui("retained-result-after-restart-and-row-removal", "#/dlq?" + urlencode({**context, "row_id": row_id}),
                "await page.locator('[data-recovery-state=\"resolved\"]').waitFor();\n"
                "await page.getByText('The selected DLQ row is no longer present',{exact:false}).waitFor();\n"
                "await page.locator('[data-operation-state=\"completed\"]').first().waitFor();\n"
                "await page.screenshot({path:" + json.dumps(str(self.out / "retained-recovery.png")) + ",fullPage:true,animations:'disabled'});")

        self.ui("missing-recovery-does-not-select-another-record",
                "#/issues?" + urlencode({**context, "recovery_id": "00000000-0000-0000-0000-000000000001"}),
                "await page.getByText('Recovery record unavailable',{exact:false}).waitFor();\n"
                "if (await page.locator('[data-recovery-id]').count()) throw new Error('selected another record');")
        self.ui("missing-pipeline-disables-DLQ-actions",
                "#/dlq?" + urlencode({**context, "pipeline_id": "00000000-0000-0000-0000-000000000002"}),
                "await page.getByText('The selected pipeline is unavailable',{exact:false}).waitFor();\n"
                "if (await page.getByTestId('dlq-confirm-replay').count()) throw new Error('fallback replay offered');")
        self.ui("mismatched-node-is-explicit",
                "#/issues?" + urlencode({**context, "node_id": "other-node"}),
                "await page.getByText('This link does not match the stored pipeline',{exact:false}).waitFor();")
        self.ui("sensitive-link-cleared",
                "#/dlq?" + query + "&api_token=fixture-never-echo",
                "await page.getByTestId('not-found-page').waitFor();\n"
                "if (page.url().includes('fixture-never-echo') || (await page.locator('body').innerText()).includes('fixture-never-echo')) throw new Error('secret retained');")

        # A real file checkpoint can advance only because the fallback DLQ write
        # succeeded, even though the optional event-capture transaction failed.
        fixture = self.out / "recovery_fallback.jsonl"
        fixture.write_text(json.dumps({"id": 10001, "name": "too-long"}) + "\n")
        self.container("cp", str(fixture), self.sqlite + ":/app/data/output/recovery_fallback.jsonl")
        self.mysql_sql("CREATE TABLE recovery_fallback(id BIGINT PRIMARY KEY,name VARCHAR(1));")
        fallback_spec = {
            "name": "p1-recovery-fallback", "batch_size": 1, "checkpoint_interval_sec": 1,
            "source": {"type": "file", "config": {"path": "/app/data/output/recovery_fallback.jsonl", "format": "json"}},
            "sink": {"type": "mysql", "connection_ref": "p1-recovery-target",
                     "config": {"table": "recovery_fallback", "batch_mode": "upsert", "pk_columns": ["id"]}},
            "retry": {"max_attempts": 1, "initial_interval_ms": 100, "max_interval_ms": 100},
        }
        fallback_id = self.api("POST", "/api/v2/pipelines", {"spec": fallback_spec})["id"]
        self.sqlite_sql("CREATE TRIGGER recovery_capture_fault BEFORE INSERT ON recovery_events "
                        "BEGIN SELECT RAISE(ABORT,'fixture capture metadata outage'); END;")
        self.api("POST", "/api/v2/pipelines/" + fallback_id + "/start")
        fallback_rows = self.wait("durable capture fallback", lambda: self.rows(fallback_id))
        self.wait("source checkpoint after durable fallback", lambda: self.sqlite_sql(
            "SELECT COUNT(*) FROM checkpoints WHERE job_name='" + fallback_id + "';") == "1")
        self.sqlite_sql("DROP TRIGGER recovery_capture_fault;")
        if any(item.get("recovery_event_id") or not item.get("recovery_tracking_error") for item in fallback_rows) \
                or self.events(fallback_id)["events"]:
            raise RuntimeError("metadata failure guessed an event")
        self.record("capture metadata failure retains DLQ and permits a safe file checkpoint")
        self.mysql_sql("ALTER TABLE recovery_fallback MODIFY name VARCHAR(255);")
        self.api("POST", "/api/v2/dlq/" + fallback_id + "/replay")
        orphaned = self.events(fallback_id)["unassociated_operations"]
        if self.rows(fallback_id) or not any(op["state"] == "completed" for op in orphaned):
            raise RuntimeError("unassociated fallback replay lost its outcome")
        self.record("unassociated capture fallback retains its replay result")
        self.ui("pipeline-switch-clears-previous-task-scope", "#/dlq?" + query,
                "await page.locator('.pipeline-row').filter({hasText:'p1-recovery-fallback'}).click();\n"
                "await page.waitForURL(url => url.hash.includes('pipeline_id=" + fallback_id + "'));\n"
                "if (page.url().includes('recovery_id=') || page.url().includes('row_id=')) throw new Error('kept old owner filters');")
        self.ui("missing-connection-is-explicit", "#/connections?connection_name=missing-recovery-fixture",
                "await page.getByText('The requested connection is no longer saved',{exact:false}).waitFor();\n"
                "if (await page.locator('[data-context-connection]').count()) throw new Error('highlighted an alternate connection');")

        for legacy_id in (90001, 90002):
            legacy_record = {**row["record"], "data": {"id": legacy_id, "name": "legacy"}}
            encoded = json.dumps(legacy_record)
            legacy_hash = hashlib.sha256(encoded.encode()).hexdigest()
            self.sqlite_sql("INSERT INTO dead_letters(job_name,record_json,error,error_class,attempt,record_hash,identity_context_json) VALUES('"
                            + self.pipeline_id + "','" + encoded.replace("'", "''")
                            + "','legacy fixture','unknown',1,'" + legacy_hash + "','{}');")
        legacy = next(item for item in self.rows() if item["record"]["data"]["id"] == 90001)
        if legacy.get("recovery_event_id") or legacy.get("recovery_association_known"):
            raise RuntimeError("legacy row gained a speculative association")
        self.record("legacy rows remain explicitly unassociated")
        legacy_query = urlencode({"pipeline_id": self.pipeline_id, "row_id": legacy["row_id"]})
        self.ui("legacy-association-visible", "#/dlq?" + legacy_query,
                "await page.getByText('legacy fixture',{exact:true}).click();\n"
                "await page.getByText('Recovery association unknown',{exact:true}).waitFor();")
        self.ui("purge-keeps-exact-row-scope", "#/dlq?" + legacy_query,
                "await page.getByRole('button',{name:'Delete All',exact:true}).click();\n"
                "await page.getByTestId('confirm-dialog-confirm').click();\n"
                "await page.getByText('The selected DLQ row is no longer present',{exact:false}).waitFor();")
        remaining_legacy = self.rows()
        if len(remaining_legacy) != 1 or remaining_legacy[0]["record"]["data"]["id"] != 90002:
            raise RuntimeError("scoped UI purge deleted unrelated legacy rows")
        self.api("DELETE", "/api/v2/pipelines/" + self.pipeline_id)
        if self.detail(event_id)["event"]["state"] != "resolved":
            raise RuntimeError("pipeline deletion removed a retained completed result")
        self.ui("deleted-pipeline-keeps-recovery-facts", "#/issues?" + query,
                "await page.locator('[data-recovery-state=\"resolved\"]').waitFor();")


if __name__ == "__main__":
    main(RecoveryProbe)
