from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
import json
import time
import threading

# Concurrency tracker: records the peak number of simultaneously in-flight
# requests so e2e can assert the enricher's concurrency cap is honored.
_INFLIGHT = 0
_INFLIGHT_LOCK = threading.Lock()
MAX_INFLIGHT = 0

# Per-key call counters so the e2e can assert that retries actually happened.
# Keyed by the trailing path segment (the user_id template value).
CALL_COUNTS = {}
CALL_COUNTS_LOCK = threading.Lock()


def _record_call(key):
    with CALL_COUNTS_LOCK:
        CALL_COUNTS[key] = CALL_COUNTS.get(key, 0) + 1
        return CALL_COUNTS[key]


def _max_inflight():
    with CALL_COUNTS_LOCK:
        return MAX_INFLIGHT


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        global _INFLIGHT, MAX_INFLIGHT
        parsed = urlparse(self.path)

        if parsed.path == "/health":
            body = b"ok"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if parsed.path == "/stats":
            # Report peak concurrency + per-key call counts for e2e assertions.
            payload = {
                "max_inflight": _max_inflight(),
                "call_counts": dict(_all_counts()),
            }
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # Enrichment endpoints: /users/<key> and /slow/<key> and /fail/<key>
        # and /throttle/<key>
        parts = parsed.path.strip("/").split("/")
        if len(parts) != 2:
            self.send_response(404)
            self.end_headers()
            return
        kind, key = parts[0], parts[1]

        # Track in-flight concurrency.
        with _INFLIGHT_LOCK:
            _INFLIGHT += 1
            if _INFLIGHT > MAX_INFLIGHT:
                MAX_INFLIGHT = _INFLIGHT
        try:
            self._handle(kind, key)
        finally:
            with _INFLIGHT_LOCK:
                _INFLIGHT -= 1

    def _handle(self, kind, key):
        if kind == "users":
            # Happy path: return enrichment JSON for any key.
            call = _record_call(key)
            payload = {"user_id": key, "tier": "vip", "call": call}
            self._json(200, payload)
            return

        if kind == "throttle":
            # Rate limiting: odd calls return 429 + Retry-After: 1, even calls
            # succeed. This is deterministic across fixture restarts so e2e
            # assertions don't depend on call-count history.
            call = _record_call(key)
            if call % 2 == 1:
                self.send_response(429)
                self.send_header("Retry-After", "1")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self._json(200, {"user_id": key, "tier": "vip", "call": call})
            return

        if kind == "fail500":
            # Always 500: transient, will be retried until max_retries exhausted.
            _record_call(key)
            self._json(500, {"error": "internal"})
            return

        if kind == "mixed":
            # Fail only for key=="bad", succeed otherwise. Used to test batch
            # partial-failure routing where only failed records enter DLQ.
            _record_call(key)
            if key == "bad":
                self._json(500, {"error": "internal"})
                return
            self._json(200, {"user_id": key, "tier": "vip"})
            return

        if kind == "missing":
            # 404: data-class error, not retried.
            _record_call(key)
            self._json(404, {"error": "not found"})
            return

        if kind == "slow":
            # Sleep longer than the enricher timeout to trigger context deadline.
            _record_call(key)
            time.sleep(3)
            self._json(200, {"user_id": key, "tier": "vip"})
            return

        self.send_response(404)
        self.end_headers()

    def _json(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return


def _all_counts():
    with CALL_COUNTS_LOCK:
        return dict(CALL_COUNTS)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
