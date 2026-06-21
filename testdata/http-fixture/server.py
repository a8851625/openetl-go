from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse
import json


ITEMS = [
    {"id": 1, "name": "HTTP Ada", "status": "active"},
    {"id": 2, "name": "HTTP Alan", "status": "active"},
    {"id": 3, "name": "HTTP Grace", "status": "deleted"},
]


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            body = b"ok"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path != "/customers":
            self.send_response(404)
            self.end_headers()
            return
        if self.headers.get("X-API-Key") != "secret-token":
            self.send_response(401)
            self.end_headers()
            return

        query = parse_qs(parsed.query)
        page = int(query.get("page", [1])[0])
        size = int(query.get("size", [2])[0])
        start = (page - 1) * size
        payload = {"items": ITEMS[start:start + size]}

        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return


HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
