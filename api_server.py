import json
import os
import sys
from http.server import SimpleHTTPRequestHandler, HTTPServer

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT, "data")
STATE_FILE = os.path.join(DATA_DIR, "state.json")

def ensure_data():
    if not os.path.isdir(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.isfile(STATE_FILE):
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "orders": [],
                "skuHsn": [],
                "hsnPercent": [],
                "companyGstinDefault": "33ABNCS8962N1ZE",
                "lastInvoiceSeq": 0
            }, f)

class Handler(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        if self.path == "/api/state":
            ensure_data()
            try:
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                data = {"orders": [], "skuHsn": [], "hsnPercent": [], "companyGstinDefault": "", "lastInvoiceSeq": 0}
            payload = json.dumps(data).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        return super().do_GET()

    def do_POST(self):
        if self.path == "/api/state":
            ensure_data()
            length = int(self.headers.get("Content-Length", "0") or "0")
            body = self.rfile.read(length) if length > 0 else b"{}"
            try:
                data = json.loads(body.decode("utf-8"))
            except Exception:
                self.send_response(400)
                self.end_headers()
                return
            try:
                with open(STATE_FILE, "w", encoding="utf-8") as f:
                    json.dump(data, f)
                resp = json.dumps({"ok": True}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(resp)))
                self.end_headers()
                self.wfile.write(resp)
            except Exception:
                self.send_response(500)
                self.end_headers()
            return
        self.send_response(404)
        self.end_headers()

def main():
    os.chdir(ROOT)
    port = 8002
    if len(sys.argv) >= 2:
        try:
            port = int(sys.argv[1])
        except Exception:
            pass
    ensure_data()
    httpd = HTTPServer(("", port), Handler)
    print(f"API server running on http://localhost:{port}/ (static + /api/state)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()

if __name__ == "__main__":
    main()
