#!/usr/bin/env python3
"""
SOAR / ticketing intake: the target of the broker's REST Delivery Point.

The broker (not an application) binds to q.cop.soar, which attracts every
cop/alert/> message, and POSTs each one here. This process has no Solace
client in it at all: it is a plain HTTP server, standing in for a SOAR,
ServiceNow, or any system that can only accept webhooks. The integration
is configured in the broker (broker/bootstrap.py, step 9) and visible in
Broker Manager under Clients > REST Delivery Points.

    GET  /          a page listing the last 100 alerts delivered
    GET  /alerts    the same as JSON
    POST /alerts    what the broker calls
"""
import json
import os
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8088"))
RECENT = deque(maxlen=100)
COUNT = {"n": 0}

PAGE = """<!doctype html><html><head><meta charset="utf-8"><meta http-equiv="refresh" content="5">
<title>SOAR intake</title>
<style>body{font:14px/1.4 system-ui,Segoe UI,sans-serif;background:#0e1726;color:#e6ebf2;margin:0;padding:24px}
h1{font-size:18px;margin:0 0 4px}p{color:#8fa3bf;margin:0 0 16px}table{border-collapse:collapse;width:100%}
th,td{text-align:left;padding:6px 10px;border-bottom:1px solid #22304a;font-size:13px}th{color:#8fa3bf;font-weight:600}
.sev{font-weight:700}.CRITICAL{color:#ff5c5c}.HIGH{color:#ffb020}.MEDIUM{color:#ffd93d}.ACTION{color:#7ee0a0}.NOTIFY{color:#7cc4ff}
code{color:#2fa7a0}.n{font-size:28px;font-weight:700;color:#e39a2b}</style></head><body>
<h1>SOAR intake <span class="n">@@N@@</span> <span style="color:#8fa3bf;font-weight:400">tickets opened</span></h1>
<p>Delivered by the broker's REST Delivery Point <code>rdp-soar</code> from queue <code>q.cop.soar</code> (subscriptions <code>cop/alert/&gt;</code>, <code>cop/decision/&gt;</code> and <code>cop/notify/&gt;</code>): alerts open tickets, decision records close them with the full context attached, the agents' notifications to host nations, airspace and the ship's watch go out from here. No Solace client in this process.</p>
<table><tr><th>received</th><th>severity</th><th>type</th><th>entity</th><th>message</th><th>source topic</th></tr>@@ROWS@@</table>
</body></html>"""


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        n = int(self.headers.get("content-length", 0))
        raw = self.rfile.read(n) if n else b""
        try:
            body = json.loads(raw or b"{}")
        except Exception:
            body = {"raw": raw.decode("utf-8", "replace")}
        body["_received"] = time.strftime("%H:%M:%S")
        body["_topic"] = self.headers.get("Solace-Topic") or self.headers.get("solace-topic") or ""
        body["_ticket"] = f"TKT-{COUNT['n'] + 1:05d}"
        if body.get("notification_id"):                                 # an agent's notification to a partner or the ship's watch
            body["alert_type"] = "notify"
            body["severity"] = "NOTIFY"
            body["entity"] = body.get("target_id") or body.get("ship_id") or body.get("track_id") or ""
            body["message"] = f"→ {body.get('recipient', '?')} [{body.get('priority', '')}] {body.get('subject', '')}: {body.get('message', '')}"
            body["source_topic"] = body.get("_topic") or "cop/notify/…"
        if body.get("decision_id"):                                     # a decision record, not an alert
            tg = body.get("target") or {}; tl = body.get("timeline") or {}
            body["alert_type"] = "decision"
            body["severity"] = "ACTION"
            body["entity"] = tg.get("id", "")
            body["message"] = (f"{str(body.get('action', '')).replace('_', ' ').upper()} by {body.get('operator', '?')} on {tg.get('summary', '')} · "
                               f"{len(body.get('sources', []))} sources, {len(body.get('alerts', []))} alerts, {len(body.get('enrichment') or {})} enrichments"
                               + (f" · decided {tl['report_to_decision_s']}s after the report" if tl.get("report_to_decision_s") is not None else ""))
            body["source_topic"] = body.get("_topic") or "cop/decision/record/v1/…"
        COUNT["n"] += 1
        RECENT.appendleft(body)
        print(f"[SOAR] {body['_ticket']} {body.get('severity','?'):8} {body.get('alert_type','?'):9} {body.get('entity','?'):14} {body.get('message','')}", flush=True)
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"ticket": body["_ticket"]}).encode())

    def do_GET(self):
        if self.path.startswith("/alerts"):
            out = json.dumps(list(RECENT)).encode()
            self.send_response(200); self.send_header("content-type", "application/json"); self.end_headers(); self.wfile.write(out)
            return
        rows = "".join(
            f"<tr><td>{a.get('_received','')}<br><code>{a.get('_ticket','')}</code></td><td class='sev {a.get('severity','')}'>{a.get('severity','')}</td>"
            f"<td>{a.get('alert_type','')}</td><td>{a.get('entity','')}</td><td>{a.get('message','')}</td><td><code>{a.get('source_topic','')}</code></td></tr>"
            for a in RECENT) or "<tr><td colspan=6>waiting for the first alert…</td></tr>"
        out = PAGE.replace("@@N@@", str(COUNT["n"])).replace("@@ROWS@@", rows).encode()
        self.send_response(200); self.send_header("content-type", "text/html; charset=utf-8"); self.end_headers(); self.wfile.write(out)


if __name__ == "__main__":
    print(f"SOAR intake listening on :{PORT}  (POST /alerts from the broker's RDP; GET / for the page)", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), H).serve_forever()
