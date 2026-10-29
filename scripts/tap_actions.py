#!/usr/bin/env python3
"""
Watch decisions flow through the broker, live, without consuming anything.

    python3 scripts/tap_actions.py                 # conus broker, q.cop.actions + q.cop.audit
    python3 scripts/tap_actions.py --queues q.cop.actions,q.cop.audit,q.cop.soar

Two things at once:
  * a DIRECT subscription to cop/c2/action/v1/> and cop/decision/>  — the
    broker hands this tap a copy of every decision and its enrichment; the
    queues are untouched (a direct subscriber is not a queue consumer)
  * the spooled depth of each queue from the SEMP monitor API, once a second —
    so when an operator clicks, the line appears here and the counts move.

The story it shows:  COP action -> cop/c2/action/... -> attracted by q.cop.actions,
q.cop.audit, q.cop.soar -> fusion enriches it -> cop/fused/action/... -> q.cop.audit.
"""
import argparse
import json
import os
import sys
import threading
import time

import requests
from solace.messaging.messaging_service import MessagingService, RetryStrategy
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.config.solace_properties import transport_layer_properties as T
from solace.messaging.config.solace_properties import service_properties as S
from solace.messaging.config.solace_properties import authentication_properties as A
from solace.messaging.config.solace_properties import client_properties as CP


def payload(m):
    """Message body as text. SMF string payloads come back from get_payload_as_string();
    MQTT and REST publishers deliver a binary attachment, which only get_payload_as_bytes() sees."""
    s = m.get_payload_as_string()
    if s:
        return s
    b = m.get_payload_as_bytes()
    return b.decode("utf-8", "replace") if b else ""


C = {"g": "\033[92m", "y": "\033[93m", "c": "\033[96m", "m": "\033[95m", "d": "\033[90m", "b": "\033[1m", "0": "\033[0m"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.environ.get("SOLACE_HOST", "tcp://localhost:55555"))
    ap.add_argument("--semp", default=os.environ.get("SEMP_URL", "http://localhost:8080"))
    ap.add_argument("--queues", default="q.cop.actions,q.cop.decisions,q.cop.audit")
    ap.add_argument("--full", action="store_true", help="print the whole decision record as JSON (what a downstream consumer gets)")
    ap.add_argument("--user", default="cop-viewer")
    ap.add_argument("--password", default=os.environ.get("APP_PASSWORD", "cop-demo"))
    a = ap.parse_args()
    queues = [q.strip() for q in a.queues.split(",") if q.strip()]
    sess = requests.Session(); sess.auth = ("admin", os.environ.get("SEMP_PASSWORD", "admin"))

    depth = {q: None for q in queues}
    lock = threading.Lock()

    def poll():
        while True:
            for q in queues:
                try:
                    r = sess.get(f"{a.semp}/SEMP/v2/monitor/msgVpns/cop/queues/{q}", timeout=2).json()["data"]
                    flows = sess.get(f"{a.semp}/SEMP/v2/monitor/msgVpns/cop/queues/{q}/txFlows", params={"count": 100}, timeout=2).json().get("data", [])
                    with lock:
                        depth[q] = (r.get("spooledMsgCount", r.get("msgSpoolUsage", "?")), len(flows))
                except Exception as e:
                    with lock:
                        depth[q] = ("?", f"semp error: {type(e).__name__}")
            time.sleep(1)
    threading.Thread(target=poll, daemon=True).start()

    def status_line():
        with lock:
            parts = []
            for q in queues:
                d = depth[q]
                parts.append(f"{C['c']}{q}{C['0']} {C['b']}{d[0] if d else '?'}{C['0']} queued, {d[1] if d else '?'} consumer(s)")
        return "   ".join(parts)

    class H(MessageHandler):
        def on_message(self, m):
            t = m.get_destination_name()
            try:
                r = json.loads(payload(m) or "{}")
            except Exception:
                r = {}
            ts = time.strftime("%H:%M:%S")
            if t.startswith("cop/c2/action/"):
                print(f"\r{C['d']}{ts}{C['0']} {C['y']}{C['b']}ACTION {C['0']}{C['y']}{str(r.get('action','')).replace('_',' ').upper():14}{C['0']} on {C['b']}{r.get('target_id','')}{C['0']} by {r.get('operator','')} · {(r.get('details') or {}).get('message','')}")
                print(f"           {C['d']}topic {t}{C['0']}")
                print(f"           {C['d']}→ attracted into: {', '.join(queues)}{C['0']}")
            else:
                tl = r.get("timeline") or {}; tg = r.get("target") or {}; en = r.get("enrichment") or {}
                lat = f" · decided {tl['report_to_decision_s']}s after the report" if tl.get("report_to_decision_s") is not None else ""
                print(f"\r{C['g']}{C['b']}DECISION RECORD {r.get('decision_id','')}{C['0']} {C['g']}{str(r.get('action','')).replace('_',' ').upper()}{C['0']} on {tg.get('id')} ({tg.get('kind')}): {tg.get('summary','')}{lat}")
                print(f"           {C['d']}merged: {len(r.get('sources', []))} sources · {len(r.get('alerts', []))} alerts · enrichment: {', '.join(en.keys()) or 'none'}{C['0']}")
                print(f"           {C['d']}sources: {'; '.join(x['system'] + ' (' + x['protocol'] + ')' for x in r.get('sources', []))}{C['0']}")
                print(f"           {C['d']}topic {t}  → q.cop.decisions (downstream), q.cop.audit, q.cop.soar{C['0']}")
                if a.full:
                    print(json.dumps(r, indent=2)[:6000])
            sys.stdout.flush()

    props = {T.HOST: a.host, S.VPN_NAME: "cop", CP.NAME: "tap-actions",
             A.SCHEME_BASIC_USER_NAME: a.user, A.SCHEME_BASIC_PASSWORD: a.password}
    svc = MessagingService.builder().from_properties(props).with_reconnection_retry_strategy(RetryStrategy.forever_retry(3000)).build()
    svc.connect()
    rcv = (svc.create_direct_message_receiver_builder()
           .with_subscriptions([TopicSubscription.of("cop/c2/action/v1/>"), TopicSubscription.of("cop/decision/>")]).build())
    rcv.start(); rcv.receive_async(H())
    print(f"{C['b']}tap{C['0']} connected to {a.host} as {a.user}; direct subscriptions on cop/c2/action/v1/> and cop/decision/> (queues untouched)")
    print(f"watching queue depth via SEMP monitor: {', '.join(queues)}\n")
    try:
        while True:
            sys.stdout.write("\r" + status_line() + "   " + C["d"] + "waiting for the next decision…" + C["0"] + "\033[K")
            sys.stdout.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
