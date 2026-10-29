#!/usr/bin/env python3
"""
Raw topic tap over MQTT: print every message the broker delivers for the given
subscriptions, the way the browser map sees them (MQTT on conus:1883, user cop-viewer).

    python3 scripts/tap_topic.py 'cop/air/uas/#' 'cop/air/uas-event/#'
    python3 scripts/tap_topic.py --broker fwd --port 1884 'cop/air/missile/#'      # the forward broker instead

Prints topic, size and the first 100 characters of the payload, plus a per-topic-prefix
count every 10 s. Consumes nothing (direct subscriptions only).
"""
import argparse
import json
import sys
import time

import paho.mqtt.client as mqtt

ap = argparse.ArgumentParser()
ap.add_argument("subs", nargs="+", help="MQTT subscriptions, e.g. 'cop/air/uas/#'")
ap.add_argument("--broker", default="localhost")
ap.add_argument("--port", type=int, default=1883)
ap.add_argument("--user", default="cop-viewer")
ap.add_argument("--password", default="cop-demo")
ap.add_argument("--quiet", action="store_true", help="counts only")
a = ap.parse_args()

counts = {}
t0 = time.time()


def on_connect(c, u, flags, rc, props=None):
    print(f"✓ connected {a.broker}:{a.port} as {a.user} (rc={rc}); subscribing {a.subs}", flush=True)
    for s in a.subs:
        c.subscribe(s, qos=0)


def on_message(c, u, m):
    p = m.topic.split("/")
    key = "/".join(p[:3])
    counts[key] = counts.get(key, 0) + 1
    if not a.quiet:
        body = m.payload.decode("utf-8", "replace")
        try:
            j = json.loads(body); short = " ".join(f"{k}={j[k]}" for k in ("track_id", "event", "phase", "tti_s", "target_id", "wave_id") if k in j)
        except Exception:
            short = body[:100]
        print(f"{time.strftime('%H:%M:%S')} {m.topic}  [{len(m.payload)} B]  {short}", flush=True)


cl = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"tap-{int(time.time()) % 100000}")
cl.username_pw_set(a.user, a.password)
cl.on_connect = on_connect
cl.on_message = on_message
cl.connect(a.broker, a.port, keepalive=30)
cl.loop_start()
try:
    while True:
        time.sleep(10)
        print(f"--- {int(time.time() - t0)} s: " + (", ".join(f"{k}={v}" for k, v in sorted(counts.items())) or "nothing received"), flush=True)
except KeyboardInterrupt:
    sys.exit(0)
