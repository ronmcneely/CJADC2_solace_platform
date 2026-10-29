#!/usr/bin/env python3
"""
Exercise control: stage a scenario on cue instead of waiting for the simulator's random tempo.

    python3 scripts/inject.py ship-attack                 # 3 drones at a random US ship, then a missile at it -> IAMD + ship-defense agents, 1MC panel
    python3 scripts/inject.py coalition-attack            # a wave or a missile at a coalition partner asset -> partner MOD notified, US operator decides, partner told
    python3 scripts/inject.py coalition-attack --target COAL-GBR-AKROTIRI
    python3 scripts/inject.py ship-attack --target DDG-89
    python3 scripts/inject.py missile  --target FAC-010   # one missile at Ali Al Salem (or --kind ship / --kind base for a random one)
    python3 scripts/inject.py uas --target CVN-72 --n 3   # a drone wave at the carrier
    python3 scripts/inject.py probe --target CG-70        # a hostile aircraft turns toward a ship: proximity + ship-threat alert
    python3 scripts/inject.py site-active --site LS-IR-02 # three quick launches from one site -> launch-site alert -> targeting agent

Publishes one message on cop/c2/action/v1/{aor}/exercise/{scenario}/inject over MQTT (user cop-viewer,
the only topic that username may publish); the generator subscribes to operator actions already and
treats domain EXERCISE as a scenario inject. Fusion ignores it (not a decision); the audit queue keeps it.
"""
import argparse
import json
import sys
import time

import paho.mqtt.client as mqtt

ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
ap.add_argument("scenario", choices=["ship-attack", "coalition-attack", "missile", "uas", "probe", "site-active"])
ap.add_argument("--target", help="hull number or facility id (default: random of the right kind)")
ap.add_argument("--kind", choices=["ship", "base", "coalition", "us"], help="for missile/uas without --target")
ap.add_argument("--n", type=int, help="wave size for uas / ship-attack")
ap.add_argument("--site", help="launch site id, e.g. LS-IR-02 (site-active, missile, uas)")
ap.add_argument("--class", dest="wclass", choices=["ballistic", "cruise"], help="missile class (missile scenario)")
ap.add_argument("--broker", default="localhost"); ap.add_argument("--port", type=int, default=1883)
ap.add_argument("--user", default="cop-viewer"); ap.add_argument("--password", default="cop-demo")
a = ap.parse_args()

body = {"action": "inject", "domain": "EXERCISE", "scenario": a.scenario, "operator": "exercise-control",
        "target_id": a.target, "target_kind": a.kind, "n": a.n, "site_id": a.site, "weapon_class": a.wclass, "timestamp": int(time.time() * 1000)}
topic = f"cop/c2/action/v1/centcom/exercise/{a.scenario}/inject"

cl = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"exercise-control-{int(time.time()) % 10000}")
cl.username_pw_set(a.user, a.password)
cl.connect(a.broker, a.port, keepalive=10)
cl.loop_start()
info = cl.publish(topic, json.dumps(body), qos=1)
info.wait_for_publish(timeout=5)
cl.loop_stop(); cl.disconnect()
print(f"✓ {topic}  {json.dumps({k: v for k, v in body.items() if v is not None and k not in ('timestamp', 'action', 'domain')})}")
print("  watch: the generator pane ([EXERCISE] …, then LAUNCH), the map's INBOUND and AGENT MESH panels, ship.html for the OOD")
