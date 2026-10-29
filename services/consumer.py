#!/usr/bin/env python3
"""
Queue consumer for the COP mesh. Binds to a queue, prints what arrives, acks.

    python3 services/consumer.py                       # q.cop.audit on CONUS
    python3 services/consumer.py --queue q.cop.cyber   # SOC consumer
    python3 services/consumer.py --queue q.cop.cyber --name soc-2   # second instance: round-robin
    python3 services/consumer.py --queue q.cop.audit --replay all   # rewind the replay log
    python3 services/consumer.py --queue q.cop.audit --replay 10m   # last ten minutes
    python3 services/consumer.py --slow 0.5            # ack slowly: watch the spool drain in Broker Manager

What it shows
  - Guaranteed delivery: messages that queued up while nobody was listening are
    delivered now, in order, and stay until acknowledged.
  - Consumer scaling: two instances on a non-exclusive queue share the stream
    with no rebalance and no coordination (Solace tutorial step 6).
  - Replay: a consumer that joined late can ask the broker for history.
"""
import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

from solace.messaging.messaging_service import MessagingService, RetryStrategy
from solace.messaging.resources.queue import Queue
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.config.solace_properties import transport_layer_properties as T
from solace.messaging.config.solace_properties import service_properties as S
from solace.messaging.config.solace_properties import authentication_properties as A


def payload(m):
    """Message body as text. SMF string payloads come back from get_payload_as_string();
    MQTT and REST publishers deliver a binary attachment, which only get_payload_as_bytes() sees."""
    s = m.get_payload_as_string()
    if s:
        return s
    b = m.get_payload_as_bytes()
    return b.decode("utf-8", "replace") if b else ""


HOST = os.environ.get("SOLACE_HOST", "tcp://localhost:55555")     # CONUS
VPN = os.environ.get("SOLACE_VPN", "cop")
USER = os.environ.get("SOLACE_USER", "cop-viewer")
PASSWORD = os.environ.get("APP_PASSWORD", "cop-demo")

C = {"dim": "\033[90m", "y": "\033[33m", "g": "\033[32m", "r": "\033[31m", "c": "\033[36m", "m": "\033[35m", "0": "\033[0m"}


def summary(topic, rec):
    """One line a human can read, per domain."""
    d = topic.split("/")[1]
    try:
        if d == "cyber":
            return f"{rec['severity']:8} {rec['attack_type']} on {rec['target_facility']} by {rec['threat_actor']} from {rec['source_ip']}"
        if d == "sigint":
            who = rec.get("emitter_classification") or rec.get("category") or ""
            tail = f" [{rec['emission_pattern']}]" if rec.get("emission_pattern") else (f" → {rec['target_facility']}" if rec.get("target_facility") else "")
            return f"{rec.get('threat_level','?'):8} {rec.get('signal_type')} {who} {rec.get('frequency_mhz','?')} MHz{tail}"
        if d == "ew":
            return f"{rec.get('mission_impact_severity','?'):8} {rec.get('jammer_system')} jamming {rec.get('target_satellite')} → {rec.get('mission_impact')}"
        if d == "space":
            return f"{rec['sensor_type']:8} {rec['source_id']} {rec['resolution_m']} m @ {rec['center_lat']:.2f},{rec['center_lon']:.2f} [{rec['classification']}]"
        if d == "c2":
            return f"{'ACTION':8} {rec.get('domain')} {rec.get('action')} by {rec.get('operator')} → {rec.get('details',{}).get('message','')}"
        if d == "alert":
            return f"{rec['severity']:8} {rec['alert_type']} {rec['entity']}: {rec['message']}"
        if d == "intel":
            return f"{'INTEL':8} {rec.get('agency','?')} {rec.get('feed','?')}: {rec.get('summary') or json.dumps(rec)[:80]}"
    except Exception:
        pass
    return json.dumps(rec)[:110]


class Printer(MessageHandler):
    def __init__(self, receiver, name, slow):
        self.receiver, self.name, self.slow = receiver, name, slow
        self.count = 0
        self.t0 = time.time()

    def on_message(self, msg):
        self.count += 1
        topic = msg.get_destination_name()
        try:
            rec = json.loads(payload(msg) or "{}")
        except Exception:
            rec = {}
        age = ""
        ts = rec.get("timestamp")
        if ts:
            secs = time.time() - ts / 1000
            age = f"{C['dim']}({'%.0fs' % secs if secs < 90 else '%.0fm' % (secs/60)} old){C['0']}"
        dom = topic.split("/")[1]
        col = {"cyber": "m", "sigint": "c", "ew": "y", "alert": "r", "c2": "g"}.get(dom, "0")
        print(f"{C['dim']}{self.name} #{self.count:<5}{C['0']} {C[col]}{dom:7}{C['0']} {summary(topic, rec)} {age}")
        print(f"{C['dim']}        {topic}{C['0']}")
        if self.slow:
            time.sleep(self.slow)
        self.receiver.ack(msg)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queue", default="q.cop.audit")
    ap.add_argument("--name", default=f"consumer-{os.getpid() % 1000}")
    ap.add_argument("--slow", type=float, default=0.0, help="seconds to hold each message before ack")
    ap.add_argument("--replay", help="'all' or a duration like 10m / 2h: replay from the broker's replay log")
    ap.add_argument("--host", default=HOST)
    a = ap.parse_args()

    props = {T.HOST: a.host, S.VPN_NAME: VPN, A.SCHEME_BASIC_USER_NAME: USER, A.SCHEME_BASIC_PASSWORD: PASSWORD}
    svc = (MessagingService.builder().from_properties(props)
           .with_reconnection_retry_strategy(RetryStrategy.forever_retry(3000)).build())
    svc.connect()

    builder = svc.create_persistent_message_receiver_builder()
    if a.replay:
        from solace.messaging.config.replay_strategy import ReplayStrategy
        if a.replay == "all":
            builder = builder.with_message_replay(ReplayStrategy.all_messages())
            print(f"{C['y']}replay: everything in the replay log{C['0']}")
        else:
            n, unit = int(a.replay[:-1]), a.replay[-1]
            delta = timedelta(**{{"s": "seconds", "m": "minutes", "h": "hours"}[unit]: n})
            since = datetime.now(timezone.utc) - delta
            builder = builder.with_message_replay(ReplayStrategy.time_based(since))
            print(f"{C['y']}replay: since {since.isoformat(timespec='seconds')}{C['0']}")

    rcv = builder.build(Queue.durable_non_exclusive_queue(a.queue))
    rcv.start()
    h = Printer(rcv, a.name, a.slow)
    rcv.receive_async(h)
    print(f"{C['g']}✓ {a.name} bound to {a.queue} on {a.host} as {USER}{C['0']}  (Ctrl-C to stop)")

    def stop(*_):
        rcv.terminate()
        svc.disconnect()
        print(f"\n{a.name}: {h.count} messages in {time.time()-h.t0:.0f}s")
        sys.exit(0)
    signal.signal(signal.SIGINT, stop)
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
