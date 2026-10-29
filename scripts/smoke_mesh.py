#!/usr/bin/env python3
"""
Proves the mesh before any simulator exists.

Publishes a guaranteed message on the FWD broker and consumes it from a queue
on the CONUS broker. If this works, DMR is routing between sites.

    pip install solace-pubsubplus
    python scripts/smoke_mesh.py

Then:  scripts/link.sh down ; python scripts/smoke_mesh.py --publish-only ;
       scripts/link.sh up   ; python scripts/smoke_mesh.py --consume-only
to watch messages spool on fwd and drain to conus when the link returns.
"""
import argparse
import os
import time

from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.queue import Queue
from solace.messaging.resources.topic import Topic
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.config.solace_properties import transport_layer_properties as T
from solace.messaging.config.solace_properties import service_properties as S
from solace.messaging.config.solace_properties import authentication_properties as A

PW = os.environ.get("APP_PASSWORD", "cop-demo")


def service(host, user):
    props = {
        T.HOST: host,
        S.VPN_NAME: "cop",
        A.SCHEME_BASIC_USER_NAME: user,
        A.SCHEME_BASIC_PASSWORD: PW,
        T.RECONNECTION_ATTEMPTS: 20,
    }
    svc = MessagingService.builder().from_properties(props).build()
    svc.connect()
    return svc


class Printer(MessageHandler):
    def __init__(self, receiver):
        self.receiver = receiver
        self.count = 0

    def on_message(self, msg):
        self.count += 1
        print(f"  [conus q.cop.actions] {msg.get_destination_name()}  {msg.get_payload_as_string() or (msg.get_payload_as_bytes() or b"").decode("utf-8", "replace")}")
        self.receiver.ack(msg)


def publish(n):
    svc = service("tcp://localhost:55556", "sim-bft")       # FWD broker
    pub = svc.create_persistent_message_publisher_builder().build()
    pub.start()
    for i in range(n):
        topic = Topic.of(f"cop/c2/action/v1/centcom/cyber/fac-00{i%3+1}/block-ip")
        pub.publish(f'{{"seq":{i},"ts":{time.time():.0f},"action":"block_ip","operator":"smoke-test"}}', topic)
        print(f"  [fwd  publish ] {topic.get_name()}")
        time.sleep(0.2)
    pub.terminate()
    svc.disconnect()


def consume(seconds):
    svc = service("tcp://localhost:55555", "cop-viewer")    # CONUS broker
    rcv = svc.create_persistent_message_receiver_builder().build(Queue.durable_exclusive_queue("q.cop.actions"))
    rcv.start()
    h = Printer(rcv)
    rcv.receive_async(h)
    time.sleep(seconds)
    rcv.terminate()
    svc.disconnect()
    print(f"  consumed {h.count}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--publish-only", action="store_true")
    ap.add_argument("--consume-only", action="store_true")
    ap.add_argument("-n", type=int, default=5)
    a = ap.parse_args()
    if not a.consume_only:
        print("publishing on FWD …")
        publish(a.n)
    if not a.publish_only:
        print("consuming on CONUS …")
        consume(4)
