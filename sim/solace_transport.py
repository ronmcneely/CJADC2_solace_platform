"""
Transport layer for the COP simulators: routes each simulated record to a
hierarchical Solace topic with the right quality of service.

Topic shape (see docs/topics.md):

    cop/{domain}/{event}/v1/{aor}/{source-or-iff}/{entity}/{status}

QoS:
    Direct      : tracks and sites (high rate, latest value wins; eliding-friendly)
    Guaranteed  : incidents, intercepts, imagery, jamming, actions (must not be lost)

Ground-force tracks are additionally published over MQTT, the tactical
sensor transport, to show the broker accepting a second protocol on the same
topic space with no translation code.
"""
import json
import os
import threading
import time

from solace.messaging.messaging_service import MessagingService, RetryStrategy
from solace.messaging.resources.topic import Topic
from solace.messaging.config.solace_properties import transport_layer_properties as T
from solace.messaging.config.solace_properties import service_properties as S
from solace.messaging.config.solace_properties import authentication_properties as A
from solace.messaging.config.solace_properties import client_properties as CP

SOLACE_HOST = os.environ.get("SOLACE_HOST", "tcp://localhost:55556")   # fwd broker by default
SOLACE_VPN = os.environ.get("SOLACE_VPN", "cop")
SOLACE_USER = os.environ.get("SOLACE_USER", "sim-bft")
SOLACE_PASSWORD = os.environ.get("APP_PASSWORD", "cop-demo")
MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1884"))                      # fwd broker's MQTT port
MQTT_ENABLED = os.environ.get("MQTT_ENABLED", "1") == "1"

# Legacy stream names used inside the simulators, kept as stable keys.
AIRCRAFT = "aircraft"
NAVAL = "naval"
GPS = "ground"
CYBER = "cyber"
SIGINT = "sigint"
SATELLITE = "satellite"
SPECTRUM = "spectrum"
EMBM = "embm"
MISSILE = "missile"          # in-flight track, direct
MISSILE_EVENT = "missile-event"   # launch / terminal / intercepted / impact, guaranteed
UAS = "uas"
UAS_EVENT = "uas-event"
INTERCEPTOR = "interceptor"              # the defender's shot, direct
INTERCEPTOR_EVENT = "interceptor-event"  # launch / hit / miss, guaranteed


def aor(lat, lon):
    """Area of responsibility from position. Coarse boxes; enough to route on."""
    if lon >= 90:
        return "indopacom"
    if lat < 17 and lon < 44.5:
        return "africom"
    if lon < 33.5 or lat >= 36.5:
        return "eucom"
    return "centcom"


def lvl(s):
    """Make a value safe as a topic level: no slashes, wildcards or spaces."""
    return str(s).strip().lower().replace("/", "-").replace("*", "-").replace(">", "-").replace(" ", "-")


def iff(country):
    return "friend" if country == "US" else "hostile"


def route(stream, value):
    """Return (topic, guaranteed: bool) for a simulated record."""
    if stream == AIRCRAFT:
        return (f"cop/air/track/v1/{aor(value['lat'], value['lon'])}/{iff(value['country'])}/"
                f"{lvl(value['tail_number'])}/{lvl(value['status'])}", False)
    if stream == NAVAL:
        return (f"cop/sea/track/v1/{aor(value['lat'], value['lon'])}/{iff(value['country'])}/"
                f"{lvl(value['hull_number'])}/{lvl(value['status'])}", False)
    if stream == GPS:
        kind = "site" if value.get("speed_kph", 0) == 0 and str(value.get("unit_id", "")).startswith("FAC-") else "track"
        return (f"cop/land/{kind}/v1/{aor(value['lat'], value['lon'])}/friend/"
                f"{lvl(value['unit_id'])}/{lvl(value['mission_status'])}", False)
    if stream == CYBER:
        return (f"cop/cyber/incident/v1/{lvl(value['target_region']) if value['target_region'] != 'MOBILE_ASSET' else aor(value['target_lat'], value['target_lon'])}/"
                f"{lvl(value['target_facility_id'])}/{lvl(value['incident_id'])}/{lvl(value['severity'])}", True)
    if stream == SIGINT:
        return (f"cop/sigint/intercept/v1/{aor(value['lat'], value['lon'])}/{lvl(value['collector_id'])}/"
                f"{lvl(value['intercept_id'])}/{lvl(value['threat_level'])}", True)
    if stream == SATELLITE:
        return (f"cop/space/image/v1/{aor(value['center_lat'], value['center_lon'])}/{lvl(value['source_id'])}/"
                f"{lvl(value['image_id'])}/{lvl(value['sensor_type'])}", True)
    if stream == SPECTRUM:
        return (f"cop/ew/jamming/v1/{aor(value['jammer_lat'], value['jammer_lon'])}/{lvl(value['jammer_id'])}/"
                f"{lvl(value['incident_id'])}/{lvl(value['mission_impact_severity'])}", True)
    if stream in (MISSILE, UAS):
        return (f"cop/air/{stream}/v1/{aor(value['lat'], value['lon'])}/hostile/"
                f"{lvl(value['track_id'])}/{lvl(value['phase'])}", False)
    if stream in (MISSILE_EVENT, UAS_EVENT):
        return (f"cop/air/{stream}/v1/{aor(value['lat'], value['lon'])}/hostile/"
                f"{lvl(value['track_id'])}/{lvl(value['event'])}", True)
    if stream == INTERCEPTOR:
        return (f"cop/air/interceptor/v1/{aor(value['lat'], value['lon'])}/friend/"
                f"{lvl(value['interceptor_id'])}/{lvl(value['phase'])}", False)
    if stream == INTERCEPTOR_EVENT:
        return (f"cop/air/interceptor-event/v1/{aor(value['lat'], value['lon'])}/friend/"
                f"{lvl(value['interceptor_id'])}/{lvl(value['event'])}", True)
    if stream == EMBM:
        return (f"cop/ew/track/v1/{aor(value['lat'], value['lon'])}/{lvl(value['emitter_country'])}/"
                f"{lvl(value['track_id'])}/{lvl(value['threat_level'])}", False)
    raise ValueError(f"unknown stream {stream}")


class SolaceTransport:
    """One messaging service, one direct publisher, one persistent publisher."""

    def __init__(self):
        self.service = None
        self.direct = None
        self.persistent = None
        self._mqtt = None
        self._lock = threading.Lock()
        self.sent = {"direct": 0, "guaranteed": 0, "mqtt": 0}

    def connect(self):
        props = {
            T.HOST: SOLACE_HOST,
            S.VPN_NAME: SOLACE_VPN,
            CP.NAME: "sim-multidomain-smf",
            A.SCHEME_BASIC_USER_NAME: SOLACE_USER,
            A.SCHEME_BASIC_PASSWORD: SOLACE_PASSWORD,
        }
        for i in range(30):
            try:
                self.service = (MessagingService.builder().from_properties(props)
                                .with_reconnection_retry_strategy(RetryStrategy.forever_retry(3000))
                                .build())
                self.service.connect()
                self.direct = self.service.create_direct_message_publisher_builder().build()
                self.direct.start()
                self.persistent = self.service.create_persistent_message_publisher_builder().build()
                self.persistent.start()
                print(f"✓ Solace connected {SOLACE_HOST} vpn={SOLACE_VPN} user={SOLACE_USER}")
                return True
            except Exception as e:
                print(f"Attempt {i+1}: {e}")
                time.sleep(2)
        return False

    def publish(self, stream, value):
        topic, guaranteed = route(stream, value)
        payload = json.dumps(value)
        with self._lock:
            if guaranteed:
                self.persistent.publish(payload, Topic.of(topic))
                self.sent["guaranteed"] += 1
            else:
                self.direct.publish(payload, Topic.of(topic))
                self.sent["direct"] += 1
        if stream == GPS and MQTT_ENABLED:
            self._mqtt_publish(topic, payload)
        return topic

    def _mqtt_publish(self, topic, payload):
        """Ground tracks also ride MQTT into the same broker, same topic."""
        try:
            if self._mqtt is None:
                import paho.mqtt.client as mqtt
                cl = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="cop-ground-sensors")
                cl.username_pw_set(SOLACE_USER, SOLACE_PASSWORD)
                cl.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
                cl.loop_start()
                self._mqtt = cl
                print(f"✓ MQTT connected {MQTT_HOST}:{MQTT_PORT} (tactical sensor feed)")
            self._mqtt.publish(topic, payload, qos=0)
            self.sent["mqtt"] += 1
        except Exception as e:
            print(f"MQTT publish failed: {e}")
            self._mqtt = None

    def subscribe_actions(self, callback):
        """The closed loop: hand operator actions (cop/c2/action/v1/>) to the simulator."""
        from solace.messaging.resources.topic_subscription import TopicSubscription
        from solace.messaging.receiver.message_receiver import MessageHandler

        class H(MessageHandler):
            def on_message(self, m):
                try:
                    body = m.get_payload_as_string() or (m.get_payload_as_bytes() or b"").decode("utf-8", "replace")
                    callback(m.get_destination_name(), json.loads(body or "{}"))
                except Exception as e:
                    print(f"action handler error: {e}")
        rcv = (self.service.create_direct_message_receiver_builder()
               .with_subscriptions([TopicSubscription.of("cop/c2/action/v1/>")]).build())
        rcv.start(); rcv.receive_async(H())
        self._action_rcv = rcv
        print("✓ subscribed to operator actions (cop/c2/action/v1/>): engagements close the loop")

    def subscribe(self, topics, callback):
        """Direct subscriptions for anything the simulator needs to know from the mesh (partner tracks, …)."""
        from solace.messaging.resources.topic_subscription import TopicSubscription
        from solace.messaging.receiver.message_receiver import MessageHandler

        class H(MessageHandler):
            def on_message(self, m):
                try:
                    body = m.get_payload_as_string() or (m.get_payload_as_bytes() or b"").decode("utf-8", "replace")
                    callback(m.get_destination_name(), json.loads(body or "{}"))
                except Exception as e:
                    print(f"subscription handler error: {e}")
        rcv = (self.service.create_direct_message_receiver_builder()
               .with_subscriptions([TopicSubscription.of(t) for t in topics]).build())
        rcv.start(); rcv.receive_async(H())
        self._subs = getattr(self, "_subs", []) + [rcv]
        print(f"✓ subscribed to {topics}")
        return rcv

    def stats(self):
        return f"direct={self.sent['direct']} guaranteed={self.sent['guaranteed']} mqtt={self.sent['mqtt']}"

    def close(self):
        for p in (self.direct, self.persistent):
            try:
                p.terminate()
            except Exception:
                pass
        if self.service:
            self.service.disconnect()
