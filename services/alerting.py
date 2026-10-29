#!/usr/bin/env python3
"""
Alerting subscriber: turns the raw COP streams into operator alerts.

This is one plain subscriber on the enclave (CONUS) broker. It takes the
place of the stream-processing layer in the previous design: the broker
routes, the subscriber computes, and its output is just another event on the
mesh that the map, the audit queue and anything else can subscribe to.

Inputs
    direct      cop/*/track/v1/>              air, sea, land, ew tracks (latest value wins)
    direct      cop/intel/ioc/v1/>            DIA indicators (learned into the IOC set)
    guaranteed  q.cop.alerting                cyber incidents, SIGINT intercepts, jamming
                                              (topic-to-queue mapped, survives restarts)
Output
    guaranteed  cop/alert/{type}/v1/{aor}/{severity}/{entity}/new

Rules (kept deliberately small and readable; add more in RULES):
    fuel        friendly aircraft fuel below threshold
    readiness   ground unit fuel or ammo below threshold
    proximity   hostile track within range of a friendly carrier or facility
    cyber       CRITICAL incident, or source IP matches an IOC learned from the DIA feed
    sigint      emitter in FIRE_CONTROL mode, or CRITICAL threat level
    jamming     SATCOM interference with HIGH/CRITICAL mission impact
    missile     launch (CRITICAL, with time-to-impact), terminal phase, intercept/impact outcome
    interceptor each shot from the defender (MEDIUM), a miss (HIGH) -- filed against the inbound it is engaging
    inbound     one per launch, routed by whose asset it is: …/{track}/us -> IAMD agent, …/{track}/coalition -> coalition liaison agent
    ship-threat an inbound aimed at a US ship, or a hostile track inside 25 nm of one -> the ship-defense agent
    launch-site every third launch from one site -> the targeting agent
    uas         one-way-attack drone wave launch and outcome
"""
import json
import math
import os
import threading
import time
import uuid

from solace.messaging.messaging_service import MessagingService, RetryStrategy
from solace.messaging.resources.queue import Queue
from solace.messaging.resources.topic import Topic
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


HOST = os.environ.get("SOLACE_HOST", "tcp://localhost:55555")       # CONUS by default
VPN = os.environ.get("SOLACE_VPN", "cop")
USER = os.environ.get("SOLACE_USER", "cop-alerting")
PASSWORD = os.environ.get("APP_PASSWORD", "cop-demo")
QUEUE = "q.cop.alerting"

FUEL_CRITICAL = 25.0
FUEL_LOW = 35.0
GROUND_FUEL_LOW = 30.0
GROUND_AMMO_LOW = 40.0
PROXIMITY_NM = 60.0
SHIP_THREAT_NM = 25.0     # a hostile track inside this range of any US ship is a ship-threat (the ship-defense agent's trigger)
SHIP_THREAT_COOLDOWN_S = 900   # one ship-threat per (ship, contact) per 15 min for loitering tracks; launches are separate
LAUNCH_SITE_EVERY = 3     # a launch-site alert (the targeting agent's trigger) every N launches from one site
COOLDOWN_S = 120          # per (type, entity) for track-derived alerts

# The IOC list is learned from the DIA feed on the mesh (cop/intel/ioc/v1/>),
# so an IOC match here is a real cross-source correlation: DIA publishes an
# indicator over REST, the simulator's cyber incident mentions the same IP, and
# this subscriber joins the two. Starts empty.
KNOWN_BAD_IPS = set()


def nm(lat1, lon1, lat2, lon2):
    """Great-circle distance in nautical miles."""
    r = 3440.065
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def lvl(s):
    return str(s).strip().lower().replace("/", "-").replace("*", "-").replace(">", "-").replace(" ", "-")


class Alerting:
    def __init__(self):
        self.svc = None
        self.pub = None
        self.friendly_anchors = {}      # entity -> (lat, lon, name)   carriers + facilities
        self.us_ships = {}              # hull -> (lat, lon, name)      every US ship, for ship-threat
        self.site_launches = {}         # launch site id -> count
        self.last_fired = {}            # (type, entity) -> ts
        self.lock = threading.Lock()
        self.count = 0

    # ------------------------------------------------------------ connection
    def connect(self):
        props = {T.HOST: HOST, S.VPN_NAME: VPN, CP.NAME: "cop-alerting",
                 A.SCHEME_BASIC_USER_NAME: USER, A.SCHEME_BASIC_PASSWORD: PASSWORD}
        self.svc = (MessagingService.builder().from_properties(props)
                    .with_reconnection_retry_strategy(RetryStrategy.forever_retry(3000)).build())
        self.svc.connect()
        self.pub = self.svc.create_persistent_message_publisher_builder().build()
        self.pub.start()

        tracks = (self.svc.create_direct_message_receiver_builder()
                  .with_subscriptions([TopicSubscription.of("cop/*/track/v1/>"),
                                       TopicSubscription.of("cop/land/site/v1/>"),
                                       TopicSubscription.of("cop/intel/ioc/v1/>")])
                  .build())
        tracks.start()
        tracks.receive_async(_Handler(self.on_direct))

        reports = self.svc.create_persistent_message_receiver_builder().build(
            Queue.durable_non_exclusive_queue(QUEUE))
        reports.start()
        reports.receive_async(_Handler(self.on_report, ack_with=reports))
        print(f"✓ alerting connected {HOST} vpn={VPN} user={USER}; tracks via direct, reports via {QUEUE}")

    # ------------------------------------------------------------ publishing
    def alert(self, atype, severity, entity, message, topic_in, lat=None, lon=None, **extra):
        aor = topic_in.split("/")[4] if topic_in.count("/") >= 4 else "unknown"
        key = (atype, entity)
        now = time.time()
        with self.lock:
            ck = extra.pop("cooldown_key", None)
            if ck:
                key = (atype,) + tuple(ck)
            cd = SHIP_THREAT_COOLDOWN_S if atype == "ship-threat" and extra.get("contact_kind") in ("aircraft", "surface") else COOLDOWN_S
            if atype in ("fuel", "readiness", "proximity", "ship-threat") and now - self.last_fired.get(key, 0) < cd:
                return
            self.last_fired[key] = now
        body = {
            "alert_id": f"ALERT-{uuid.uuid4().hex[:8].upper()}",
            "alert_type": atype, "severity": severity, "entity": entity,
            "message": message, "lat": lat, "lon": lon,
            "source_topic": topic_in, "timestamp": int(now * 1000), **extra,
        }
        suffix = body.pop("topic_suffix", None) or "new"
        topic = f"cop/alert/{atype}/v1/{aor}/{lvl(severity)}/{lvl(entity)}/{lvl(suffix)}"
        self.pub.publish(json.dumps(body), Topic.of(topic))
        self.count += 1
        print(f"[ALERT] {severity:8} {atype:9} {entity:14} {message}")

    # ------------------------------------------------------------ direct stream
    def on_direct(self, topic, msg):
        if topic.startswith("cop/intel/ioc/"):
            if msg.get("ioc_type") == "ipv4" and msg.get("ioc_value"):
                if msg["ioc_value"] not in KNOWN_BAD_IPS:
                    KNOWN_BAD_IPS.add(msg["ioc_value"])
                    print(f"[IOC]   learned {msg['ioc_value']} ← {msg.get('threat_actor')} (DIA) · {len(KNOWN_BAD_IPS)} known")
            return
        self.on_track(topic, msg)

    # ------------------------------------------------------------ rules: tracks
    def on_track(self, topic, msg):
        parts = topic.split("/")
        domain, event, iff = parts[1], parts[2], parts[5]
        lat, lon = msg.get("lat"), msg.get("lon")
        if lat is None or lon is None:
            return

        if domain == "air" and iff == "friend":
            fuel = msg.get("fuel_pct", 100)
            tail = msg.get("tail_number", "?")
            if fuel < FUEL_CRITICAL:
                self.alert("fuel", "CRITICAL", tail, f"{msg.get('aircraft_type')} fuel {fuel:.0f}% — divert or tank now", topic, lat, lon, fuel_pct=fuel)
            elif fuel < FUEL_LOW and msg.get("status") in ("INTERCEPT", "STRIKE", "CAP"):
                self.alert("fuel", "HIGH", tail, f"{msg.get('aircraft_type')} fuel {fuel:.0f}% while {msg.get('status')}", topic, lat, lon, fuel_pct=fuel)

        if domain == "land" and event == "track":
            unit = msg.get("unit_id", "?")
            f, a = msg.get("fuel_level_pct", 100), msg.get("ammo_level_pct", 100)
            if f < GROUND_FUEL_LOW or a < GROUND_AMMO_LOW:
                self.alert("readiness", "HIGH", unit, f"{msg.get('unit_name')} fuel {f:.0f}% ammo {a:.0f}%", topic, lat, lon, fuel=f, ammo=a)

        # Friendly anchors we measure hostile proximity against
        if iff == "friend" and (domain == "sea" and msg.get("vessel_class") == "CARRIER"
                                or (domain == "land" and event == "site")):
            name = msg.get("vessel_type") or msg.get("unit_name")
            self.friendly_anchors[msg.get("hull_number") or msg.get("unit_id")] = (lat, lon, name)

        if iff == "friend" and domain == "sea" and msg.get("hull_number"):
            self.us_ships[msg["hull_number"]] = (lat, lon, msg.get("vessel_type"))

        if iff == "hostile" and domain in ("air", "sea"):
            ident = msg.get("tail_number") or msg.get("hull_number") or "?"
            # Ship-threat: a hostile track closing on any US ship. One alert per (ship, contact) per cooldown;
            # the ship-defense agent takes it from here (bearing, range, 1MC options for the OOD).
            # A hostile patrolling 20 nm away is background, not a threat: the rule needs intent (an INTERCEPT /
            # STRIKE / ATTACK status, which the exercise probe sets) inside 25 nm, or anything inside 8 nm.
            hostile_intent = str(msg.get("status", "")).upper() in ("INTERCEPT", "STRIKE", "ATTACK")
            for hull, (slat, slon, sname) in list(self.us_ships.items()):
                d = nm(lat, lon, slat, slon)
                if d < SHIP_THREAT_NM and (hostile_intent or d < SHIP_THREAT_NM / 3):
                    kind = msg.get("aircraft_type") or msg.get("vessel_type")
                    self.alert("ship-threat", "CRITICAL" if d < SHIP_THREAT_NM / 3 else "HIGH", hull,
                               f"{msg.get('country')} {kind} {ident} {d:.0f} nm from {sname} ({hull})", topic, lat, lon,
                               ship_id=hull, ship_name=sname, contact_id=ident, contact_kind="aircraft" if domain == "air" else "surface",
                               contact_type=kind, contact_country=msg.get("country"), contact_status=msg.get("status"), range_nm=round(d, 1), cooldown_key=(hull, ident))
            for aid, (alat, alon, aname) in list(self.friendly_anchors.items()):
                d = nm(lat, lon, alat, alon)
                if d < PROXIMITY_NM:
                    kind = msg.get("aircraft_type") or msg.get("vessel_type")
                    sev = "CRITICAL" if d < PROXIMITY_NM / 2 else "HIGH"
                    self.alert("proximity", sev, ident, f"{msg.get('country')} {kind} {d:.0f} nm from {aname}", topic, lat, lon, range_nm=round(d, 1), near=aid)
                    break

    # ------------------------------------------------------------ rules: reports
    def on_report(self, topic, msg):
        domain, event = topic.split("/")[1], topic.split("/")[2]
        if domain == "cyber":
            ioc = msg.get("source_ip") in KNOWN_BAD_IPS
            sev = msg.get("severity", "HIGH")
            if ioc or sev == "CRITICAL":
                tag = " [known IOC]" if ioc else ""
                self.alert("cyber", "CRITICAL" if ioc else sev, msg["incident_id"],
                           f"{msg.get('attack_type')} on {msg.get('target_facility')} by {msg.get('threat_actor')}{tag}",
                           topic, msg.get("target_lat"), msg.get("target_lon"),
                           target=msg.get("target_facility_id"), ioc_match=ioc, mitre=msg.get("mitre_attack"))
        elif domain == "sigint":
            if msg.get("emission_pattern") == "FIRE_CONTROL" or msg.get("threat_level") == "CRITICAL":
                who = msg.get("emitter_classification") or msg.get("category") or "unattributed"
                what = str(msg.get("signal_type", "signal")).replace("_", " ")
                mode = f" in {msg['emission_pattern']} mode" if msg.get("emission_pattern") else ""
                where = f" targeting {msg['target_facility']}" if msg.get("target_facility") else ""
                self.alert("sigint", "CRITICAL", msg["intercept_id"], f"{who}: {what}{mode}{where}",
                           topic, msg.get("lat"), msg.get("lon"), signal_type=msg.get("signal_type"))
        elif domain == "air" and event in ("missile-event", "uas-event"):
            kind = "missile" if event == "missile-event" else "uas"
            ev = msg.get("event")
            who = f"{msg.get('weapon')} {msg.get('weapon_class')}" + (f" wave of {msg.get('wave_size')}" if msg.get("wave_size", 1) > 1 else "")
            if ev == "launch":          # one alert per wave: members publish launch-member, which is counted but not alerted
                self.alert(kind, "CRITICAL" if kind == "missile" else "HIGH", msg["track_id"],
                           f"{who} LAUNCH from {msg.get('launch_site')} → {msg.get('target_name')} · TTI {msg.get('tti_s')} s",
                           topic, msg.get("lat"), msg.get("lon"), target=msg.get("target_id"), target_name=msg.get("target_name"),
                           weapon=msg.get("weapon"), tti_s=msg.get("tti_s"), launch_site=msg.get("launch_site"), wave_id=msg.get("wave_id"))
                # Routing alert for the agents: one per launch, last topic level says whose asset is under attack.
                # us -> the IAMD agent (and ship-defense for a ship); coalition -> the coalition liaison agent.
                aff = "coalition" if msg.get("target_coalition") else "us"
                self.alert("inbound", "CRITICAL", msg["track_id"],
                           f"{who} inbound on {msg.get('target_name')} [{msg.get('target_country', 'US')}] · TTI {msg.get('tti_s')} s",
                           topic, msg.get("lat"), msg.get("lon"), affiliation=aff, kind=kind, weapon=msg.get("weapon"), weapon_class=msg.get("weapon_class"),
                           attacker_country=msg.get("country"), target=msg.get("target_id"), target_name=msg.get("target_name"), target_type=msg.get("target_type"),
                           target_country=msg.get("target_country", "US"), target_host=msg.get("target_host"), tti_s=msg.get("tti_s"),
                           launch_site=msg.get("launch_site"), launch_site_id=msg.get("launch_site_id"), wave_id=msg.get("wave_id"), wave_size=msg.get("wave_size"),
                           cooldown_key=(msg.get("wave_id") or msg["track_id"],), topic_suffix=aff)
                # Ship-threat: the inbound is aimed at a US ship. The ship-defense agent works the OOD / DCC side.
                if msg.get("target_type") in ("VESSEL", "CARRIER") and not msg.get("target_coalition"):
                    self.alert("ship-threat", "CRITICAL", msg["target_id"],
                               f"{who} {msg['track_id']} inbound on {msg.get('target_name')} · TTI {msg.get('tti_s')} s",
                               topic, msg.get("target_lat"), msg.get("target_lon"), ship_id=msg["target_id"], ship_name=msg.get("target_name"),
                               contact_id=msg["track_id"], contact_kind=kind, contact_type=msg.get("weapon"), tti_s=msg.get("tti_s"),
                               wave_id=msg.get("wave_id"), cooldown_key=(msg["target_id"], msg.get("wave_id") or msg["track_id"]))
            if ev == "launch" and msg.get("launch_site_id"):      # a wave counts once
                site = msg["launch_site_id"]
                self.site_launches[site] = self.site_launches.get(site, 0) + 1
                n = self.site_launches[site]
                if n % LAUNCH_SITE_EVERY == 0:      # the targeting agent's trigger: this site keeps firing
                    self.alert("launch-site", "HIGH", site, f"{msg.get('launch_site')} ({msg.get('country')}): {n} launches, latest {msg.get('weapon')} → {msg.get('target_name')}",
                               topic, msg.get("launch_lat"), msg.get("launch_lon"), launch_site=msg.get("launch_site"), launches=n, country=msg.get("country"))
            elif ev == "terminal":
                self.alert(kind, "CRITICAL", msg["track_id"],
                           f"{who} TERMINAL on {msg.get('target_name')} · {msg.get('tti_s')} s · defended by {msg.get('defended_by')}",
                           topic, msg.get("lat"), msg.get("lon"), target=msg.get("target_id"), target_name=msg.get("target_name"), tti_s=msg.get("tti_s"))
            elif ev == "intercepted":
                self.alert(kind, "LOW", msg["track_id"], f"{who} INTERCEPTED by {msg.get('engaged_by')} short of {msg.get('target_name')}",
                           topic, msg.get("lat"), msg.get("lon"), target=msg.get("target_id"), target_name=msg.get("target_name"), outcome="INTERCEPTED", engaged_by=msg.get("engaged_by"))
            elif ev == "impact":
                self.alert(kind, "CRITICAL", msg["track_id"], f"{who} IMPACT on {msg.get('target_name')} — BDA required",
                           topic, msg.get("lat"), msg.get("lon"), target=msg.get("target_id"), target_name=msg.get("target_name"), outcome="IMPACT")
        elif domain == "air" and event == "interceptor-event":
            # the defender's fire control: one alert per shot, filed against the inbound so the COP locates it there
            kind = "missile" if msg.get("engaging_kind") == "missile" else "uas"
            ev = msg.get("event"); shot = f" · shot {msg.get('shot')}" if (msg.get("shot") or 1) > 1 else ""
            if ev == "launch":
                self.alert(kind, "MEDIUM", msg["engaging"], f"{msg.get('interceptor')} AWAY from {msg.get('shooter')} at {msg.get('engaging_weapon')} {msg.get('engaging')} · {msg.get('range_to_target_nm')} nm{shot} · {msg.get('mode')}",
                           topic, msg.get("lat"), msg.get("lon"), interceptor=msg.get("interceptor"), interceptor_id=msg.get("interceptor_id"), shooter=msg.get("shooter"), mode=msg.get("mode"), shot=msg.get("shot"))
            elif ev == "miss":
                self.alert(kind, "HIGH", msg["engaging"], f"{msg.get('interceptor')} MISS on {msg.get('engaging_weapon')} {msg.get('engaging')}{shot} — re-engaging",
                           topic, msg.get("lat"), msg.get("lon"), interceptor=msg.get("interceptor"), interceptor_id=msg.get("interceptor_id"), shooter=msg.get("shooter"), outcome="MISS", shot=msg.get("shot"))
        elif domain == "ew" and event == "jamming":
            sev = msg.get("mission_impact_severity")
            if sev in ("HIGH", "CRITICAL"):
                self.alert("jamming", sev, msg["incident_id"],
                           f"{msg.get('jammer_system')} jamming {msg.get('target_satellite')} — {msg.get('mission_impact')} for {', '.join(msg.get('affected_units', []))}",
                           topic, msg.get("jammer_lat"), msg.get("jammer_lon"),
                           satellite=msg.get("target_satellite"), affected_units=msg.get("affected_units"))


class _Handler(MessageHandler):
    def __init__(self, fn, ack_with=None):
        self.fn, self.ack_with = fn, ack_with

    def on_message(self, m):
        try:
            self.fn(m.get_destination_name(), json.loads(payload(m) or "{}"))
        except Exception as e:
            print(f"handler error on {m.get_destination_name()}: {e}")
        finally:
            if self.ack_with is not None:
                self.ack_with.ack(m)


if __name__ == "__main__":
    a = Alerting()
    a.connect()
    try:
        while True:
            time.sleep(30)
            print(f"[STATS] alerts published: {a.count}")
    except KeyboardInterrupt:
        pass
