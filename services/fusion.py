#!/usr/bin/env python3
"""
Fusion service: enrichment in motion.

The previous design did this in a stream processor with a few dozen SQL
streams. Here it is one subscriber on the mesh that keeps small reference
tables learned from the agency feeds and joins the live streams against them,
then publishes the enriched record back onto the hierarchy under cop/fused/.
Every consumer downstream (the map, the audit queue, a partner) gets the same
enriched picture once; nobody re-derives it.

Reference tables (learned from the mesh, start empty)
    DIA country assessments   cop/intel/assessment/v1/>   by country code
    DIA threat actors         cop/intel/actor/v1/>        by actor name
    DIA indicators (IOC)      cop/intel/ioc/v1/>          by ipv4 value
    MDA (maritime) vessels of interest   cop/intel/voi/v1/>   by vessel name
    CENTCOM readiness         cop/intel/readiness/v1/>    by unit id

Joins
    hostile air/sea track  x  DIA assessment      -> cop/fused/air|sea   (direct)
    ground track           x  CENTCOM readiness   -> cop/fused/land      (direct)
    cyber incident         x  actor, IOC, country -> cop/fused/cyber     (guaranteed)
    AIS contact            x  MDA VOI             -> cop/fused/contact   (guaranteed)
    operator action        x  the full target record x every piece of intel on it
                           x  the alerts that led to it x the timeline
                                                   -> cop/decision/record (guaranteed)
                              The DECISION RECORD: one self-describing event that a
                              downstream consumer (AI, dashboard, analytics, SOAR)
                              can use without re-joining anything.
    coalition air/naval    -> normalized track    -> cop/fused/air|sea   (direct)

Output topic shape:  cop/fused/{event}/v1/{aor}/{iff|facility}/{entity}/{status}
"""
import json
import os
import threading
import time

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
USER = os.environ.get("SOLACE_USER", "cop-fusion")
PASSWORD = os.environ.get("APP_PASSWORD", "cop-demo")
QUEUE = "q.cop.fusion"
REPLAY = os.environ.get("FUSION_REPLAY", "all")     # all | off : rebuild reference tables from the replay log at startup
STALE_INCIDENT_S = 600                              # replayed incidents older than this are not re-fused

DIRECT_SUBS = ["cop/air/track/v1/>", "cop/sea/track/v1/>", "cop/land/track/v1/>",
               "cop/sea/ais/v1/>", "cop/intel/coalition-air/v1/>", "cop/intel/coalition-naval/v1/>",
               "cop/alert/>", "cop/land/site/v1/>", "cop/air/missile/v1/>", "cop/air/uas/v1/>"]


def lvl(s):
    return str(s).strip().lower().replace("/", "-").replace("*", "-").replace(">", "-").replace(" ", "-")


class Fusion:
    def __init__(self):
        self.assessments = {}      # country -> rec
        self.actors = {}           # actor name -> rec
        self.iocs = {}             # ipv4 -> rec
        self.voi = {}              # vessel name -> rec
        self.readiness = {}        # unit id -> rec
        self.incidents = {}        # incident id -> (topic, rec), recent cyber incidents
        self.tracks = {}           # entity id -> (topic, rec), latest raw track (air/sea/land)
        self.alerts = {}           # entity id -> [alert recs], what the operator saw before deciding
        self.sites = {}            # facility id -> rec
        self.recommendations = {}  # recommendation id -> (topic, rec): what an agent proposed before the operator decided
        self.decisions = 0
        self.joins = {"air⋈dia": 0, "sea⋈dia": 0, "land⋈readiness": 0, "cyber⋈dia": 0, "ais⋈voi": 0, "coalition": 0, "decision-record": 0}
        self.lock = threading.Lock()
        self.voi_seen = set()
        self.started = time.time()

    # ------------------------------------------------------------ connection
    def connect(self):
        props = {T.HOST: HOST, S.VPN_NAME: VPN, CP.NAME: "cop-fusion",
                 A.SCHEME_BASIC_USER_NAME: USER, A.SCHEME_BASIC_PASSWORD: PASSWORD}
        self.svc = (MessagingService.builder().from_properties(props)
                    .with_reconnection_retry_strategy(RetryStrategy.forever_retry(3000)).build())
        self.svc.connect()
        self.direct = self.svc.create_direct_message_publisher_builder().build(); self.direct.start()
        self.persistent = self.svc.create_persistent_message_publisher_builder().build(); self.persistent.start()

        live = (self.svc.create_direct_message_receiver_builder()
                .with_subscriptions([TopicSubscription.of(s) for s in DIRECT_SUBS]).build())
        live.start(); live.receive_async(_Handler(self.on_live))

        # A late-joining service does not need the feeds restarted: it asks the
        # broker to replay the queue from the replay log and rebuilds its reference
        # tables from history, then carries on live.
        rb = self.svc.create_persistent_message_receiver_builder()
        if REPLAY == "all":
            from solace.messaging.config.replay_strategy import ReplayStrategy
            rb = rb.with_message_replay(ReplayStrategy.all_messages())
        ref = rb.build(Queue.durable_exclusive_queue(QUEUE))   # exclusive: replay is only allowed on exclusive queues
        ref.start(); ref.receive_async(_Handler(self.on_reference, ack_with=ref))
        print(f"✓ fusion connected {HOST} vpn={VPN} user={USER}; live via direct ({len(DIRECT_SUBS)} subs), reference via {QUEUE}"
              + (" (replaying the log to rebuild reference tables)" if REPLAY == "all" else ""))

    def publish(self, topic, body, guaranteed=False):
        (self.persistent if guaranteed else self.direct).publish(json.dumps(body), Topic.of(topic))

    # ------------------------------------------------------------ reference tables (guaranteed, via queue)
    def on_reference(self, topic, m):
        p = topic.split("/")
        domain, event = p[1], p[2]
        with self.lock:
            if domain == "intel" and event == "assessment":
                self.assessments[m.get("country_code")] = m
            elif domain == "intel" and event == "actor":
                self.actors[m.get("name")] = m
            elif domain == "intel" and event == "ioc":
                if m.get("ioc_type") == "ipv4" and m.get("ioc_value"):
                    self.iocs[m["ioc_value"]] = m
            elif domain == "intel" and event == "voi":
                # a VOI advisory is good for an hour; replayed older ones would pin stale hulls as vessels of interest
                ts = m.get("timestamp")
                try:
                    from datetime import datetime, timezone
                    age = time.time() - datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp() if isinstance(ts, str) else time.time() - float(ts) / 1000
                except Exception:
                    age = 0
                if age < 3600:
                    self.voi[m.get("vessel_name")] = m
            elif domain == "intel" and event == "readiness":
                self.readiness[m.get("unit_id")] = m
            elif domain == "cyber" and event == "incident":
                self.incidents[m.get("incident_id")] = (topic, m)
                if len(self.incidents) > 300:
                    self.incidents.pop(next(iter(self.incidents)))
                self.fuse_cyber(topic, m)
            elif domain == "c2" and event == "action":
                self.fuse_action(topic, m)
            elif domain == "agent" and event == "recommendation":
                self.recommendations[m.get("recommendation_id")] = (topic, m)
                if len(self.recommendations) > 200:
                    self.recommendations.pop(next(iter(self.recommendations)))

    # ------------------------------------------------------------ live streams (direct)
    def on_live(self, topic, m):
        p = topic.split("/")
        domain, event, aor, src = p[1], p[2], p[4], p[5]
        if domain == "alert":
            ent = m.get("entity")
            if ent:
                self.alerts.setdefault(ent, []).append(m)
                self.alerts[ent] = self.alerts[ent][-10:]
            return
        if domain == "land" and event == "site":
            self.sites[m.get("unit_id")] = m
            return
        if domain == "air" and event in ("missile", "uas"):
            self.tracks[m.get("track_id")] = (topic, m)
            return
        if domain == "air" and event == "track":
            self.tracks[m.get("aircraft_id")] = (topic, m)
            self.fuse_track("air", aor, src, m.get("aircraft_id"), m.get("country"), m.get("status"), m)
        elif domain == "sea" and event == "track":
            self.tracks[m.get("vessel_id")] = (topic, m)
            self.fuse_track("sea", aor, src, m.get("vessel_id"), m.get("country"), m.get("status"), m)
        elif domain == "sea" and event == "ais":
            self.fuse_ais(aor, m)
        elif domain == "land" and event == "track":
            self.tracks[m.get("unit_id")] = (topic, m)
            self.fuse_land(aor, m)
        elif domain == "intel" and event == "coalition-air":
            self.fuse_coalition_air(aor, m)
        elif domain == "intel" and event == "coalition-naval":
            self.fuse_coalition_naval(aor, m)

    # hostile track ⋈ DIA country assessment
    def fuse_track(self, kind, aor, iff, entity, country, status, m):
        if iff != "hostile":
            return
        a = self.assessments.get(country)
        if not a:
            return
        out = {"fused_of": entity, "domain": kind, "source": "fusion",
               "dia_threat_level": a["threat_level"], "dia_country_name": a.get("country_name"),
               "dia_posture": a.get("cyber_posture"), "dia_priority": a.get("priority"),
               "dia_active_campaigns": a.get("active_campaigns"), "joined_at": int(time.time() * 1000)}
        self.publish(f"cop/fused/{kind}/v1/{aor}/hostile/{lvl(entity)}/{lvl(status)}", out)
        self.joins[f"{kind}⋈dia"] += 1

    # ground track ⋈ CENTCOM readiness
    def fuse_land(self, aor, m):
        r = self.readiness.get(m.get("unit_id"))
        if not r:
            return
        out = {"fused_of": m["unit_id"], "domain": "land", "source": "fusion",
               "readiness": r.get("readiness"), "training_status": r.get("training_status"),
               "equipment_or_rate": r.get("equipment_or_rate"), "personnel_available": r.get("personnel_available"),
               "personnel_assigned": r.get("personnel_assigned"), "readiness_as_of": r.get("last_assessment"),
               "joined_at": int(time.time() * 1000)}
        self.publish(f"cop/fused/land/v1/{aor}/friend/{lvl(m['unit_id'])}/{lvl(m.get('mission_status', 'active'))}", out)
        self.joins["land⋈readiness"] += 1

    # cyber incident ⋈ actor profile ⋈ IOC ⋈ country assessment
    def fuse_cyber(self, topic, m):
        ts = m.get("timestamp")
        if isinstance(ts, (int, float)) and time.time() - ts / 1000 > STALE_INCIDENT_S:
            return                                  # replayed history: learn nothing, publish nothing
        actor = self.actors.get(m.get("threat_actor"))
        ioc = self.iocs.get(m.get("source_ip"))
        a = self.assessments.get(m.get("actor_country"))
        if not (actor or ioc or a):
            return
        out = {"fused_of": m.get("incident_id"), "domain": "cyber", "source": "fusion", "joined_at": int(time.time() * 1000)}
        if actor:
            out.update({"dia_actor_name": actor.get("name"), "dia_affiliation": actor.get("affiliation"),
                        "dia_sophistication": actor.get("sophistication"), "dia_active_since": actor.get("active_since"),
                        "dia_known_operations": actor.get("known_operations")})
        if ioc:
            out.update({"ioc_id": ioc.get("ioc_id"), "ioc_match": "KNOWN_IOC", "ioc_malware_family": ioc.get("malware_family"),
                        "ioc_confidence": ioc.get("confidence")})
        if a:
            out.update({"dia_threat_level": a.get("threat_level"), "dia_country_name": a.get("country_name"), "dia_priority": a.get("priority")})
        p = topic.split("/")
        self.publish(f"cop/fused/cyber/v1/{p[4]}/{p[5]}/{lvl(m.get('incident_id'))}/{lvl(m.get('severity', 'high'))}", out, guaranteed=True)
        self.joins["cyber⋈dia"] += 1
        print(f"[FUSE]  cyber {m.get('incident_id')} ⋈ {'actor ' if actor else ''}{'ioc ' if ioc else ''}{'country' if a else ''}")

    # ------------------------------------------------------------ the decision record
    def fuse_action(self, topic, m):
        """Operator action -> one complete, self-describing record for downstream consumers."""
        if str(m.get("domain", "")).upper() == "EXERCISE":      # scenario injects are not decisions
            return
        p = topic.split("/")
        target = m.get("target_id")
        now_ms = int(time.time() * 1000)
        self.decisions += 1
        sources = [{"system": "COP operator", "topic": topic, "protocol": "MQTT/WebSocket"}]
        rec = {
            "decision_id": f"DEC-{now_ms:x}-{self.decisions:04d}",
            "schema": "cop.decision.record.v1",
            "decided_at": m.get("timestamp"),
            "operator": m.get("operator"),
            "action": m.get("action"),
            "action_domain": m.get("domain"),
            "action_details": m.get("details") or {},
            "scope": m.get("scope"),
            "target": {"id": target, "kind": "unknown", "record": None},
            "enrichment": {},
            "alerts": [a for a in self.alerts.get(target, [])],
            "timeline": {},
            "sources": sources,
            "produced_by": {"service": "cop-fusion", "at": now_ms},
        }
        inc = self.incidents.get(target)
        trk = self.tracks.get(target)
        reported_at = None
        if inc:
            itopic, i = inc
            rec["target"] = {"id": target, "kind": "cyber-incident", "record": i,
                             "summary": f"{i.get('attack_type')} on {i.get('target_facility')} by {i.get('threat_actor')}"}
            sources.append({"system": "Cyber incident system", "topic": itopic, "protocol": "SMF"})
            reported_at = i.get("timestamp")
            actor = self.actors.get(i.get("threat_actor")); ioc = self.iocs.get(i.get("source_ip")); a = self.assessments.get(i.get("actor_country"))
            if actor:
                rec["enrichment"]["dia_threat_actor"] = actor; sources.append({"system": "DIA threat actors", "topic": "cop/intel/actor/v1/global/dia/…", "protocol": "REST"})
            if ioc:
                rec["enrichment"]["dia_ioc_match"] = ioc; sources.append({"system": "DIA indicators", "topic": "cop/intel/ioc/v1/global/dia/…", "protocol": "REST"})
            if a:
                rec["enrichment"]["dia_country_assessment"] = a; sources.append({"system": "DIA country assessment", "topic": "cop/intel/assessment/v1/global/dia/…", "protocol": "REST"})
            fac = self.sites.get(i.get("target_facility_id"))
            if fac:
                rec["enrichment"]["target_facility"] = fac; sources.append({"system": "Facilities", "topic": "cop/land/site/v1/…", "protocol": "SMF"})
            mob = self.tracks.get(i.get("target_facility_id"))
            if mob:
                rec["enrichment"]["target_asset_now"] = mob[1]; sources.append({"system": "Track of the targeted asset", "topic": mob[0], "protocol": "SMF"})
        elif trk:
            ttopic, t = trk
            kind = "missile-track" if t.get("kind") == "missile" else "uas-track" if t.get("kind") == "uas" else "air-track" if "aircraft_id" in t else "sea-track" if "vessel_id" in t else "land-track"
            rec["target"] = {"id": target, "kind": kind, "record": t,
                             "summary": (f"{t.get('weapon')} {t.get('weapon_class')} from {t.get('launch_site')} → {t.get('target_name')} · {t.get('phase')} · TTI {t.get('tti_s')} s" if kind in ("missile-track", "uas-track")
                                         else f"{t.get('aircraft_type') or t.get('vessel_type') or t.get('unit_name')} ({t.get('country', 'US')}) {t.get('status') or t.get('mission_status', '')}")}
            sources.append({"system": {"air-track": "Aircraft tracker", "sea-track": "Naval tracker", "land-track": "Ground-force GPS", "missile-track": "Missile warning (OPIR/radar)", "uas-track": "C-UAS sensors"}[kind], "topic": ttopic, "protocol": "SMF"})
            if kind in ("missile-track", "uas-track"):
                fac = self.sites.get(t.get("target_id"))
                if fac:
                    rec["enrichment"]["threatened_asset"] = fac; sources.append({"system": "Facilities", "topic": "cop/land/site/v1/…", "protocol": "SMF"})
                shp = self.tracks.get(t.get("target_id"))
                if shp:
                    rec["enrichment"]["threatened_asset"] = shp[1]; sources.append({"system": "Naval tracker", "topic": shp[0], "protocol": "SMF"})
            reported_at = t.get("timestamp")
            a = self.assessments.get(t.get("country"))
            if a:
                rec["enrichment"]["dia_country_assessment"] = a; sources.append({"system": "DIA country assessment", "topic": "cop/intel/assessment/v1/global/dia/…", "protocol": "REST"})
            r = self.readiness.get(target)
            if r:
                rec["enrichment"]["centcom_readiness"] = r; sources.append({"system": "CENTCOM readiness", "topic": "cop/intel/readiness/v1/centcom/…", "protocol": "REST"})
        # Agent Mesh: if the operator acted on an agent's recommendation, the record carries it (what was proposed, why,
        # and how long the human took to decide). A rejection is a decision too.
        rid = m.get("recommendation_id")
        if rid and rid in self.recommendations:
            rtopic, r = self.recommendations[rid]
            rec["enrichment"]["agent_recommendation"] = r
            rec["agent"] = {"recommendation_id": rid, "agent": r.get("agent"), "recommended_action": r.get("action"),
                            "outcome": {"reject_recommendation": "rejected", "acknowledge_recommendation": "overtaken by events"}.get(m.get("action"), "approved"),
                            "recommended_at": r.get("timestamp"),
                            "recommendation_to_decision_s": round((m.get("timestamp") - r.get("timestamp")) / 1000, 1) if isinstance(m.get("timestamp"), (int, float)) and isinstance(r.get("timestamp"), (int, float)) else None}
            sources.append({"system": f"Agent Mesh ({r.get('agent')})", "topic": rtopic, "protocol": "SMF (A2A)"})
        if m.get("domain") == "SHIP" and rec["target"]["kind"] == "sea-track":
            rec["target"]["summary"] = f"1MC on {rec['target']['record'].get('vessel_type')} ({target}): {m.get('announcement') or (m.get('details') or {}).get('message', '')}"
        for a in rec["alerts"]:
            sources.append({"system": "Alerting service", "topic": a.get("source_topic", "cop/alert/…"), "protocol": "SMF"})
        decided = m.get("timestamp")
        first_alert = min((a.get("timestamp") for a in rec["alerts"] if isinstance(a.get("timestamp"), (int, float))), default=None)
        rec["timeline"] = {"reported_at": reported_at, "first_alert_at": first_alert, "decided_at": decided,
                           "report_to_decision_s": round((decided - reported_at) / 1000, 1) if isinstance(decided, (int, float)) and isinstance(reported_at, (int, float)) else None,
                           "alert_to_decision_s": round((decided - first_alert) / 1000, 1) if isinstance(decided, (int, float)) and first_alert else None}
        # de-duplicate sources by (system, topic)
        seen = set(); rec["sources"] = [x for x in sources if not ((x["system"], x["topic"]) in seen or seen.add((x["system"], x["topic"])))]
        self.publish(f"cop/decision/record/v1/{p[4]}/{p[5]}/{lvl(target)}/{lvl(m.get('action', 'action'))}", rec, guaranteed=True)
        self.joins["decision-record"] += 1
        tl = rec["timeline"]
        print(f"[DECISION] {rec['decision_id']} {m.get('action')} on {target} ({rec['target']['kind']}) · {len(rec['sources'])} sources, {len(rec['alerts'])} alerts, {len(rec['enrichment'])} enrichments"
              + (f" · decided {tl['report_to_decision_s']}s after the report" if tl.get('report_to_decision_s') is not None else ""))

    # AIS contact ⋈ MDA vessel of interest
    def fuse_ais(self, aor, m):
        v = self.voi.get(m.get("vessel_name"))
        if not v:
            return
        key = (m.get("mmsi"), v.get("alert_id"))
        first = key not in self.voi_seen
        self.voi_seen.add(key)
        out = {**m, "fused_of": m.get("mmsi"), "domain": "contact", "source": "fusion", "voi": True,
               "voi_alert_id": v.get("alert_id"), "voi_reason": v.get("reason"), "voi_severity": v.get("severity"),
               "joined_at": int(time.time() * 1000)}
        # First sighting is guaranteed (it is a report); position updates are direct.
        self.publish(f"cop/fused/contact/v1/{aor}/voi/{lvl(m.get('mmsi'))}/{lvl(v.get('severity', 'high'))}", out, guaranteed=first)
        self.joins["ais⋈voi"] += 1
        if first:
            print(f"[FUSE]  AIS {m.get('vessel_name')} ({m.get('flag_country')}) ⋈ VOI {v.get('severity')}: {v.get('reason')}")

    # coalition tracks normalized into the COP's own track schema
    def fuse_coalition_air(self, aor, t):
        out = {"aircraft_id": t["track_id"], "tail_number": t.get("callsign"), "aircraft_type": t.get("aircraft_type"),
               "role": t.get("mission_type"), "country": t.get("nation"), "iff_mode": "FRIENDLY", "status": t.get("mission_type"),
               "lat": t["lat"], "lon": t["lon"], "altitude_ft": t.get("altitude_ft"), "heading_deg": t.get("heading_deg"),
               "speed_kts": t.get("speed_knots"), "weapons": "coalition asset", "timestamp": int(time.time() * 1000),
               "source": "fusion", "coalition": {"nation": t.get("nation_name"), "alliance": t.get("alliance"), "iff": t.get("iff_mode"), "feed": t.get("source")}}
        self.publish(f"cop/fused/air/v1/{aor}/friend/{lvl(t['track_id'])}/{lvl(t.get('mission_type', 'active'))}", out)
        self.joins["coalition"] += 1

    def fuse_coalition_naval(self, aor, t):
        out = {"vessel_id": t["track_id"], "hull_number": t["track_id"], "vessel_type": t.get("vessel_name"),
               "vessel_class": t.get("hull_type"), "country": t.get("nation"), "iff_mode": "FRIENDLY", "status": t.get("mission"),
               "lat": t["lat"], "lon": t["lon"], "heading_deg": t.get("heading_deg"), "speed_kts": t.get("speed_knots"),
               "weapons": "coalition asset", "timestamp": int(time.time() * 1000),
               "source": "fusion", "coalition": {"nation": t.get("nation_name"), "feed": t.get("source")}}
        self.publish(f"cop/fused/sea/v1/{aor}/friend/{lvl(t['track_id'])}/{lvl(t.get('mission', 'patrol'))}", out)
        self.joins["coalition"] += 1

    def tables(self):
        return {"assessments": len(self.assessments), "actors": len(self.actors), "iocs": len(self.iocs),
                "voi": len(self.voi), "readiness": len(self.readiness)}

    def stats(self):
        print(f"[STATS] joins {self.joins} · reference {self.tables()}")

    def publish_stats(self):
        """Service telemetry as an event: the console (or anything else) subscribes to it."""
        self.publish("cop/fused/stats/v1/global/fusion/cop-fusion/ok",
                     {"service": "cop-fusion", "tables": self.tables(), "joins": self.joins,
                      "uptime_s": int(time.time() - self.started), "timestamp": int(time.time() * 1000)})


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
    f = Fusion()
    f.connect()
    try:
        n = 0
        while True:
            time.sleep(10)
            f.publish_stats()
            n += 1
            if n % 3 == 0:
                f.stats()
    except KeyboardInterrupt:
        pass
