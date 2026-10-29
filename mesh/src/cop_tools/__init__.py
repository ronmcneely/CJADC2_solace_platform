"""
Tools the Agent Mesh agents use to see and act on the COP event mesh.

Every tool here is a plain Python function that Solace Agent Mesh exposes to an
agent's LLM (tool_type: python in the agent YAML). They share one Solace client
(the `Mesh` singleton) that keeps a live picture from direct subscriptions and
publishes the agents' outputs:

    cop/agent/recommendation/v1/{aor}/{agent}/{target}/{action}   guaranteed  needs a human APPROVE on the COP
    cop/agent/task/v1/{aor}/{agent}/{target}/{task}               guaranteed  tasking (ISR, BDA) for other systems
    cop/agent/report/v1/{aor}/{agent}/{target}/summary            guaranteed  the agent's narrative (written by the gateway)
    cop/notify/{channel}/v1/{aor}/{recipient}/{subject}/{priority} guaranteed  notifications: host-nation, airspace, ship (OOD/DCC)

Nothing an agent does fires a weapon. An engagement recommendation becomes an
action only when an operator approves it on the COP (or has set the panel to
AUTO), and the approval travels the same cop/c2/action topic a manual ENGAGE
does, so the fusion service, the audit queue and the SOAR cannot tell the two
apart except by the `recommendation_id` they carry.
"""
import asyncio
import json
import math
import os
import threading
import time
import uuid
from typing import Any, Dict, Optional

from solace.messaging.messaging_service import MessagingService, RetryStrategy
from solace.messaging.resources.topic import Topic
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.config.solace_properties import transport_layer_properties as T
from solace.messaging.config.solace_properties import service_properties as S

from . import slim_prompt  # noqa: F401  (side effect: trims the runtime's generic prompt blocks for the local 3B model)
from solace.messaging.config.solace_properties import authentication_properties as A
from solace.messaging.config.solace_properties import client_properties as CP

HOST = os.environ.get("COP_SOLACE_HOST", "tcp://conus:55555")
VPN = os.environ.get("COP_SOLACE_VPN", "cop")
USER = os.environ.get("COP_SOLACE_USER", "agent-mesh")
PASSWORD = os.environ.get("COP_SOLACE_PASSWORD", "cop-demo")

SUBS = ["cop/sea/track/v1/>", "cop/air/track/v1/>", "cop/land/site/v1/>",
        "cop/air/missile/v1/>", "cop/air/uas/v1/>", "cop/air/missile-event/v1/>", "cop/air/uas-event/v1/>",
        "cop/air/interceptor/v1/>", "cop/alert/>", "cop/c2/action/v1/>", "cop/agent/>", "cop/notify/>", "cop/decision/>"]

# Who gets told when a US asset on a partner's soil is attacked: the host nation's air-defence / joint ops
# watch, and for ships the coalition maritime watch and the merchant-shipping advisory.
HOST_NATION = {
    "Qatar":        ("qatar-joc",   "Qatar Armed Forces Joint Operations Center (MOD)"),
    "UAE":          ("uae-adoc",    "UAE Air Force & Air Defence Operations Centre"),
    "Kuwait":       ("kuwait-adc",  "Kuwait Air Defense Command (MOD)"),
    "Bahrain":      ("bdf-joc",     "Bahrain Defence Force JOC"),
    "Saudi Arabia": ("rsadf",       "Royal Saudi Air Defense Forces (MOD)"),
    "Iraq":         ("iraq-joc",    "Iraqi Joint Operations Command"),
    "Jordan":       ("jaf-joc",     "Jordan Armed Forces Joint Operations Center"),
    "Djibouti":     ("djibouti-mod","Djibouti Ministry of Defence / FAD"),
    "Turkey":       ("tuaf-caoc",   "Turkish Air Force / NATO CAOC"),
    "Oman":         ("rafo",        "Royal Air Force of Oman Air Defence"),
}
# Host nation of each US facility, for a site record that predates the generator carrying 'country'.
FACILITY_COUNTRY = {
    "FAC-001": "Qatar",   # Al Udeid AB
    "FAC-002": "UAE",   # Al Dhafra AB
    "FAC-003": "Kuwait",   # Camp Arifjan
    "FAC-004": "Bahrain",   # NSA Bahrain
    "FAC-005": "Saudi Arabia",   # Prince Sultan AB
    "FAC-006": "Iraq",   # Al Asad AB
    "FAC-007": "Turkey",   # Incirlik AB
    "FAC-009": "Jordan",   # Muwaffaq Salti AB
    "FAC-010": "Kuwait",   # Ali Al Salem AB
    "FAC-012": "Djibouti",   # Camp Lemonnier
    "FAC-013": "Djibouti",   # Chabelley Airfield
}
MARITIME = [("cmf-ctf153", "Combined Maritime Forces CTF-153 (Red Sea) / CTF-150"), ("ukmto", "UKMTO merchant shipping advisory")]
# Coalition partners: the national authority told when one of their assets is attacked and when US forces engage for them.
PARTNER = {
    "GBR": ("uk-pjhq",      "UK Permanent Joint Headquarters (PJHQ) Northwood / RN Maritime Operations Centre"),
    "FRA": ("fra-cpco",     "French Joint Staff CPCO (Centre de planification et de conduite des opérations) / ALINDIEN"),
    "AUS": ("aus-hqjoc",    "Australian Defence Force Headquarters Joint Operations Command (HQJOC)"),
    "DEU": ("deu-einsfukdo","Bundeswehr Joint Forces Operations Command (Einsatzführungskommando)"),
    "JPN": ("jpn-jso",      "Japan Joint Staff Office / JMSDF Self Defense Fleet HQ"),
    "SAU": ("rsadf",        "Royal Saudi Air Defense Forces (MOD)"),
    "ARE": ("uae-adoc",     "UAE Air Force & Air Defence Operations Centre"),
    "QAT": ("qatar-joc",    "Qatar Armed Forces Joint Operations Center (MOD)"),
    "KWT": ("kuwait-adc",   "Kuwait Air Defense Command (MOD)"),
    "BHR": ("bdf-joc",      "Bahrain Defence Force JOC"),
    "JOR": ("jaf-joc",      "Jordan Armed Forces Joint Operations Center"),
    "DJI": ("djibouti-mod", "Djibouti Ministry of Defence / FAD"),
    "OMN": ("rafo",         "Royal Air Force of Oman Air Defence"),
    "TUR": ("tuaf-caoc",    "Turkish Air Force / NATO CAOC"),
    "CYP": ("cyp-ng",       "Cyprus National Guard Joint Rescue Coordination Centre"),
}
COUNTRY_NAME = {"GBR": "United Kingdom", "FRA": "France", "AUS": "Australia", "DEU": "Germany", "JPN": "Japan", "SAU": "Saudi Arabia", "ARE": "United Arab Emirates",
                "QAT": "Qatar", "KWT": "Kuwait", "BHR": "Bahrain", "JOR": "Jordan", "DJI": "Djibouti", "OMN": "Oman", "TUR": "Turkey", "CYP": "Cyprus", "US": "United States",
                "IR": "Iran", "YE": "Yemen (Houthi)"}
AIRSPACE = [("faa-icao-notam", "FAA / ICAO NOTAM desk (Bahrain & Jeddah FIRs)"), ("navwarn", "NAVWARN / NGA navigational warning")]

# Interceptor assignment by target type and threat class: what the asset under attack can shoot with.
DEFENSE = {
    ("CARRIER", "missile"): "Aegis escorts: SM-6 / SM-2, ESSM inside 25 nm",
    ("VESSEL", "missile"):  "Aegis: SM-6 / SM-2, ESSM inside 25 nm, CIWS last-ditch",
    ("CARRIER", "uas"):     "ESSM / SM-2, carrier air wing CAP (F/A-18E) beyond SAM range, CIWS last-ditch",
    ("VESSEL", "uas"):      "ESSM / SM-2, CIWS last-ditch, CAP if beyond SAM range",
    ("AIR_BASE", "missile"): "THAAD (upper tier) and Patriot PAC-3 (lower tier)",
    ("AIR_BASE", "uas"):    "Coyote C-UAS, NASAMS, F-15E CAP beyond SAM range",
}


def _nm(lat1, lon1, lat2, lon2):
    r = 3440.065
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _bearing(lat1, lon1, lat2, lon2):
    p1, p2, dl = math.radians(lat1), math.radians(lat2), math.radians(lon2 - lon1)
    x = math.sin(dl) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def _spoken_bearing(b):
    """'2-7-0' the way it is said on the 1MC."""
    return "-".join(f"{int(round(b)) % 360:03d}")


def _lvl(s):
    return str(s or "unknown").strip().lower().replace("/", "-").replace("*", "-").replace(">", "-").replace(" ", "-")


def _payload(m):
    s = m.get_payload_as_string()
    if s:
        return s
    b = m.get_payload_as_bytes()
    return b.decode("utf-8", "replace") if b else ""


class _Handler(MessageHandler):
    def __init__(self, fn):
        self.fn = fn

    def on_message(self, m):
        try:
            self.fn(m.get_destination_name(), json.loads(_payload(m) or "{}"))
        except Exception as e:  # never let a bad message kill the receiver
            print(f"[cop_tools] handler error on {m.get_destination_name()}: {e}")


class Mesh:
    """One Solace client per agent host: live picture in, agent outputs out."""
    _inst = None
    _lock = threading.Lock()

    @classmethod
    def get(cls):
        with cls._lock:
            if cls._inst is None:
                cls._inst = Mesh()
                cls._inst.connect()
            return cls._inst

    def __init__(self):
        self.ships, self.aircraft, self.sites = {}, {}, {}
        self.threats = {}            # track_id -> rec (missiles / uas, latest)
        self.interceptors = {}       # interceptor_id -> rec
        self.alerts = []             # recent alerts, newest first
        self.actions = []            # operator actions, newest first
        self.recommendations = {}    # recommendation_id -> rec (+ decision)
        self.notifications = []
        self.decisions = []
        self.launches = {}           # site_id -> [launch recs]
        self.coalition_watch = {}    # track_id -> {rec, partner, notified: [...]}   the liaison agent's standing watch on partner attacks
        self.launch_watch = {}       # track_id -> {"done": Event, "result": dict}  the IAMD standing watch on launches at US assets
        self.ship_watch = {}         # (ship_id, contact_id) -> {"done": Event, "result": dict}  the ship-defense standing watch
        self.lock = threading.Lock()
        self.svc = self.direct = self.persistent = None

    def connect(self):
        props = {T.HOST: HOST, S.VPN_NAME: VPN, CP.NAME: "agent-mesh-tools",
                 A.SCHEME_BASIC_USER_NAME: USER, A.SCHEME_BASIC_PASSWORD: PASSWORD}
        self.svc = (MessagingService.builder().from_properties(props)
                    .with_reconnection_retry_strategy(RetryStrategy.forever_retry(3000)).build())
        self.svc.connect()
        self.direct = self.svc.create_direct_message_publisher_builder().build(); self.direct.start()
        self.persistent = self.svc.create_persistent_message_publisher_builder().build(); self.persistent.start()
        rx = (self.svc.create_direct_message_receiver_builder()
              .with_subscriptions([TopicSubscription.of(s) for s in SUBS]).build())
        rx.start(); rx.receive_async(_Handler(self.on_message))
        print(f"[cop_tools] connected {HOST} vpn={VPN} user={USER}; {len(SUBS)} subscriptions", flush=True)

    def on_message(self, topic, rec):
        p = topic.split("/")
        if len(p) < 4 or p[0] != "cop":
            return
        domain, event = p[1], p[2]
        with self.lock:
            if domain == "sea" and event == "track" and rec.get("vessel_id"):
                self.ships[rec["vessel_id"]] = rec
            elif domain == "air" and event == "track" and rec.get("aircraft_id"):
                self.aircraft[rec["aircraft_id"]] = rec
            elif domain == "land" and event == "site" and rec.get("unit_id"):
                self.sites[rec["unit_id"]] = rec
            elif domain == "air" and event in ("missile", "uas") and rec.get("track_id"):
                self.threats[rec["track_id"]] = rec
            elif domain == "air" and event in ("missile-event", "uas-event") and rec.get("track_id"):
                self.threats[rec["track_id"]] = rec
                if rec.get("event") in ("launch", "launch-member") and rec.get("launch_site_id"):
                    self.launches.setdefault(rec["launch_site_id"], []).insert(0, rec)
                if rec.get("event") in ("intercepted", "impact"):
                    w = self.coalition_watch.get(rec["track_id"]) or next((v for v in self.coalition_watch.values() if rec.get("wave_id") and v["rec"].get("wave_id") == rec.get("wave_id")), None)
                    if w and "outcome:" + rec["track_id"] not in w["notified"]:
                        w["notified"].append("outcome:" + rec["track_id"])
                        threading.Thread(target=self._partner_outcome, args=(w, rec), daemon=True).start()
            elif domain == "air" and event == "interceptor" and rec.get("interceptor_id"):
                self.interceptors[rec["interceptor_id"]] = rec
            elif domain == "alert":
                self.alerts.insert(0, {"topic": topic, **rec}); del self.alerts[200:]
                # Standing watches: the time-critical workflows run the moment the alert is on the mesh, with no model in the
                # path. The agent is woken by the same alert, calls the same tool, gets the result the watch already produced
                # and writes the narrative. A small local model that skips the tool call can no longer lose the engagement.
                if event == "inbound" and rec.get("affiliation") == "us" and rec.get("entity") and rec["entity"] not in self.launch_watch:
                    self.launch_watch[rec["entity"]] = {"done": threading.Event(), "result": None, "at": time.time()}
                    threading.Thread(target=self._watch_run, args=(self.launch_watch[rec["entity"]], handle_launch, (rec["entity"],), f"launch {rec['entity']}"), daemon=True).start()
                if event == "ship-threat" and rec.get("ship_id") and rec.get("contact_id"):
                    key = (str(rec["ship_id"]).upper(), str(rec["contact_id"]).upper())
                    if key not in self.ship_watch or time.time() - self.ship_watch[key]["at"] > 600:     # a loitering contact alerts again after the cooldown
                        self.ship_watch[key] = {"done": threading.Event(), "result": None, "at": time.time()}
                        threading.Thread(target=self._watch_run, args=(self.ship_watch[key], handle_ship_threat, key, f"ship threat {key[0]} <- {key[1]}"), daemon=True).start()
                if event == "inbound" and rec.get("affiliation") == "coalition" and rec.get("entity") and rec["entity"] not in self.coalition_watch:
                    # the partner is told the moment the launch is on the mesh -- no model in that path; the agent assesses afterwards
                    slug, name = _partner_for(rec.get("target_country"))
                    self.coalition_watch[rec["entity"]] = {"rec": dict(rec, track_id=rec["entity"], target_id=rec.get("target"), country=rec.get("attacker_country")), "partner": (slug, name), "notified": []}
                    threading.Thread(target=self._partner_attack, args=(self.coalition_watch[rec["entity"]], rec), daemon=True).start()
            elif domain == "c2" and event == "action":
                self.actions.insert(0, rec); del self.actions[200:]
                if str(rec.get("domain", "")).upper() == "COALITION" and rec.get("action") in ("monitor", "partner_engaged", "end_event"):
                    w = self.coalition_watch.get(str(rec.get("target_id", "")).upper())
                    if w:
                        threading.Thread(target=self._partner_decision, args=(w, rec), daemon=True).start()
                rid = rec.get("recommendation_id")
                if rid and rid in self.recommendations:
                    st = {"reject_recommendation": "rejected", "acknowledge_recommendation": "overtaken"}.get(rec.get("action"), "approved")
                    self.recommendations[rid]["decision"] = {"status": st, "by": rec.get("operator"), "at": rec.get("timestamp")}
            elif domain == "agent" and event == "recommendation" and rec.get("recommendation_id"):
                self.recommendations.setdefault(rec["recommendation_id"], rec)
            elif domain == "agent" and event == "decision" and rec.get("recommendation_id"):
                if rec["recommendation_id"] in self.recommendations:
                    self.recommendations[rec["recommendation_id"]]["decision"] = rec
            elif domain == "notify":
                self.notifications.insert(0, {"topic": topic, **rec}); del self.notifications[200:]
            elif domain == "decision":
                self.decisions.insert(0, rec); del self.decisions[100:]

    def _watch_run(self, slot, fn, args, label):
        """Run a workflow tool from the standing watch (own thread, own event loop) and keep its result for the agent."""
        threading.current_thread().name = "cop-watch"
        time.sleep(0.5)                                   # let the direct track stream deliver the inbound/contact record first
        try:
            slot["result"] = asyncio.run(fn(*args))
            print(f"[cop_tools] standing watch ran {label}: ok={slot['result'].get('ok')}", flush=True)
        except Exception as e:                             # noqa: BLE001
            slot["result"] = {"ok": False, "error": str(e)}
            print(f"[cop_tools] standing watch {label} failed: {e}", flush=True)
        slot["done"].set()

    @staticmethod
    def _watch_result(slot, wait_s=8.0):
        """The agent's tool call lands after the watch started the same workflow: hand it the watch's result."""
        if slot and slot["done"].wait(wait_s) and slot["result"] is not None:
            return dict(_compact(slot["result"]), already_handled=True, note="the standing watch ran this workflow the moment the alert hit the mesh; nothing was published twice")
        return None

    def _partner_attack(self, w, a):
        """Notification #1, automatic: the partner's national authority hears about the launch at once."""
        slug, name = w["partner"]
        who = f"{a.get('weapon')} {a.get('weapon_class')}" + (f" wave of {a.get('wave_size')}" if (a.get("wave_size") or 1) > 1 else "")
        text = (f"FLASH. {who} launched by {COUNTRY_NAME.get(a.get('attacker_country'), a.get('attacker_country'))} forces from {a.get('launch_site')} at your asset "
                f"{a.get('target_name')} ({a.get('target')}), time to impact {a.get('tti_s')} s. US sensors hold the track. The US watch officer is deciding whether a US unit "
                f"engages on your behalf; you will be told what is decided and the outcome.")
        body = _stamp("coalition-liaison", {"notification_id": f"NTF-{uuid.uuid4().hex[:8].upper()}", "schema": "cop.notify.v1", "channel": "coalition", "sequence": 1, "stage": "attack",
                                            "recipient": name, "recipient_id": slug, "partner_country": a.get("target_country"), "attacker_country": a.get("attacker_country"),
                                            "subject": f"Attack on {a.get('target_name')}", "track_id": a.get("entity"), "wave_id": a.get("wave_id"), "target_id": a.get("target"),
                                            "target_name": a.get("target_name"), "message": text, "tti_s": a.get("tti_s"), "lat": a.get("lat"), "lon": a.get("lon"), "priority": "FLASH"})
        self.publish(f"cop/notify/coalition/v1/{_aor_of(a)}/{slug}/{_lvl(a.get('entity'))}/attack", body)
        w["notified"].append("attack")
        print(f"[cop_tools] partner attack notice -> {name}: {who} at {a.get('target_name')}", flush=True)

    def _partner_decision(self, w, act):
        """The operator chose STANDBY & MONITOR, PARTNER ENGAGED or END EVENT: tell the partner."""
        slug, name = w["partner"]; r = w["rec"]; tid = str(act.get("target_id", "")).upper()
        stage = {"monitor": "standby", "partner_engaged": "partner-engaged", "end_event": "closed"}[act["action"]]
        text = {"monitor": f"US forces are standing by and monitoring {r.get('weapon')} {tid} inbound on {r.get('target_name')}; no US engagement ordered. Your defences have the engagement. Request status.",
                "partner_engaged": f"Acknowledged: your defences are engaging {r.get('weapon')} {tid} inbound on {r.get('target_name')}. US sensors continue to track and will report the outcome.",
                "end_event": f"Event closed by the US watch officer: {r.get('weapon')} {tid} on {r.get('target_name')}. No further action from US forces."}[act["action"]]
        body = _stamp("coalition-liaison", {"notification_id": f"NTF-{uuid.uuid4().hex[:8].upper()}", "schema": "cop.notify.v1", "channel": "coalition", "sequence": 2, "stage": stage,
                                            "recipient": name, "recipient_id": slug, "partner_country": r.get("target_country"), "attacker_country": r.get("country"),
                                            "subject": f"{stage.replace('-', ' ').title()}: {r.get('target_name')}", "track_id": tid, "target_id": r.get("target_id"), "target_name": r.get("target_name"),
                                            "message": text, "decision": act.get("action"), "operator": act.get("operator"), "lat": r.get("lat"), "lon": r.get("lon"), "priority": "IMMEDIATE"})
        self.publish(f"cop/notify/coalition/v1/{_aor_of(r)}/{slug}/{_lvl(tid)}/{stage}", body)
        w["notified"].append(stage)
        print(f"[cop_tools] partner {stage} notice -> {name}", flush=True)

    def _partner_outcome(self, w, rec):
        """Third message to the partner, sent by the standing watch the moment the mesh reports the outcome."""
        slug, name = w["partner"]
        ok = rec.get("event") == "intercepted"
        text = (f"{'INTERCEPTED' if ok else 'IMPACT'}: {rec.get('weapon')} {rec.get('track_id')} aimed at {rec.get('target_name')} "
                + (f"was destroyed by {str(rec.get('engaged_by') or 'the defending unit').replace(' (operator engage)', '').replace(' (auto, self-defense)', '')} short of the target."
                   if ok else "struck the target. Battle damage assessment being tasked; US forces stand by to assist."))
        body = _stamp("coalition-liaison", {"notification_id": f"NTF-{uuid.uuid4().hex[:8].upper()}", "schema": "cop.notify.v1", "channel": "coalition", "sequence": 3,
                                            "stage": "outcome", "recipient": name, "recipient_id": slug, "partner_country": w["rec"].get("target_country"),
                                            "attacker_country": rec.get("country"), "subject": f"Outcome: {rec.get('target_name')}", "track_id": rec.get("track_id"),
                                            "target_id": rec.get("target_id"), "target_name": rec.get("target_name"), "message": text, "outcome": rec.get("event"),
                                            "lat": rec.get("lat"), "lon": rec.get("lon"), "priority": "IMMEDIATE"})
        self.publish(f"cop/notify/coalition/v1/{_aor_of(rec)}/{slug}/{_lvl(rec.get('track_id'))}/outcome", body)
        print(f"[cop_tools] partner outcome -> {name}: {text}", flush=True)

    def publish(self, topic, body, guaranteed=True):
        data = json.dumps(body)
        (self.persistent if guaranteed else self.direct).publish(data, Topic.of(topic))
        return topic

    # ---- lookups
    def asset(self, asset_id):
        with self.lock:
            s = self.ships.get(asset_id)
            if s:
                return {"kind": "ship", "id": asset_id, "name": s.get("vessel_type"), "lat": s.get("lat"), "lon": s.get("lon"),
                        "heading_deg": s.get("heading_deg"), "speed_kts": s.get("speed_knots") or s.get("speed_kts"), "record": s}
            f = self.sites.get(asset_id)
            if f:
                return {"kind": "facility", "id": asset_id, "name": f.get("unit_name"), "country": f.get("country") or FACILITY_COUNTRY.get(asset_id),
                        "lat": f.get("lat"), "lon": f.get("lon"), "record": f}
        return None


def _in_watch():
    return threading.current_thread().name == "cop-watch"


def _aor_of(rec):
    lat, lon = rec.get("lat"), rec.get("lon")
    if lat is None or lon is None:
        return "centcom"
    if lon >= 90:
        return "indopacom"
    if lat < 17 and lon < 44.5:
        return "africom"
    if lon < 33.5 or lat >= 36.5:
        return "eucom"
    return "centcom"


def _stamp(agent, extra=None):
    body = {"agent": agent, "produced_by": f"agent-mesh/{agent}", "timestamp": int(time.time() * 1000)}
    if extra:
        body.update(extra)
    return body


_llm_checked = False


def check_llm():
    """One clear line in the log about the LLM at startup, because LiteLLM reports a missing Ollama model and an
    unreachable Ollama the same way ("Unable to connect to the LLM service")."""
    global _llm_checked
    if _llm_checked:
        return
    _llm_checked = True
    import urllib.request
    endpoint = os.environ.get("LLM_SERVICE_ENDPOINT", "http://ollama:11434").rstrip("/")
    model = os.environ.get("LLM_SERVICE_GENERAL_MODEL_NAME", "ollama_chat/qwen2.5:7b")
    if not model.startswith(("ollama", "ollama_chat")):
        print(f"[cop_tools] LLM: {model} at {endpoint} (cloud/OpenAI-compatible; not checked here)", flush=True)
        return
    name = model.split("/", 1)[1]
    try:
        with urllib.request.urlopen(f"{endpoint}/api/tags", timeout=5) as r:
            tags = json.loads(r.read().decode())
        have = [m.get("name") for m in tags.get("models", [])]
        if name in have or (name + ":latest") in have:
            print(f"[cop_tools] LLM ok: {name} is pulled at {endpoint}", flush=True)
        else:
            print(f"[cop_tools] LLM: Ollama at {endpoint} is up but '{name}' is NOT pulled (have: {have or 'nothing'}). "
                  f"Run: docker exec solace-ollama ollama pull {name}", flush=True)
    except Exception as e:
        print(f"[cop_tools] LLM: Ollama NOT reachable at {endpoint} ({e}). Is the ollama container up on the cop-mesh network?", flush=True)


async def init_mesh(component=None, tool_config=None):
    """Lifecycle hook (init_function in the agent YAML): connect to the broker when the agent starts,
    so the first tool call is not the one that pays for the connection, and say whether the LLM is there."""
    check_llm()
    try:
        Mesh.get()
    except Exception as e:
        print(f"[cop_tools] broker not reachable yet ({e}); will retry on first tool call", flush=True)
        with Mesh._lock:
            Mesh._inst = None


# =============================================================================== tools
async def get_inbound_threat(track_id: str, tool_context=None, tool_config: Optional[Dict[str, Any]] = None) -> dict:
    """Return the live record of an inbound missile or one-way-attack drone (its weapon, launch site, target,
    time to impact, phase), the other airframes of its wave if it is one, the asset it is aimed at as the COP
    sees it now, what that asset can defend itself with, and the host nation the asset sits in.

    Args:
        track_id: the inbound's track id, e.g. MSL-3F2A1C or UAS-77B0D1 (case-insensitive).
    """
    m = Mesh.get(); tid = track_id.upper()
    with m.lock:
        rec = m.threats.get(tid)
        wave = [r for r in m.threats.values() if rec and rec.get("wave_id") and r.get("wave_id") == rec.get("wave_id")]
    if not rec:
        return {"found": False, "track_id": tid, "hint": "not in the live picture; it may have been intercepted or the id is wrong"}
    target = m.asset(rec.get("target_id")) or {"id": rec.get("target_id"), "name": rec.get("target_name"), "kind": "unknown"}
    kind = "missile" if rec.get("kind") == "missile" else "uas"
    ttype = rec.get("target_type") or ""
    defense = DEFENSE.get((ttype if ttype in ("CARRIER", "VESSEL") else "AIR_BASE", kind), "")
    host = None
    if target.get("kind") == "facility":
        c = target.get("country"); host = {"country": c, "agency": HOST_NATION.get(c, (None, f"{c} MOD"))[1]}
    return {"found": True, "threat": {k: rec.get(k) for k in ("track_id", "kind", "weapon", "weapon_class", "warhead", "country", "launch_site", "launch_site_id",
                                                              "target_id", "target_name", "target_type", "phase", "tti_s", "range_nm", "flown_pct", "speed_kts",
                                                              "wave_id", "wave_size", "outcome", "engaged_by", "engagement", "lat", "lon")},
            "wave_members": [r.get("track_id") for r in wave if r.get("track_id") != tid and not r.get("outcome")],
            "target_now": {k: target.get(k) for k in ("kind", "id", "name", "country", "lat", "lon", "heading_deg", "speed_kts")},
            "can_defend_with": defense, "host_nation": host,
            "time_now": time.strftime("%H:%M:%SZ", time.gmtime())}


async def recommend_engagement(track_id: str, action: str, rationale: str, tool_context=None, tool_config: Optional[Dict[str, Any]] = None) -> dict:
    """Recommend an engagement to the COP operator (US asset under attack; the IAMD workflow). This does NOT fire: it publishes a recommendation the
    operator approves or rejects on the COP (or that the IAMD panel auto-approves when set to AUTO). On
    approval the COP publishes the real cop/c2/action engage, the defending asset fires, and the decision
    record carries this recommendation's id.

    Args:
        track_id: the inbound track id (MSL-… or UAS-…). For a drone wave give the lead track: the whole wave is engaged.
        action: one of engage, weapons_free, warn_asset, track. Use engage for a missile or drone aimed at a friendly asset.
        rationale: one or two sentences for the operator: weapon, target, time to impact, why this action.
    """
    return await _recommend_engagement(track_id, action, rationale)


async def _recommend_engagement(track_id, action, rationale, extra=None, agent="iamd"):

    m = Mesh.get(); tid = track_id.upper()
    with m.lock:
        rec = m.threats.get(tid) or {"track_id": tid}
        wave = [r.get("track_id") for r in m.threats.values() if rec.get("wave_id") and r.get("wave_id") == rec.get("wave_id") and not r.get("outcome")]
    action = (action or "engage").lower().replace(" ", "_")
    if action not in ("engage", "weapons_free", "warn_asset", "track"):
        return {"ok": False, "error": "action must be engage, weapons_free, warn_asset or track"}
    rid = f"REC-{uuid.uuid4().hex[:8].upper()}"
    aor = _aor_of(rec)
    ttype = rec.get("target_type") or ""
    defense = DEFENSE.get((ttype if ttype in ("CARRIER", "VESSEL") else "AIR_BASE", "missile" if rec.get("kind") == "missile" else "uas"), "")
    body = _stamp(agent, {
        "recommendation_id": rid, "schema": "cop.agent.recommendation.v1", "action": action, "defense": defense, "wave_size": max(1, len(wave)),
        "action_domain": "missile" if rec.get("kind") == "missile" else "uas", "target_id": tid,
        "targets": wave if len(wave) > 1 else [tid], "wave_id": rec.get("wave_id"),
        "summary": f"{action.upper().replace('_', ' ')} {rec.get('weapon', '')} {tid}" + (f" wave of {len(wave)}" if len(wave) > 1 else "") + f" → {rec.get('target_name', '')}",
        "rationale": rationale, "threat": {k: rec.get(k) for k in ("weapon", "weapon_class", "target_id", "target_name", "target_type", "tti_s", "phase", "launch_site", "country")},
        "lat": rec.get("lat"), "lon": rec.get("lon"), "requires_approval": True, **(extra or {}),
    })
    topic = m.publish(f"cop/agent/recommendation/v1/{aor}/{agent}/{_lvl(tid)}/{_lvl(action)}", body)
    with m.lock:
        m.recommendations[rid] = body
    return {"ok": True, "recommendation_id": rid, "topic": topic, "targets": body["targets"], "awaiting": "operator approval on the COP (or AUTO)"}


async def notify_host_nation(target_id: str, message: str, tool_context=None, tool_config: Optional[Dict[str, Any]] = None) -> dict:
    """Notify the host nation's air-defence / joint-operations watch that a US asset on its territory is under
    missile or drone attack (for a ship: the coalition maritime watch CTF-153/150 and UKMTO). Publishes a
    guaranteed notification the liaison systems and the SOAR pick up.

    Args:
        target_id: the attacked asset's id (facility id such as FAC-010, or hull number such as DDG-51).
        message: the notification text: what is inbound, from where, time to impact, what we are doing about it.
    """
    m = Mesh.get(); asset = m.asset(target_id) or {"id": target_id, "kind": "unknown"}
    if asset.get("kind") == "ship":
        recipients = MARITIME
    else:
        c = asset.get("country"); recipients = [HOST_NATION.get(c, (_lvl(c), f"{c} Ministry of Defence"))] if c else [("unknown-host", "host nation watch (unresolved)")]
    aor = _aor_of(asset)
    out = []
    for slug, name in recipients:
        body = _stamp("iamd", {"notification_id": f"NTF-{uuid.uuid4().hex[:8].upper()}", "schema": "cop.notify.v1", "channel": "host-nation",
                               "recipient": name, "recipient_id": slug, "subject": f"Attack on {asset.get('name') or target_id}",
                               "target_id": target_id, "target_name": asset.get("name"), "country": asset.get("country"), "message": message,
                               "lat": asset.get("lat"), "lon": asset.get("lon"), "priority": "FLASH"})
        out.append(m.publish(f"cop/notify/host-nation/v1/{aor}/{slug}/{_lvl(target_id)}/flash", body))
    return {"ok": True, "notified": [n for _, n in recipients], "topics": out}


async def notify_airspace_and_maritime(track_id: str, message: str, tool_context=None, tool_config: Optional[Dict[str, Any]] = None) -> dict:
    """Warn civil aviation (FAA/ICAO NOTAM desk for the Bahrain and Jeddah FIRs) and shipping (NAVWARN) about
    a missile or drone corridor from the launch site to the target.

    Args:
        track_id: the inbound track id.
        message: the warning text with the corridor (launch site → target) and the expected duration.
    """
    m = Mesh.get(); tid = track_id.upper()
    with m.lock:
        rec = m.threats.get(tid) or {"track_id": tid}
    aor = _aor_of(rec); out = []
    for slug, name in AIRSPACE:
        body = _stamp("iamd", {"notification_id": f"NTF-{uuid.uuid4().hex[:8].upper()}", "schema": "cop.notify.v1", "channel": "airspace",
                               "recipient": name, "recipient_id": slug, "subject": f"Missile/UAS corridor {rec.get('launch_site', '?')} → {rec.get('target_name', '?')}",
                               "track_id": tid, "message": message, "corridor": {"from": [rec.get("launch_lat"), rec.get("launch_lon")], "to": [rec.get("target_lat"), rec.get("target_lon")]},
                               "lat": rec.get("lat"), "lon": rec.get("lon"), "priority": "IMMEDIATE"})
        out.append(m.publish(f"cop/notify/airspace/v1/{aor}/{slug}/{_lvl(tid)}/immediate", body))
    return {"ok": True, "notified": [n for _, n in AIRSPACE], "topics": out}


async def get_ship_threat_geometry(ship_id: str, contact_id: str, tool_context=None, tool_config: Optional[Dict[str, Any]] = None) -> dict:
    """Compute the tactical geometry between a friendly ship and a threat (missile, drone, hostile aircraft or
    vessel): true and relative bearing from the ship, range in nautical miles, closing speed, time to the
    merge, and which 1MC announcement the situation calls for.

    Args:
        ship_id: the friendly ship's hull number, e.g. DDG-51, CVN-72.
        contact_id: the threat's id: a missile/drone track (MSL-…/UAS-…), a hostile aircraft tail or hull number.
    """
    m = Mesh.get(); sid, cid = ship_id.upper(), contact_id.upper()
    ship = m.asset(sid)
    if not ship or ship.get("kind") != "ship":
        return {"found": False, "error": f"{sid} is not a ship in the live picture"}
    with m.lock:
        c = m.threats.get(cid) or m.aircraft.get(cid) or m.ships.get(cid)
    if not c or c.get("lat") is None:
        return {"found": False, "error": f"{cid} not in the live picture"}
    b_true = _bearing(ship["lat"], ship["lon"], c["lat"], c["lon"])
    hdg = float(ship.get("heading_deg") or 0)
    b_rel = (b_true - hdg + 360) % 360
    rng = _nm(ship["lat"], ship["lon"], c["lat"], c["lon"])
    c_speed = float(c.get("speed_kts") or c.get("speed_knots") or 0)
    c_hdg = float(c.get("heading_deg") or 0)
    # closing component of the contact's velocity along the line to the ship
    bearing_to_ship = _bearing(c["lat"], c["lon"], ship["lat"], ship["lon"])
    closing = c_speed * math.cos(math.radians(c_hdg - bearing_to_ship))
    tti = c.get("tti_s")
    if tti is None:
        tti = round(rng / closing * 3600, 0) if closing > 5 else None
    kind = "missile" if str(cid).startswith("MSL") else "uas" if str(cid).startswith("UAS") else "aircraft" if cid in m.aircraft else "surface"
    if tti is not None and tti <= 60 or rng < 1.0:
        stage = "immediate"; announcement = f"Inbound {'missile' if kind == 'missile' else 'threat'}, inbound {'missile' if kind == 'missile' else 'threat'}! All hands brace for shock!"
        alarm = "general_quarters"
    elif rng <= 1.5 and kind in ("uas", "surface"):
        stage = "close_in"; announcement = "CIWS, CIWS, mount 2-1 is tracking. Stand clear of mount 2-1!"; alarm = "none"
    else:
        stage = "tactical"
        what = {"missile": "inbound missile", "uas": "inbound air contact, one-way-attack UAS", "aircraft": "inbound air contact", "surface": "inbound surface contact"}[kind]
        announcement = f"General Quarters, General Quarters! All hands man your battle stations for {what}, bearing {_spoken_bearing(b_rel)}, {rng:.0f} miles closing."
        alarm = "general_quarters"
    return {"found": True, "ship": {"id": sid, "name": ship.get("name"), "heading_deg": hdg, "speed_kts": ship.get("speed_kts")},
            "contact": {"id": cid, "kind": kind, "type": c.get("weapon") or c.get("aircraft_type") or c.get("vessel_type"), "country": c.get("country"), "phase": c.get("phase")},
            "bearing_true_deg": round(b_true), "bearing_relative_deg": round(b_rel), "bearing_spoken": _spoken_bearing(b_rel),
            "range_nm": round(rng, 1), "closing_kts": round(closing), "time_to_impact_s": tti,
            "stage": stage, "recommended_alarm": alarm, "recommended_announcement": announcement,
            "stages": {"immediate": "seconds to impact: GQ alarm + 'brace for shock'", "tactical": "minutes away: GQ alarm + battle stations with bearing and range", "close_in": "inside ~1 nm: CIWS mount warning"}}


async def notify_ship_watch(ship_id: str, contact_id: str, threat_summary: str, recommended_alarm: str, recommended_announcement: str,
                            bearing_spoken: str = "", range_nm: float = 0, contact: Optional[Dict[str, Any]] = None, geometry: Optional[Dict[str, Any]] = None,
                            tool_context=None, tool_config: Optional[Dict[str, Any]] = None) -> dict:
    """Notify the ship's Officer of the Deck and Damage Control Central of a threat, with the 1MC options
    ready to use. The OOD chooses on the ship's 1MC panel: General Quarters alarm, Collision alarm, the
    recommended announcement, a CIWS warning, or to secure. Publishes a guaranteed notification.

    Args:
        ship_id: hull number, e.g. DDG-51.
        contact_id: the threat's id.
        threat_summary: one line: what it is, bearing, range, time to impact.
        recommended_alarm: general_quarters, collision, or none.
        recommended_announcement: the exact words for the 1MC.
        bearing_spoken: relative bearing as spoken, e.g. 2-7-0.
        range_nm: range in nautical miles.
        contact: optional, the contact block from get_ship_threat_geometry (id, kind, type, country, phase).
        geometry: optional, the geometry (bearing_true_deg, closing_kts, time_to_impact_s, stage).
    """
    m = Mesh.get(); sid = ship_id.upper(); ship = m.asset(sid) or {"id": sid, "name": sid}
    contact = contact or {}; geometry = geometry or {}
    aor = _aor_of(ship)
    body = _stamp("ship-defense", {
        "notification_id": f"NTF-{uuid.uuid4().hex[:8].upper()}", "schema": "cop.notify.v1", "channel": "ship",
        "recipient": f"{ship.get('name') or sid} — Officer of the Deck and Damage Control Central", "recipient_id": "ood-dcc",
        "ship_id": sid, "ship_name": ship.get("name"), "contact_id": contact_id.upper(), "subject": f"Threat to {sid}: {threat_summary}",
        "message": threat_summary, "recommended_alarm": recommended_alarm, "recommended_announcement": recommended_announcement,
        "bearing_spoken": bearing_spoken, "range_nm": range_nm,
        "contact_kind": contact.get("kind"), "contact_type": contact.get("type"), "contact_country": contact.get("country"), "contact_phase": contact.get("phase"),
        "bearing_true_deg": geometry.get("bearing_true_deg"), "closing_kts": geometry.get("closing_kts"), "time_to_impact_s": geometry.get("time_to_impact_s"), "stage": geometry.get("stage"),
        "options": [
            {"id": "1mc_gq", "label": "General Quarters alarm", "alarm": "general_quarters"},
            {"id": "1mc_collision", "label": "Collision alarm", "alarm": "collision"},
            {"id": "1mc_brace", "label": "Brace for shock", "announcement": "Inbound threat, inbound threat! All hands brace for shock!"},
            {"id": "1mc_battle_stations", "label": "Battle stations with bearing", "announcement": recommended_announcement if recommended_announcement.startswith("General Quarters") else f"General Quarters, General Quarters! All hands man your battle stations for an inbound contact, bearing {bearing_spoken or '0-0-0'}, {range_nm:.0f} miles closing."},
            {"id": "1mc_ciws", "label": "CIWS mount warning", "announcement": "CIWS, CIWS, mount 2-1 is tracking. Stand clear of mount 2-1!"},
            {"id": "1mc_secure", "label": "Secure from General Quarters", "announcement": "Secure from General Quarters. Set the normal underway watch."},
        ],
        "lat": ship.get("lat"), "lon": ship.get("lon"), "priority": "FLASH",
    })
    topic = m.publish(f"cop/notify/ship/v1/{aor}/{_lvl(sid)}/ood-dcc/flash", body)
    return {"ok": True, "notification_id": body["notification_id"], "topic": topic, "panel": f"http://localhost:3000/ship.html?ship={sid}"}


async def get_launch_site_history(site_id: str, tool_context=None, tool_config: Optional[Dict[str, Any]] = None) -> dict:
    """Launch history of a hostile launch site as this mesh has seen it: how many launches, which weapons,
    which targets, the outcomes. Use it to decide whether the site should be nominated for targeting or ISR.

    Args:
        site_id: launch site id, e.g. LS-IR-01, or its name.
    """
    m = Mesh.get()
    with m.lock:
        key = next((k for k in m.launches if k.lower() == site_id.lower() or any(site_id.lower() in str(r.get("launch_site", "")).lower() for r in m.launches[k])), None)
        recs = list(m.launches.get(key, [])) if key else []
        outcomes = {r.get("track_id"): (m.threats.get(r.get("track_id")) or {}).get("outcome") for r in recs}
    if not recs:
        return {"found": False, "site_id": site_id}
    r0 = recs[0]
    return {"found": True, "site_id": key, "site": r0.get("launch_site"), "country": r0.get("country"), "lat": r0.get("launch_lat"), "lon": r0.get("launch_lon"),
            "launches": len(recs), "weapons": sorted({r.get("weapon") for r in recs}), "targets": sorted({r.get("target_name") for r in recs}),
            "outcomes": {k: v for k, v in outcomes.items()}, "last_launch": r0.get("timestamp")}


async def request_isr(site_id: str, reason: str, tool_context=None, tool_config: Optional[Dict[str, Any]] = None) -> dict:
    """Task ISR (satellite imagery / BDA) against a launch site or an impact point. Publishes a guaranteed
    tasking the imagery and collection systems on the mesh pick up.

    Args:
        site_id: launch site id (LS-…) or the impacted asset id for BDA.
        reason: why: e.g. 'third launch in 10 minutes, confirm TEL location for targeting' or 'BDA after impact'.
    """
    m = Mesh.get()
    with m.lock:
        recs = m.launches.get(site_id.upper(), [])
    r0 = recs[0] if recs else {}
    asset = m.asset(site_id.upper())
    lat = r0.get("launch_lat") or (asset or {}).get("lat"); lon = r0.get("launch_lon") or (asset or {}).get("lon")
    body = _stamp("targeting", {"task_id": f"TSK-{uuid.uuid4().hex[:8].upper()}", "schema": "cop.agent.task.v1", "task": "imagery",
                                "target_id": site_id.upper(), "target_name": r0.get("launch_site") or (asset or {}).get("name"), "lat": lat, "lon": lon,
                                "reason": reason, "sensor": "EO/SAR, next pass", "priority": "PRIORITY"})
    topic = m.publish(f"cop/agent/task/v1/{_aor_of({'lat': lat, 'lon': lon})}/targeting/{_lvl(site_id)}/imagery", body)
    return {"ok": True, "task_id": body["task_id"], "topic": topic}


async def nominate_target(site_id: str, rationale: str, tool_context=None, tool_config: Optional[Dict[str, Any]] = None) -> dict:
    """Nominate a hostile launch site to the joint target list for counter-fire. A recommendation the COP
    operator must approve; nothing is struck by this call.

    Args:
        site_id: launch site id (LS-…).
        rationale: launch count, weapons, what it has hit, why it should be struck.
    """
    m = Mesh.get()
    with m.lock:
        recs = m.launches.get(site_id.upper(), [])
    r0 = recs[0] if recs else {}
    rid = f"REC-{uuid.uuid4().hex[:8].upper()}"
    body = _stamp("targeting", {"recommendation_id": rid, "schema": "cop.agent.recommendation.v1", "action": "nominate_target", "action_domain": "targeting",
                                "target_id": site_id.upper(), "targets": [site_id.upper()], "summary": f"NOMINATE {r0.get('launch_site') or site_id} to the joint target list",
                                "rationale": rationale, "launches": len(recs), "lat": r0.get("launch_lat"), "lon": r0.get("launch_lon"), "requires_approval": True})
    topic = m.publish(f"cop/agent/recommendation/v1/{_aor_of({'lat': r0.get('launch_lat'), 'lon': r0.get('launch_lon')})}/targeting/{_lvl(site_id)}/nominate-target", body)
    with m.lock:
        m.recommendations[rid] = body
    return {"ok": True, "recommendation_id": rid, "topic": topic, "awaiting": "operator approval on the COP"}



def _deliver_direct(tool_context, text: str) -> bool:
    """Hand the text to the task as its final answer and end the turn without a second LLM call.

    The Agent Mesh runtime builds a RUN_BASED task's final message from a response buffer (normally the
    model's streamed text) and treats a tool result marked skip_summarization as the last event. Writing
    the brief into that buffer means the Desktop shows it exactly as built, and the only model call is
    the one that chose to call the tool: ~10 s instead of ~30, and a 3B model cannot paraphrase, cut or
    embellish it. COP_BRIEF_DIRECT=0 restores the stock path (the model copies the brief)."""
    if os.environ.get("COP_BRIEF_DIRECT", "1") in ("0", "false", "no") or tool_context is None:
        return False
    try:
        a2a_ctx = tool_context.state.get("a2a_context", {}) or {}
        if not a2a_ctx.get("is_run_based_session", False):
            return False
        task_id = a2a_ctx.get("logical_task_id")
        host = tool_context._invocation_context.agent.host_component
        with host.active_tasks_lock:
            tc = host.active_tasks.get(task_id)
        if tc is None:
            return False
        tc.append_to_run_based_buffer(text)
        tool_context.actions.skip_summarization = True
        print(f"[cop_tools] brief delivered directly to task {task_id} ({len(text)} chars)", flush=True)
        return True
    except Exception as e:  # any surprise: the model answers as before
        print(f"[cop_tools] direct brief delivery unavailable ({e}); the model will write the answer", flush=True)
        return False

async def get_threat_picture(tool_context=None, tool_config: Optional[Dict[str, Any]] = None) -> dict:
    """The current picture, grouped by event (one wave at one target): the tracks and their state, the agent's
    recommendation and the operator's decision on it, the operator's actions, and who was notified. Use it to
    answer questions about what is happening and what has been done."""
    m = Mesh.get(); now = time.time() * 1000
    WINDOW = 15 * 60 * 1000

    def _t(ms):
        try:
            return time.strftime("%H:%M:%SZ", time.gmtime(float(ms) / 1000))
        except Exception:
            return "--:--:--Z"

    def _short_target(name, tid=None):
        name = str(name or tid or "?")
        if "(" in name and name.endswith(")"):
            return name[name.rfind("(") + 1:-1]
        return name[:28]

    def _dec(d):
        if not d:
            return "PENDING operator decision"
        if isinstance(d, dict):
            st = str(d.get("status") or d.get("decision") or "decided").upper()
            at = d.get("at") or d.get("timestamp")
            return f"{st} by {d.get('by') or d.get('operator') or 'operator'}" + (f" {_t(at)}" if at else "")
        return str(d).upper()

    def _cut(x, n):
        x = str(x or "")
        return x if len(x) <= n else x[:n].rsplit(" ", 1)[0] + " …"

    def _up(x):
        return str(x or "").upper().replace("_", " ")

    with m.lock:
        # everything in the brief is from the last WINDOW; an older item is not attached to an event just because it names the same target
        def _recent(x):
            return now - (x.get("timestamp") or 0) < WINDOW
        threats = [dict(r) for r in m.threats.values() if _recent(r)]
        recs = [r for r in list(m.recommendations.values())[-40:] if _recent(r)][-20:]
        actions = [a for a in m.actions[:80] if _recent(a) and _up(a.get("domain")) != "EXERCISE" and a.get("target_id") and a.get("operator") != "smoke-test"][:40]
        notes = [n for n in m.notifications[:120] if _recent(n)][:60]
        ints = [f"{r.get('interceptor')} from {r.get('shooter')} on {r.get('engaging')} ({r.get('phase')})"
                for r in m.interceptors.values() if r.get("phase") in ("STANDBY", "LAUNCH", "CLOSING")][-6:]
        launches = ", ".join(f"{k}: {len(v)}" for k, v in m.launches.items()) or "none"

    # ---- events: one per wave (fallback: same target, same launch minute)
    events, track_ev = {}, {}
    for r in threats:
        t0 = r.get("launched_at") or r.get("timestamp") or 0
        key = r.get("wave_id") or f"{r.get('target_id')}@{int(t0 // 60000)}"
        e = events.setdefault(key, {"t0": t0, "weapon": r.get("weapon"), "site": r.get("launch_site"), "site_id": r.get("launch_site_id"),
                                    "target_id": r.get("target_id"), "target_name": r.get("target_name"), "country": r.get("target_country") or ("partner" if r.get("target_coalition") else "US"),
                                    "tracks": [], "recs": [], "actions": [], "notes": []})
        e["t0"] = min(e["t0"], t0) if t0 else e["t0"]
        e["tracks"].append(r); track_ev[str(r.get("track_id")).upper()] = e
    by_target = {}
    for e in events.values():
        by_target.setdefault(str(e["target_id"]).upper(), []).append(e)

    def _ev_for(*ids):
        for x in ids:
            if x and str(x).upper() in track_ev:
                return track_ev[str(x).upper()]
        for x in ids:
            lst = by_target.get(str(x or "").upper())
            if lst:
                return max(lst, key=lambda e: e["t0"])
        return None

    other_recs, other_actions, other_notes = [], [], []
    for r in recs:
        e = _ev_for(r.get("target_id"), *(r.get("targets") or []))
        (e["recs"] if e else other_recs).append(r)
    for a in actions:
        e = _ev_for(a.get("target_id"), a.get("contact"), a.get("target"))
        (e["actions"] if e else other_actions).append(a)
    for n in notes:
        e = _ev_for(n.get("track_id"), n.get("contact_id"), n.get("target_id"), n.get("ship_id"))
        (e["notes"] if e else other_notes).append(n)

    def _track_lines(tr):
        out = []
        for r in sorted(tr, key=lambda r: (bool(r.get("outcome")), r.get("launched_at") or 0)):
            t0 = _t(r.get("launched_at") or r.get("launch_time") or r.get("first_seen"))
            if r.get("outcome"):
                out.append(f"launched {t0} {r.get('track_id')} {r.get('weapon')}: {r.get('outcome')} {_t(r.get('timestamp'))}" + (f" by {r.get('engaged_by')}" if r.get("engaged_by") else ""))
            else:
                out.append(f"launched {t0} {r.get('track_id')} {r.get('weapon')}: {r.get('phase')}, {int(r.get('tti_s') or 0)} s to impact" + (f", engaged by {r.get('engaged_by')}" if r.get("engaged_by") else ", not engaged"))
        return out

    def _rec_lines(rs):
        return [f"{_t(r.get('timestamp'))} {r.get('recommendation_id')} {r.get('agent')} agent: {_up(r.get('action'))} {r.get('target_id')}"
                + (f" (+{len(r.get('targets')) - 1} more in the wave)" if len(r.get("targets") or []) > 1 else "") + f" — {_dec(r.get('decision'))}"
                for r in sorted(rs, key=lambda r: r.get("timestamp") or 0)]

    def _actions_lines(acts):
        groups = {}
        for a in acts:
            k = (int((a.get("timestamp") or 0) // 1000), a.get("operator"), _up(a.get("action")), a.get("recommendation_id"))
            groups.setdefault(k, []).append(a)
        out = []
        for (sec, op, act, rid), rs in sorted(groups.items(), key=lambda kv: kv[0][0]):
            tg = ", ".join(str(a.get("target_id")) for a in rs)
            out.append(f"{_t(sec * 1000)} {op} {act} {tg}" + (f" (approving {rid})" if rid else ""))
        return out

    def _notes_lines(ns):
        groups = {}
        for n in sorted(ns, key=lambda n: n.get("timestamp") or 0):
            k = (_cut(n.get("recipient"), 60), _cut(n.get("subject"), 90))
            g = groups.setdefault(k, {"n": 0, "first": n.get("timestamp"), "last": n.get("timestamp")})
            g["n"] += 1; g["last"] = n.get("timestamp")
        out = []
        for (rcpt, subj), g in groups.items():
            out.append(f"{_t(g['first'])} {rcpt}: {subj}" + (f" (×{g['n']}, last {_t(g['last'])})" if g["n"] > 1 else ""))
        return out

    def _section(title, lines):
        return title + "\n" + ("\n".join(f"- {x}" for x in lines) if lines else "- none")

    blocks = [f"**PICTURE AS OF {time.strftime('%H:%M:%SZ', time.gmtime())}**"]
    ordered = sorted(events.values(), key=lambda e: e["t0"])[-8:]
    if not ordered:
        blocks.append("**EVENTS**\n- none in the last 15 minutes")
    for n, e in enumerate(ordered, 1):
        live = sum(1 for r in e["tracks"] if not r.get("outcome"))
        state = f"{live} of {len(e['tracks'])} in flight" if live else f"over, {len(e['tracks'])} tracks"
        c = str(e["country"] or "US").upper()
        whose = "US TARGET" if c in ("US", "USA") else f"COALITION PARTNER TARGET ({c})"
        head = (f"**EVENT {n} · {_t(e['t0'])} · {whose}: {e['target_name'] or e['target_id']}**\n"
                f"{e['weapon']} wave of {len(e['tracks'])} from {e['site'] or 'unknown site'} · {state}")
        body = "\n\n".join([
            _section("INBOUND", _track_lines(e["tracks"])),
            _section("AGENT RECOMMENDATIONS", _rec_lines(e["recs"])),
            _section("OPERATOR ACTIONS", _actions_lines(e["actions"])),
            _section("NOTIFIED", _notes_lines(e["notes"])),
        ])
        blocks.append(head + "\n\n" + body)
    extra = []
    extra += _rec_lines(other_recs[-5:])
    extra += _actions_lines(other_actions)[-5:]
    extra += _notes_lines(other_notes)[-5:]
    if extra:
        blocks.append(_section("**LAUNCH SITES AND OTHER**", extra))
    if ints:
        blocks.append(_section("**INTERCEPTORS AWAY**", ints))
    brief = "\n\n---\n\n".join(blocks)
    # Delivered to the Desktop as-is (see _deliver_direct), so size is a display question, not a token one.
    if _deliver_direct(tool_context, brief):
        return {"ok": True, "delivered": "the brief is the answer; nothing more to write"}
    return {"brief_markdown": brief, "time_now": time.strftime("%H:%M:%SZ", time.gmtime()), "launches_by_site": launches}



_COMPACT_DROP = {"panel", "lat", "lon", "timestamp", "schema", "topic", "source_topic", "raw", "message_id", "wave_id"}


def _compact(v, depth=0):
    """A tool result sized for a 3B model's prompt on a CPU: scalars only two levels down, short strings,
    lists of records reduced to their ids. Everything the model needs for a four-line summary survives."""
    if isinstance(v, dict):
        if depth >= 2:
            return {k: _compact(x, depth + 1) for k, x in list(v.items())[:10] if k not in _COMPACT_DROP and x not in (None, "", [], {}) and not isinstance(x, (dict, list))}
        out = {k: _compact(x, depth + 1) for k, x in v.items() if k not in _COMPACT_DROP and x not in (None, "", [], {})}
        return {k: x for k, x in out.items() if x not in (None, "", [], {})}
    if isinstance(v, list):
        if v and isinstance(v[0], dict):
            ids = [d.get("track_id") or d.get("id") or d.get("recommendation_id") or d.get("name") for d in v]
            return {"count": len(v), "ids": [i for i in ids if i][:6]}
        return [_compact(x, depth + 1) for x in v[:6]]
    if isinstance(v, str) and len(v) > 160:
        return v[:157] + "..."
    if isinstance(v, float):
        return round(v, 1)
    return v

# =============================================================================== one-call workflows
# A small local model reliably makes one tool call per turn, not four in sequence. Each workflow below runs the
# whole procedure in one call and returns what happened; the model's job is the wording and the summary.
async def handle_launch(track_id: str, rationale: str = "", tool_context=None, tool_config: Optional[Dict[str, Any]] = None) -> dict:
    """THE IAMD workflow for a missile or drone launch, in one call: reads the inbound, publishes the ENGAGE
    recommendation for operator approval on the COP, sends the FLASH notification to the host nation (or the
    coalition maritime watch for a ship), and warns airspace and shipping about the corridor. Returns everything
    it found and did. Call this exactly once per launch, then summarize.

    Args:
        track_id: the inbound's track id from the launch event (MSL-… or UAS-…).
        rationale: optional, one sentence for the operator on why to engage; a default is written from the record if empty.
    """
    m = Mesh.get(); tid = track_id.upper()
    if not _in_watch():
        prior = Mesh._watch_result(m.launch_watch.get(tid))
        if prior:
            return prior
    info = await get_inbound_threat(track_id)
    if not info.get("found"):
        return {"ok": False, **info}
    t = info["threat"]
    wave = info.get("wave_members") or []
    who = f"{t.get('weapon')} {t.get('weapon_class')}" + (f" wave of {len(wave) + 1}" if wave else "")
    rationale = rationale or f"{who} launched from {t.get('launch_site')} at {t.get('target_name')}, time to impact {t.get('tti_s')} s; {info.get('can_defend_with') or 'defenses available'}."
    rec = await recommend_engagement(track_id, "engage", rationale)
    msg = (f"FLASH. {who} ({t.get('country')}) launched from {t.get('launch_site')} at {t.get('target_name')} ({t.get('target_id')}), "
           f"time to impact {t.get('tti_s')} s. The asset is engaging; interceptors may fall in the area. Request you alert your air-defence and civil authorities.")
    host = await notify_host_nation(t.get("target_id"), msg)
    air = await notify_airspace_and_maritime(track_id, f"Missile/UAS corridor active from {t.get('launch_site')} to {t.get('target_name')} for the next 10 minutes; interceptor debris possible. Avoid.")
    return _compact({"ok": True, "threat": t, "wave_members": wave, "target_now": info.get("target_now"), "can_defend_with": info.get("can_defend_with"),
            "recommendation": {"id": rec.get("recommendation_id"), "action": "engage", "targets": rec.get("targets"), "awaiting": rec.get("awaiting")},
            "notified_host_nation": host.get("notified"), "notified_airspace": air.get("notified"), "rationale": rationale})


async def handle_ship_threat(ship_id: str, contact_id: str, tool_context=None, tool_config: Optional[Dict[str, Any]] = None) -> dict:
    """THE ship self-defense workflow in one call: computes the geometry (true and relative bearing, range,
    closing speed, time to impact, stage) and notifies the Officer of the Deck and Damage Control Central with
    the recommended alarm, the exact 1MC words, and the options. Call this exactly once, then summarize.

    Args:
        ship_id: the friendly ship's hull number, e.g. DDG-51.
        contact_id: the threat's id (MSL-…, UAS-…, a hostile aircraft or vessel id).
    """
    m = Mesh.get()
    slot = m.ship_watch.get((ship_id.upper(), contact_id.upper()))
    if not _in_watch():
        prior = Mesh._watch_result(slot)
        if prior:
            return prior
    g = await get_ship_threat_geometry(ship_id, contact_id)
    if not g.get("found"):
        return {"ok": False, **g}
    c = g["contact"]
    summary = (f"{c.get('type') or c.get('kind')} {c.get('id')} bearing {g['bearing_spoken']} relative ({g['bearing_true_deg']:03d} true), {g['range_nm']} nm, "
               f"closing {g['closing_kts']} kts" + (f", time to impact {int(g['time_to_impact_s'])} s" if g.get("time_to_impact_s") else "") + f" — {g['stage'].upper().replace('_', ' ')}")
    n = await notify_ship_watch(ship_id, contact_id, summary, g["recommended_alarm"], g["recommended_announcement"], g["bearing_spoken"], g["range_nm"], contact=c, geometry=g)
    return _compact({"ok": True, "geometry": {k: g[k] for k in ("bearing_true_deg", "bearing_relative_deg", "bearing_spoken", "range_nm", "closing_kts", "time_to_impact_s", "stage")},
            "ship": g["ship"], "contact": c, "recommended_alarm": g["recommended_alarm"], "recommended_announcement": g["recommended_announcement"],
            "ood_dcc_notified": n.get("ok"), "panel": n.get("panel")})


async def handle_launch_site(site_id: str, tool_context=None, tool_config: Optional[Dict[str, Any]] = None) -> dict:
    """THE targeting workflow for a launch site that keeps firing, in one call: pulls its launch history, tasks
    ISR imagery to confirm the launcher, and nominates the site to the joint target list for operator approval.
    Call this exactly once, then summarize.

    Args:
        site_id: the launch site id (LS-…) from the alert.
    """
    h = await get_launch_site_history(site_id)
    if not h.get("found"):
        return {"ok": False, **h}
    reason = f"{h['launches']} launches from {h['site']} ({', '.join(h['weapons'])}) at {', '.join(h['targets'])}; confirm launcher position for counter-fire."
    isr = await request_isr(h["site_id"], reason)
    nom = await nominate_target(h["site_id"], reason)
    return {"ok": True, "site": {k: h[k] for k in ("site_id", "site", "country", "launches", "weapons", "targets", "outcomes")},
            "isr_task": isr.get("task_id"), "nomination": {"id": nom.get("recommendation_id"), "awaiting": nom.get("awaiting")}, "rationale": reason}


async def handle_impact(target_id: str, launch_site_id: str, tool_context=None, tool_config: Optional[Dict[str, Any]] = None) -> dict:
    """THE battle-damage workflow after an impact, in one call: tasks BDA imagery on the hit asset, then runs the
    launch-site workflow (history, ISR, nomination) for the site that fired. Call this exactly once, then summarize.

    Args:
        target_id: the asset that was hit (facility id or hull number).
        launch_site_id: the launch site id (LS-…) from the impact event.
    """
    bda = await request_isr(target_id, "BDA after impact: damage, casualties, runway/pier status.")
    site = await handle_launch_site(launch_site_id)
    return {"ok": True, "bda_task": bda.get("task_id"), "target_id": target_id.upper(), "launch_site": site}


# =============================================================================== coalition partner workflow
def _partner_for(country):
    return PARTNER.get(country or "", (_lvl(country or "partner"), f"{COUNTRY_NAME.get(country, country or 'partner nation')} Ministry of Defence"))


def _best_us_shooter(m, lat, lon, kind):
    """Nearest US unit with an interceptor that can reach a target at (lat, lon)."""
    cands = []
    with m.lock:
        for hull, sh in m.ships.items():
            if sh.get("country") == "US" and sh.get("lat") is not None and str(sh.get("vessel_class", "")).upper() != "SUBMARINE":
                d = _nm(lat, lon, sh["lat"], sh["lon"]); reach = 130 if kind == "missile" else 90
                if d <= reach * 1.3:
                    cands.append({"id": hull, "name": f"{sh.get('vessel_type')} ({hull})", "kind": "ship", "weapon": "SM-6 / SM-2 (Aegis)" if kind == "missile" else "SM-2 / ESSM, CAP", "range_nm": round(d), "in_range": d <= reach})
        for fid, f in m.sites.items():
            if f.get("lat") is not None:
                d = _nm(lat, lon, f["lat"], f["lon"]); reach = 110 if kind == "missile" else 25
                if d <= reach * 1.3:
                    cands.append({"id": fid, "name": f.get("unit_name"), "kind": "base", "weapon": "THAAD / Patriot PAC-3" if kind == "missile" else "NASAMS / Coyote, CAP", "range_nm": round(d), "in_range": d <= reach})
    cands.sort(key=lambda c: (not c["in_range"], c["range_nm"]))
    return cands[:3]


async def handle_coalition_attack(track_id: str, tool_context=None, tool_config: Optional[Dict[str, Any]] = None) -> dict:
    """THE coalition liaison assessment for a missile or drone launched at a COALITION PARTNER's ship or base, in one
    call. The partner's national authority has already been notified automatically by the standing watch; this tool
    reads the inbound, finds the US ships and bases best placed to engage on the partner's behalf, and publishes the
    assessment to the coalition event board where the US watch officer chooses: engage from a US unit, stand by and
    monitor, record that the partner engaged, or end the event. Call this exactly once per launch, then summarize.

    Args:
        track_id: the inbound's track id (MSL-… or UAS-…).
    """
    info = await get_inbound_threat(track_id)
    if not info.get("found"):
        return {"ok": False, **info}
    t = info["threat"]; m = Mesh.get(); tid = track_id.upper()
    with m.lock:
        full = dict(m.threats.get(tid, {}))
        w = m.coalition_watch.get(tid)
    country = full.get("target_country") or "partner"
    slug, name = (w or {}).get("partner") or _partner_for(country)
    shooters = _best_us_shooter(m, full.get("target_lat") or 0, full.get("target_lon") or 0, "missile" if full.get("kind") == "missile" else "uas")
    best = shooters[0] if shooters else None
    body = _stamp("coalition-liaison", {
        "recommendation_id": f"REC-{uuid.uuid4().hex[:8].upper()}", "schema": "cop.agent.recommendation.v1", "action": "assess", "requires_approval": False,
        "coalition": True, "action_domain": "coalition", "target_id": tid, "targets": [tid], "wave_id": full.get("wave_id"),
        "partner": name, "partner_id": slug, "partner_country": country, "attacker_country": full.get("country"),
        "summary": (f"{best['name']} best placed: {best['weapon']}, {best['range_nm']} nm from {t.get('target_name')}" if best else "no US unit within interceptor range; partner defences only"),
        "shooters": shooters, "partner_defense": full.get("defended_by"),
        "threat": {k: full.get(k) for k in ("weapon", "weapon_class", "target_id", "target_name", "target_type", "tti_s", "phase", "launch_site", "country")},
        "lat": full.get("lat"), "lon": full.get("lon")})
    topic = m.publish(f"cop/agent/recommendation/v1/{_aor_of(full)}/coalition-liaison/{_lvl(tid)}/assess", body)
    return _compact({"ok": True, "threat": t, "partner_country": country, "partner_already_notified": name, "assessment": body["summary"],
            "us_units_in_reach": shooters, "partner_defense": full.get("defended_by"), "topic": topic,
            "next": "the US watch officer decides on the coalition event board; you are invoked again if a US unit engages"})



async def notify_partner_engagement(track_id: str, tool_context=None, tool_config: Optional[Dict[str, Any]] = None) -> dict:
    """Second notification to the coalition partner, after the US operator approved the engagement: which US unit is
    engaging their attacker, with what, and the state of the shot. Call this exactly once, then summarize.

    Args:
        track_id: the inbound's track id from the approved engagement.
    """
    m = Mesh.get(); tid = track_id.upper()
    rec = None; interceptor = None
    for _ in range(12):                                     # the defender's shot appears on the mesh within seconds of the approval
        with m.lock:
            rec = m.threats.get(tid)
            interceptor = next((i for i in m.interceptors.values() if i.get("engaging") == tid and i.get("phase") in ("STANDBY", "LAUNCH", "CLOSING", "HIT")), None)
        if interceptor or (rec and rec.get("outcome")):
            break
        time.sleep(1.0)
    if not rec:
        return {"ok": False, "error": f"{tid} not in the live picture"}
    w = None
    with m.lock:
        w = m.coalition_watch.get(tid)
    country = rec.get("target_country") or (w or {}).get("rec", {}).get("target_country")
    slug, name = (w or {}).get("partner") or _partner_for(country)
    if interceptor:
        shot = f"{interceptor.get('interceptor')} from {interceptor.get('shooter')} is {interceptor.get('phase', '').lower()}" + (f", {interceptor.get('range_to_target_nm')} nm to the target" if interceptor.get("range_to_target_nm") else "")
    elif rec.get("outcome"):
        shot = f"the inbound is already {rec['outcome']}" + (f" by {rec.get('engaged_by')}" if rec.get("engaged_by") else "")
    else:
        shot = "a US interceptor is being assigned"
    text = (f"US forces are engaging on your behalf: {shot}. Inbound {rec.get('weapon')} {tid} at {rec.get('target_name')}, "
            f"time to impact {rec.get('tti_s')} s. Debris may fall short of the target; you will be told the outcome.")
    body = _stamp("coalition-liaison", {"notification_id": f"NTF-{uuid.uuid4().hex[:8].upper()}", "schema": "cop.notify.v1", "channel": "coalition", "sequence": 2,
                                        "stage": "engaging", "recipient": name, "recipient_id": slug, "partner_country": country, "attacker_country": rec.get("country"),
                                        "subject": f"US engaging for {rec.get('target_name')}", "track_id": tid, "target_id": rec.get("target_id"), "target_name": rec.get("target_name"),
                                        "message": text, "interceptor": (interceptor or {}).get("interceptor"), "shooter": (interceptor or {}).get("shooter"),
                                        "lat": rec.get("lat"), "lon": rec.get("lon"), "priority": "FLASH"})
    topic = m.publish(f"cop/notify/coalition/v1/{_aor_of(rec)}/{slug}/{_lvl(tid)}/engaging", body)
    with m.lock:
        if w: w["notified"].append("engaging")
    return {"ok": True, "partner_notified": name, "message": text, "topic": topic}
