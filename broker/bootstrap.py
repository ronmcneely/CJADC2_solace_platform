#!/usr/bin/env python3
"""
Config-as-code for the two-broker COP event mesh, via SEMP v2.

Idempotent: every object is created with POST and, if it already exists,
updated with PATCH. Run it as often as you like.

    python broker/bootstrap.py            # configure both brokers
    python broker/bootstrap.py --only conus

Everything the demo relies on is declared here, so a reviewer can read one
file and see the whole broker design: the Message VPN, the client profile,
ACL profiles carved along the topic hierarchy, client usernames per source
system, queues with topic-to-queue mappings, the dead-message queue, the
replay log, and the DMR cluster + external link that joins the two sites.
"""
import argparse
import os
import sys
import time

import requests
import urllib3

urllib3.disable_warnings()

SEMP_PASSWORD = os.environ.get("SEMP_PASSWORD", "admin")
VPN = "cop"
CLUSTER_PASSWORD = os.environ.get("DMR_PASSWORD", "cop-mesh-secret")
SOAR_HOST = os.environ.get("SOAR_HOST", "cop-sink")      # reachable from the conus container (cop-mesh network)
SOAR_PORT = int(os.environ.get("SOAR_PORT", "8088"))
APP_PASSWORD = os.environ.get("APP_PASSWORD", "cop-demo")

BROKERS = {
    # name: (SEMP base URL from the host, hostname inside the docker network)
    "conus": ("http://localhost:8080", "conus"),
    "fwd":   ("http://localhost:8081", "fwd"),
}

# ---------------------------------------------------------------- topic design
# cop/{domain}/{event}/v1/{aor}/{source|iff}/{entity}/{status}     (docs/topics.md)
#
# domain : air | sea | land | cyber | sigint | space | ew | intel | c2 | alert
# event  : track | site | incident | intercept | image | jamming | action | ...
# aor    : centcom | eucom | africom | indopacom
#
# Direct (non-persistent) : air/sea/land/ew tracks, sites
# Guaranteed (persistent) : incidents, intercepts, imagery, jamming, intel, actions, alerts
TOPIC_ROOT = "cop"

# ----------------------------------------------------------------- ACL design
# One ACL profile per role. Publish/subscribe exceptions follow the hierarchy,
# so "need to know" is enforced by the broker, not by the apps.
ACL_PROFILES = {
    "acl-source-system": {
        "publish":   ["cop/>"],                       # simulators and agency feeds
        "subscribe": ["cop/c2/>", "cop/intel/coalition-naval/>", "#P2P/>"],   # operator actions close the loop; partner ship positions are targets
    },
    "acl-cop-viewer": {
        "publish":   ["cop/c2/action/v1/>"],          # the map may only publish operator actions
        "subscribe": ["cop/>", "solace-agent-mesh/>", "#P2P/>"],   # the console also watches the A2A control plane
    },
    "acl-alerting": {
        "publish":   ["cop/alert/>"],                 # the alerting subscriber may only raise alerts
        "subscribe": ["cop/>", "#P2P/>"],
    },
    "acl-fusion": {
        "publish":   ["cop/fused/>", "cop/decision/>"],   # enriched events and decision records
        "subscribe": ["cop/>", "#P2P/>"],
    },
    # Agent Mesh lives on the broker under its own root, solace-agent-mesh/ (the A2A control plane, agent cards,
    # the Desktop's control and configuration topics); what the agents say to everyone else is under cop/agent and
    # cop/notify. Neither profile may publish cop/c2/action: an agent cannot fire, the Desktop cannot either.
    "acl-agent-mesh": {
        "publish":   ["solace-agent-mesh/>", "cop/agent/>", "cop/notify/>"],
        "subscribe": ["cop/>", "solace-agent-mesh/>", "#P2P/>"],
    },
    "acl-centcom-cell": {
        "publish":   ["cop/*/*/v1/centcom/>"],        # a regional cell may publish only its own AOR
        "subscribe": ["cop/*/*/v1/centcom/>", "cop/c2/>", "#P2P/>"],
    },
}

CLIENT_USERNAMES = {
    # username: acl profile
    "sim-bft":       "acl-source-system",   # the multi-domain simulator (SMF + MQTT)
    "agency-feeds":  "acl-source-system",   # DIA / CENTCOM / DLA over REST
    "cop-viewer":    "acl-cop-viewer",      # the browser map (MQTT over WebSocket)
    "cop-alerting":  "acl-alerting",        # the alerting subscriber
    "cop-fusion":    "acl-fusion",          # the fusion (enrichment) service
    "agent-mesh":    "acl-agent-mesh",      # Solace Agent Mesh: agents, gateways and their COP tools
    "agent-mesh-desktop": "acl-agent-mesh", # Solace Agent Mesh Desktop (Linux build in WSL), same rights, its own name on the Clients page
    "centcom-cell":  "acl-centcom-cell",
}

# ----------------------------------------------------------------- queue design
# Guaranteed consumers read from queues; the queue attracts events with
# topic-to-queue mappings (wildcards allowed). Mappings only name guaranteed
# streams so the spool never fills with direct track updates.
QUEUES = {
    "q.cop.dmq": {
        "accessType": "non-exclusive", "subs": [],
        "note": "dead message queue for the VPN",
    },
    "q.cop.actions": {
        "accessType": "exclusive",
        "subs": ["cop/c2/action/v1/>"],
        "note": "operator decisions: ordered, single consumer",
    },
    "q.cop.cyber": {
        "accessType": "non-exclusive",
        "subs": ["cop/cyber/incident/v1/>"],
        "note": "round-robin across N SOC consumers",
    },
    "q.cop.intel": {
        "accessType": "non-exclusive",
        "subs": ["cop/sigint/intercept/v1/>", "cop/ew/jamming/v1/>",
                 "cop/space/image/v1/>", "cop/intel/*/v1/>"],
    },
    "q.cop.alerting": {
        "accessType": "non-exclusive",
        "subs": ["cop/cyber/incident/v1/>", "cop/sigint/intercept/v1/>", "cop/ew/jamming/v1/>",
                 "cop/air/missile-event/v1/>", "cop/air/uas-event/v1/>", "cop/air/interceptor-event/v1/>"],
        "note": "input to the alerting subscriber; non-exclusive so N instances share the load",
    },
    "q.cop.fusion": {
        "accessType": "exclusive",      # one fusion instance; exclusive is required for client-initiated replay
        "subs": ["cop/intel/assessment/v1/>", "cop/intel/actor/v1/>", "cop/intel/ioc/v1/>",
                 "cop/intel/voi/v1/>", "cop/intel/readiness/v1/>", "cop/cyber/incident/v1/>",
                 "cop/c2/action/v1/>", "cop/agent/recommendation/v1/>"],
        "note": "reference data and reports for the fusion service (replayed at startup); tracks arrive direct",
    },
    "q.cop.soar": {
        "accessType": "non-exclusive",
        "subs": ["cop/alert/>", "cop/decision/>", "cop/notify/>"],
        "note": "consumed by the broker's REST Delivery Point: alerts open tickets, decision records close them with full context, notifications go out to partners",
    },
    "q.cop.decisions": {
        "accessType": "non-exclusive",
        "subs": ["cop/decision/>"],
        "note": "the downstream feed: complete decision records for AI, dashboards, analytics; N consumers share it",
    },
    "q.cop.audit": {
        "accessType": "exclusive",
        "subs": ["cop/cyber/>", "cop/sigint/>", "cop/space/>", "cop/ew/jamming/>", "cop/air/missile-event/>", "cop/air/uas-event/>", "cop/air/interceptor-event/>",
                 "cop/intel/>", "cop/c2/>", "cop/alert/>", "cop/fused/cyber/>", "cop/fused/contact/>", "cop/decision/>",
                 "cop/agent/>", "cop/notify/>"],
        "note": "every report, decision and alert, once, for the record",
    },
}


class Semp:
    def __init__(self, base):
        self.base = base.rstrip("/") + "/SEMP/v2/config"
        self.s = requests.Session()
        self.s.auth = ("admin", SEMP_PASSWORD)
        self.s.verify = False
        self.s.headers["content-type"] = "application/json"

    def wait_ready(self, timeout=240):
        url = self.base + "/about/api"
        t0 = time.time()
        while time.time() - t0 < timeout:
            try:
                r = self.s.get(url, timeout=5)
                if r.status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(3)
        sys.exit(f"broker at {self.base} did not become ready")

    def upsert(self, collection, key_field, body, immutable=False):
        """POST; on 'already exists' fall back to PATCH.

        Some SEMP objects (topic exceptions, queue subscriptions, remote
        addresses) are create-or-delete only, so for those 'already exists'
        is success.
        """
        for attempt in range(40):
            r = self.s.post(self.base + collection, json=body)
            if r.status_code == 200:
                return "created"
            err = r.json().get("meta", {}).get("error", {})
            # Right after boot the SEMP API answers before the message spool is
            # online; guaranteed-messaging objects (queues, replay log, DMR) fail
            # with this status until it is. Wait it out.
            if err.get("status") == "MESSAGE_SPOOL_DATA_NOT_AVAILABLE":
                if attempt == 0:
                    print("  (message spool not online yet, waiting)", flush=True)
                time.sleep(3)
                continue
            break
        if err.get("status") == "ALREADY_EXISTS":
            if immutable:
                return "exists"
            key = requests.utils.quote(str(body[key_field]), safe="")
            r2 = self.s.patch(f"{self.base}{collection}/{key}", json=body)
            if r2.status_code != 200:
                raise RuntimeError(f"PATCH {collection}/{key}: {r2.status_code} {r2.text}")
            return "updated"
        raise RuntimeError(f"{collection}: {r.status_code} {r.text}")

    def patch(self, path, body):
        r = self.s.patch(self.base + path, json=body)
        r.raise_for_status()


def configure(name, semp_url, hostname, peer_name, peer_host):
    print(f"\n== {name} ({semp_url})")
    semp = Semp(semp_url)
    semp.wait_ready()
    v = f"/msgVpns/{VPN}"

    # 1. Free the MQTT / REST ports from the default VPN so 'cop' can own them.
    semp.patch("/msgVpns/default", {
        "serviceMqttPlainTextEnabled": False,
        "serviceMqttWebSocketEnabled": False,
        "serviceRestIncomingPlainTextEnabled": False,
    })

    # 2. Message VPN = the enclave boundary.
    print(" vpn", semp.upsert("/msgVpns", "msgVpnName", {
        "msgVpnName": VPN,
        "enabled": True,
        # Internal basic auth: usernames and passwords live on the broker (step 5).
        # Everything else off, so a re-run always restores a known login model.
        "authenticationBasicEnabled": True,
        "authenticationBasicType": "internal",
        "authenticationClientCertEnabled": False,
        "authenticationOauthEnabled": False,
        "authenticationKerberosEnabled": False,
        "maxMsgSpoolUsage": 1500,                 # MB of guaranteed-message spool
        "dmrEnabled": True,
        "replicationEnabled": False,
        "serviceMqttPlainTextEnabled": True,
        "serviceMqttPlainTextListenPort": 1883,
        "serviceMqttWebSocketEnabled": True,
        "serviceMqttWebSocketListenPort": 8000,
        "serviceRestIncomingPlainTextEnabled": True,
        "serviceRestIncomingPlainTextListenPort": 9000,
        "serviceRestMode": "messaging",
        "eventPublishClientEnabled": True,
        "eventPublishMsgVpnEnabled": True,
    }))

    # 3. Client profile: allow guaranteed messaging, endpoint creation, bridges.
    print(" client-profile", semp.upsert(v + "/clientProfiles", "clientProfileName", {
        "clientProfileName": "cop-apps",
        "allowGuaranteedMsgSendEnabled": True,
        "allowGuaranteedMsgReceiveEnabled": True,
        "allowGuaranteedEndpointCreateEnabled": True,
        "allowTransactedSessionsEnabled": True,
        "allowBridgeConnectionsEnabled": True,
        "elidingEnabled": True,                   # latest-value delivery for slow track consumers
        "elidingDelay": 500,
        "maxConnectionCountPerClientUsername": 200,       # an Agent Mesh host opens one connection per app (agents, gateways, services): ~25 each
    }))

    # 4. ACL profiles along the hierarchy.
    for acl, rules in ACL_PROFILES.items():
        print(" acl", acl, semp.upsert(v + "/aclProfiles", "aclProfileName", {
            "aclProfileName": acl,
            "clientConnectDefaultAction": "allow",
            "publishTopicDefaultAction": "disallow",
            "subscribeTopicDefaultAction": "disallow",
        }))
        for t in rules["publish"]:
            semp.upsert(f"{v}/aclProfiles/{acl}/publishTopicExceptions",
                        "publishTopicException",
                        {"publishTopicExceptionSyntax": "smf", "publishTopicException": t}, immutable=True)
        for t in rules["subscribe"]:
            semp.upsert(f"{v}/aclProfiles/{acl}/subscribeTopicExceptions",
                        "subscribeTopicException",
                        {"subscribeTopicExceptionSyntax": "smf", "subscribeTopicException": t}, immutable=True)

    # 5. One client username per source system / role.
    for user, acl in CLIENT_USERNAMES.items():
        semp.upsert(v + "/clientUsernames", "clientUsername", {
            "clientUsername": user,
            "password": APP_PASSWORD,
            "enabled": True,
            "aclProfileName": acl,
            "clientProfileName": "cop-apps",
        })
    print(" client-usernames", len(CLIENT_USERNAMES))

    # 6. Queues + topic-to-queue mappings. DMQ first so others can reference it.
    for q, spec in QUEUES.items():
        semp.upsert(v + "/queues", "queueName", {
            "queueName": q,
            "accessType": spec["accessType"],
            "permission": "consume",
            "ingressEnabled": True,
            "egressEnabled": True,
            "maxMsgSpoolUsage": 500,
            "respectTtlEnabled": True,
            "deadMsgQueue": "q.cop.dmq" if q != "q.cop.dmq" else "#DEAD_MSG_QUEUE",
            "maxRedeliveryCount": 3,
        })
        for sub in spec["subs"]:
            semp.upsert(f"{v}/queues/{q}/subscriptions", "subscriptionTopic",
                        {"subscriptionTopic": sub}, immutable=True)
    print(" queues", len(QUEUES))

    # 7. Replay log: lets a late consumer rewind the guaranteed stream.
    print(" replay-log", semp.upsert(v + "/replayLogs", "replayLogName", {
        "replayLogName": "cop-replay",
        "ingressEnabled": True,
        "egressEnabled": True,
        "maxSpoolUsage": 500,
    }))

    # 8. DMR, following the Solace multi-site example
    #    (https://docs.solace.com/Features/DMR/DMR-Examples-Multi-Site-Config.htm):
    #    each site is its own single-node cluster with a shared basic-auth
    #    password; an EXTERNAL link joins the two clusters; 'conus' is the
    #    initiator (initiator local + connect-via), 'fwd' accepts (initiator remote).
    #    CLI equivalent:  routing > dynamic-message-routing > create cluster <name>
    #                     > authentication basic password <pw> > no shutdown
    cluster = f"cluster-{name}"
    print(" dmr-cluster", semp.upsert("/dmrClusters", "dmrClusterName", {
        "dmrClusterName": cluster,
        "enabled": True,
        "nodeName": hostname,
        "authenticationBasicEnabled": True,
        "authenticationBasicPassword": CLUSTER_PASSWORD,
    }))
    #    CLI equivalent:  cluster <name> > create link <remote-node> > span external
    #                     > initiator local|remote > [connect-via host:55555] > no shutdown
    #    Transport is left at the documented default (plain, port 55555); TLS is the
    #    production setting (transport ssl, 55443) and is discussed on the slides.
    #    Same order as the CLI: the link is created shut down, connect-via is
    #    set, then "no shutdown" (the broker refuses an address change on an
    #    enabled link).
    initiator = "local" if name == "conus" else "remote"
    link = f"/dmrClusters/{cluster}/links"
    print(" dmr-link", semp.upsert(link, "remoteNodeName", {
        "remoteNodeName": peer_name,
        "enabled": False,
        "span": "external",
        "initiator": initiator,
        "authenticationScheme": "basic",
        "authenticationBasicPassword": CLUSTER_PASSWORD,
        "transportTlsEnabled": False,
        "transportCompressedEnabled": False,
    }))
    if initiator == "local":
        semp.upsert(f"{link}/{peer_name}/remoteAddresses",
                    "remoteAddress", {"remoteAddress": f"{peer_host}:55555"}, immutable=True)
    semp.patch(f"{link}/{peer_name}", {"enabled": True})

    # 9. DMR bridge, per "Configuring DMR on a Message VPN"
    #    (https://docs.solace.com/Features/DMR/DMR-Mgmt-Msg-Vpn.htm):
    #    the VPN already has dmrEnabled=true (step 2); the bridge joins it to the
    #    same-named VPN on the peer.
    #    CLI equivalent:  message-vpn cop > dynamic-message-routing > no shutdown
    #                     > create dmr-bridge <remote-node> > remote message-vpn cop
    print(" dmr-bridge", semp.upsert(v + "/dmrBridges", "remoteNodeName", {
        "remoteNodeName": peer_name,
        "remoteMsgVpnName": VPN,
    }, immutable=True))

    # 10. REST Delivery Point: an outbound integration owned by the broker.
    #     The RDP binds to q.cop.soar (every cop/alert/>) and POSTs each message
    #     to the SOAR intake container; no application code consumes the queue.
    #     https://docs.solace.com/Services/Managing-RDPs.htm
    #     CLI equivalent:  message-vpn cop > rest > create rest-delivery-point rdp-soar
    #                      > create rest-consumer soar-intake > remote host cop-sink / port 8088
    #                      > create queue-binding q.cop.soar > post-request-target /alerts
    if name == "conus":
        rdp = v + "/restDeliveryPoints"
        semp.upsert(rdp, "restDeliveryPointName", {
            "restDeliveryPointName": "rdp-soar", "clientProfileName": "cop-apps", "enabled": False})
        semp.upsert(rdp + "/rdp-soar/restConsumers", "restConsumerName", {
            "restConsumerName": "soar-intake", "remoteHost": SOAR_HOST, "remotePort": SOAR_PORT,
            "tlsEnabled": False, "enabled": True})
        semp.upsert(rdp + "/rdp-soar/queueBindings", "queueBindingName", {
            "queueBindingName": "q.cop.soar", "postRequestTarget": "/alerts",
            "requestTargetEvaluation": "none"}, immutable=True)
        print(" rdp", semp.upsert(rdp, "restDeliveryPointName", {
            "restDeliveryPointName": "rdp-soar", "enabled": True}))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=list(BROKERS))
    args = ap.parse_args()
    names = [args.only] if args.only else list(BROKERS)
    peers = {"conus": "fwd", "fwd": "conus"}
    for n in names:
        semp_url, host = BROKERS[n]
        configure(n, semp_url, host, peers[n], BROKERS[peers[n]][1])
    print("\ndone. Broker Manager: http://localhost:8080 (conus)  http://localhost:8081 (fwd)")


if __name__ == "__main__":
    main()
