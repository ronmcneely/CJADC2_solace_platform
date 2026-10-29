#!/usr/bin/env python3
"""
Export the live SEMP v2 configuration of both brokers to JSON files.

bootstrap.py is the configuration as code; this is the configuration as the brokers hold it,
read back through the same SEMP v2 API (GET on each collection under the Message VPN plus the
DMR cluster), one file per object type per broker:

    broker/export/<broker>/msgVpn.json
    broker/export/<broker>/aclProfiles.json          (with their publish/subscribe exceptions)
    broker/export/<broker>/clientProfiles.json
    broker/export/<broker>/clientUsernames.json      (passwords are never returned by SEMP)
    broker/export/<broker>/queues.json               (with their topic-to-queue subscriptions)
    broker/export/<broker>/replayLogs.json
    broker/export/<broker>/dmrBridges.json
    broker/export/<broker>/restDeliveryPoints.json   (with REST consumers and queue bindings)
    broker/export/<broker>/dmrClusters.json          (with links and remote addresses)

The files are what a reviewer compares against bootstrap.py, what a pipeline would diff after a
change, and what you would hand over as "the broker configuration". They are the SEMP v2 request
shape, so any object can be re-applied with POST/PATCH to the same collection.

    python3 broker/export.py                 # both brokers -> broker/export/
    python3 broker/export.py --broker conus  # one broker
    python3 broker/export.py --out /tmp/x    # elsewhere

Reads SEMP_PASSWORD from the environment like bootstrap.py (default admin).
https://docs.solace.com/Admin/SEMP/Using-SEMP.htm
"""
import argparse
import json
import os
import sys
from pathlib import Path

import requests

SEMP_PASSWORD = os.environ.get("SEMP_PASSWORD", "admin")
VPN = "cop"
BROKERS = {
    "conus": "http://localhost:8080",
    "fwd":   "http://localhost:8081",
}

# collection under /msgVpns/cop -> (file name, child collections to expand per object, key field)
VPN_COLLECTIONS = {
    "aclProfiles":        ("aclProfiles.json",        ["publishTopicExceptions", "subscribeTopicExceptions"], "aclProfileName"),
    "clientProfiles":     ("clientProfiles.json",     [], "clientProfileName"),
    "clientUsernames":    ("clientUsernames.json",    [], "clientUsername"),
    "queues":             ("queues.json",             ["subscriptions"], "queueName"),
    "replayLogs":         ("replayLogs.json",         [], "replayLogName"),
    "dmrBridges":         ("dmrBridges.json",         [], "remoteNodeName"),
    "restDeliveryPoints": ("restDeliveryPoints.json", ["restConsumers", "queueBindings"], "restDeliveryPointName"),
}


def get(s, url, optional=False):
    r = s.get(url, params={"count": 500})
    if optional and r.status_code == 400:
        return []            # a system object the config API lists but will not expand (e.g. the #ACTIVE link)
    r.raise_for_status()
    body = r.json()
    data = body.get("data", [])
    # follow SEMP paging if a collection is larger than one page
    while body.get("meta", {}).get("paging", {}).get("nextPageUri"):
        r = s.get(body["meta"]["paging"]["nextPageUri"])
        r.raise_for_status()
        body = r.json()
        data += body.get("data", [])
    return data if isinstance(data, list) else [data]


def is_system(obj, key):
    """Objects the broker creates for itself (#ACTIVE, #DEAD_MSG_QUEUE, #P2P/..., #rdp/...) are not ours to export."""
    return str(obj.get(key, "")).startswith("#")


def strip(obj):
    """Drop fields SEMP returns but does not accept back (none today) and secrets it masks."""
    return {k: v for k, v in obj.items() if v is not None}


def export_broker(name, base, out):
    s = requests.Session()
    s.auth = ("admin", SEMP_PASSWORD)
    cfg = f"{base}/SEMP/v2/config"
    vpn = f"{cfg}/msgVpns/{VPN}"
    d = Path(out) / name
    d.mkdir(parents=True, exist_ok=True)

    r = s.get(vpn)
    r.raise_for_status()
    (d / "msgVpn.json").write_text(json.dumps(strip(r.json()["data"]), indent=2) + "\n")
    print(f"  {name}/msgVpn.json")

    for coll, (fname, children, key) in VPN_COLLECTIONS.items():
        items = [strip(x) for x in get(s, f"{vpn}/{coll}") if not is_system(x, key)]
        for it in items:
            k = requests.utils.quote(str(it[key]), safe="")
            for child in children:
                it[child] = [strip(x) for x in get(s, f"{vpn}/{coll}/{k}/{child}", optional=True)]
        (d / fname).write_text(json.dumps(items, indent=2) + "\n")
        print(f"  {name}/{fname}  ({len(items)})")

    clusters = [strip(x) for x in get(s, f"{cfg}/dmrClusters")]
    for c in clusters:
        ck = requests.utils.quote(c["dmrClusterName"], safe="")
        c["links"] = [strip(x) for x in get(s, f"{cfg}/dmrClusters/{ck}/links") if not is_system(x, "remoteNodeName")]
        for link in c["links"]:
            lk = requests.utils.quote(link["remoteNodeName"], safe="")
            link["remoteAddresses"] = [strip(x) for x in get(s, f"{cfg}/dmrClusters/{ck}/links/{lk}/remoteAddresses", optional=True)]
    (d / "dmrClusters.json").write_text(json.dumps(clusters, indent=2) + "\n")
    print(f"  {name}/dmrClusters.json  ({len(clusters)})")


def main():
    ap = argparse.ArgumentParser(description="export the live SEMP v2 configuration of the COP brokers")
    ap.add_argument("--broker", choices=sorted(BROKERS), help="one broker (default: both)")
    ap.add_argument("--out", default=str(Path(__file__).resolve().parent / "export"), help="output directory (default: broker/export)")
    a = ap.parse_args()
    names = [a.broker] if a.broker else list(BROKERS)
    for n in names:
        print(f"{n}  {BROKERS[n]}")
        try:
            export_broker(n, BROKERS[n], a.out)
        except requests.RequestException as e:
            print(f"  cannot read {n}: {e}", file=sys.stderr)
            return 1
    print(f"written to {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
