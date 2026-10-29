#!/usr/bin/env bash
# Verifies the DMR link the way the Solace multi-site example does
# (step 3: "show cluster <name> link *", expect Admin and Operational = Up),
# then confirms it through the SEMP v2 monitor API.
# https://docs.solace.com/Features/DMR/DMR-Examples-Multi-Site-Config.htm
set -uo pipefail
PW="${SEMP_PASSWORD:-admin}"
"$(dirname "$0")/wait-for-brokers.sh" || exit 1

for c in solace-conus:8080:cluster-conus:fwd solace-fwd:8081:cluster-fwd:conus; do
  IFS=: read -r name port cluster peer <<< "$c"
  echo "== $name"
  printf 'show cluster * link *\n' | docker exec -i "$name" cli -A 2>/dev/null \
    | sed -n '/Cluster\|Link\|Admin\|Oper\|Up\|Down\|----/p'
  echo "-- SEMP monitor: $cluster -> $peer"
  curl -s -u "admin:$PW" \
    "http://localhost:$port/SEMP/v2/monitor/dmrClusters/$cluster/links/$peer?select=remoteNodeName,enabled,up,uptime,failureReason" \
    | python3 -c 'import sys,json; d=json.load(sys.stdin)["data"]; print("   ", {k:d.get(k) for k in ("remoteNodeName","enabled","up","uptime","failureReason")})'
done
