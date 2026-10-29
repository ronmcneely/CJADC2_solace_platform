#!/usr/bin/env bash
# Simulate the forward site's satellite link.
#
#   scripts/link.sh down   -> fwd broker loses the backhaul (DMR link) to conus
#   scripts/link.sh up     -> backhaul restored; spooled guaranteed messages flow
#   scripts/link.sh status
#
# Mechanism: detach the fwd container from the inter-site network (cop-mesh).
# It stays on its local network (cop-fwd-site), so local publishers and
# subscribers keep working; only the broker-to-broker path is cut. That is a
# DDIL link.
#
#   scripts/link.sh down --admin / up --admin
# Alternative: administratively shut the DMR link on the fwd side via SEMP
# instead of cutting the network. Same spool-and-resync behaviour, no Docker.
set -euo pipefail
NET=cop-mesh
C=solace-fwd
PW="${SEMP_PASSWORD:-admin}"
SEMP="http://localhost:8081/SEMP/v2/config/dmrClusters/cluster-fwd/links/conus"

mode="${2:-}"
case "${1:-status}" in
  down)
    if [[ "$mode" == "--admin" ]]; then
      curl -s -u "admin:$PW" -X PATCH -H 'content-type: application/json' "$SEMP" -d '{"enabled":false}' >/dev/null
      echo "LINK DOWN (admin): DMR link fwd->conus shut down"
    else
      docker network disconnect "$NET" "$C" && echo "LINK DOWN: $C detached from $NET"
    fi ;;
  up)
    if [[ "$mode" == "--admin" ]]; then
      curl -s -u "admin:$PW" -X PATCH -H 'content-type: application/json' "$SEMP" -d '{"enabled":true}' >/dev/null
      echo "LINK UP (admin): DMR link fwd->conus enabled"
    else
      docker network connect --alias fwd "$NET" "$C" && echo "LINK UP: $C reattached to $NET"
    fi ;;
  status)
    if docker network inspect "$NET" --format '{{range .Containers}}{{.Name}} {{end}}' | grep -q "$C"; then
      echo "NETWORK: attached"
    else
      echo "NETWORK: detached"
    fi
    curl -s -u "admin:$PW" "http://localhost:8081/SEMP/v2/monitor/dmrClusters/cluster-fwd/links/conus?select=enabled,up,uptime" \
      | python3 -c 'import sys,json; d=json.load(sys.stdin)["data"]; print("DMR LINK:", "up" if d["up"] else "DOWN", f"(enabled={d[\"enabled\"]}, uptime={d[\"uptime\"]}s)")' ;;
  *)
    echo "usage: $0 {down|up|status} [--admin]"; exit 1 ;;
esac
