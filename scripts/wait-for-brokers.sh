#!/usr/bin/env bash
# Block until both brokers answer SEMP and the DMR link is up.
# A PubSub+ container needs 60-90 s after `docker compose up -d` before SEMP
# and SMF accept connections; everything downstream assumes they do.
set -uo pipefail
PW="${SEMP_PASSWORD:-admin}"
TIMEOUT="${TIMEOUT:-180}"
STAGE="${1:-all}"        # semp | all

semp_ok() { curl -sf -u "admin:$PW" "http://localhost:$1/SEMP/v2/monitor/about" >/dev/null; }
link_up() {
  curl -sf -u "admin:$PW" "http://localhost:$1/SEMP/v2/monitor/dmrClusters/$2/links/$3?select=up" 2>/dev/null \
    | grep -q '"up":true'
}

t0=$(date +%s)
for step in "conus:8080" "fwd:8081"; do
  IFS=: read -r name port <<< "$step"
  printf 'waiting for %-6s SEMP ' "$name"
  until semp_ok "$port"; do
    (( $(date +%s) - t0 > TIMEOUT )) && { echo " timeout after ${TIMEOUT}s"; exit 1; }
    printf '.'; sleep 3
  done
  echo " up ($(( $(date +%s) - t0 ))s)"
done

[[ "$STAGE" == "semp" ]] && exit 0

printf 'waiting for DMR link    '
until link_up 8080 cluster-conus fwd && link_up 8081 cluster-fwd conus; do
  (( $(date +%s) - t0 > TIMEOUT )) && { echo " timeout after ${TIMEOUT}s (run: python3 broker/bootstrap.py)"; exit 1; }
  printf '.'; sleep 3
done
echo " up ($(( $(date +%s) - t0 ))s)"
