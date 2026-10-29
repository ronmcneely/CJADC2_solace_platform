#!/usr/bin/env bash
# One-shot start: brokers up, wait, configure (idempotent), wait for the mesh, verify.
# Safe to run every time, including after `docker compose down -v`.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd -P)"     # canonical path: WSL's drive mounts choke on ".." in the cwd
cd "$ROOT"
docker compose up -d
"$ROOT/scripts/wait-for-brokers.sh" semp
python3 "$ROOT/broker/bootstrap.py"
# Empty the two queues nothing drains between demos (the audit log and the DMQ). Left alone they hit their
# 500 MB cap after a few days and the broker answers every matching publish with "Spool Over Quota".
for p in 8080 8081; do for q in q.cop.audit q.cop.dmq; do
  curl -s -o /dev/null -u "admin:${SEMP_PASSWORD:-admin}" -X PUT -H 'Content-Type: application/json' -d '{}' \
    "http://localhost:$p/SEMP/v2/action/msgVpns/cop/queues/$q/deleteMsgs" && echo "purged $q on :$p"
done; done
"$ROOT/scripts/wait-for-brokers.sh"
"$ROOT/scripts/verify-dmr.sh"
