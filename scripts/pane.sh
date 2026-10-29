#!/usr/bin/env bash
# One pane of the demo window (scripts/demo-up.cmd opens six of these in Windows Terminal).
#
#   scripts/pane.sh brokers     up.sh (brokers, bootstrap, DMR check), agent mesh, warm the model, smoke test
#   scripts/pane.sh generator   waits for the brokers, then sim/generator.py
#   scripts/pane.sh feeds       waits, then sim/agency_feeds.py
#   scripts/pane.sh alerting    waits, then services/alerting.py
#   scripts/pane.sh fusion      waits, then services/fusion.py
#   scripts/pane.sh shell       an interactive shell in the repo with the venv active
#
# Every pane starts in the repo with the venv active and drops into an interactive shell (venv still active)
# when its command ends or is stopped with Ctrl+C, so a pane is never lost.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT" || exit 1
source .venv/bin/activate
export SEMP_PASSWORD="${SEMP_PASSWORD:-admin}"

ready() {   # bootstrapped (the audit queue exists) and both brokers + the DMR link are up
  printf 'waiting for the brokers to be bootstrapped '
  until curl -sf -u "admin:$SEMP_PASSWORD" "http://localhost:8080/SEMP/v2/config/msgVpns/cop/queues/q.cop.audit" >/dev/null 2>&1; do
    printf '.'; sleep 3
  done
  echo; scripts/wait-for-brokers.sh
}

run() {   # one copy of each service: a stale one left over from an earlier window fights this one for the queue
  local other; other=$(pgrep -f "python3 $1" | head -1)
  if [[ -n "$other" ]]; then
    echo "$1 was still running from an earlier window (pid $other) -- stopping it and starting here."
    pkill -f "python3 $1"; sleep 2
  fi
  python3 "$1"
}

case "${1:-shell}" in
  brokers)
    scripts/up.sh \
      && docker compose --profile mesh up -d --force-recreate agent-mesh \
      && echo "warming qwen2.5:3b ..." && docker exec solace-ollama ollama run qwen2.5:3b "ok" >/dev/null \
      && python3 scripts/smoke_mesh.py
    echo; echo "brokers, agent mesh and model ready. COP http://localhost:3000  exercise http://localhost:3000/exercise.html"
    ;;
  generator) ready && run sim/generator.py ;;
  feeds)     ready && run sim/agency_feeds.py ;;
  alerting)  ready && run services/alerting.py ;;
  fusion)    ready && run services/fusion.py ;;
  shell)     ;;
  *) echo "usage: scripts/pane.sh brokers|generator|feeds|alerting|fusion|shell" ;;
esac
exec bash --rcfile <(cat ~/.bashrc 2>/dev/null; echo "cd '$ROOT' && source .venv/bin/activate")
