# edge-cop-solace

A JADC2-style common operating picture on a Solace event mesh: seven source
systems, two sites, one intermittent link.

```
 CONUS site (solace-conus)                  Forward site (solace-fwd)
 ┌───────────────────────────┐   DMR link   ┌───────────────────────────┐
 │ VPN: cop                  │◄────────────►│ VPN: cop                  │
 │ sims: log isr pers c2 …   │  (sat link)  │ sims: bft fires wx …      │
 │ queues: orders audit …    │              │ queues: orders logistics  │
 │ map (MQTT/WebSocket)      │              │ fwd-cell map              │
 └───────────────────────────┘              └───────────────────────────┘
```

Everything is self-managed and runs on one laptop. There is no dependency
on a cloud service, which is how it would have to run in an air-gapped
enclave.

## Run it

All commands run from the repo root in a WSL (Ubuntu) terminal with the venv
active. The steps below are the documented path; `scripts/up.sh` runs steps
1, 3 and 4 in one go and is safe to repeat (bootstrap is idempotent).

The deployment follows the Solace [Get Started](https://docs.solace.com/Get-Started/get-started-lp.htm)
path step for step; `docs/solace-docs-trace.md` maps every step here to the
documentation page it came from.

```bash
# 0. Give the two sites names (one-time; Windows hosts file needs an admin prompt)
#    127.0.0.1 conus fwd      -> C:\Windows\System32\drivers\etc\hosts  and  /etc/hosts in WSL
#    Broker Manager keys its login cookie on the host name, so two brokers on
#    "localhost" log each other out; named hosts fix that and read better on screen.

# 1. Deploy the brokers (tutorial step 1, run twice: one per site)
docker compose up -d
#    Broker Manager: http://conus:8080 · http://fwd:8081   (admin / admin)

# 2. Python environment (one-time). Every python3 command below assumes the
#    venv is active: the prompt shows (.venv). Re-run the activate line in each
#    new terminal and after a reboot.
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Configure both brokers as code via SEMP v2 (idempotent)
python3 broker/bootstrap.py

# 4. Verify the DMR link the way the docs do: Admin = Up, Operational = Up
#    (waits for both brokers to finish booting first; 60-90 s after `up -d`)
scripts/verify-dmr.sh

# 5. Verify the data channel across the mesh: publish on fwd, consume on conus
python3 scripts/smoke_mesh.py
```

### Verify with the Solace Try-Me CLI (tutorial steps 3–4)

```bash
stm config init
stm manage connection --url ws://localhost:8008 --vpn cop --username cop-viewer --password cop-demo
stm manage semp-connection --semp-url http://localhost:8080/SEMP/v2/config --semp-vpn cop --semp-username admin --semp-password admin

# terminal 1: subscribe with a wildcard on the CONUS broker
stm receive --topic "cop/air/track/v1/*/>" --client-name "COP Viewer"

# terminal 2: publish on the FWD broker (port 8009) and watch it arrive on CONUS
stm send --topic cop/air/track/v1/fwd/friend/AC-101/active --message '{"lat":26.2,"lon":56.3}' \
  --url ws://localhost:8009 --vpn cop --username sim-bft --password cop-demo
```

## Run the simulator

The simulator plays the sensors at the forward site, so it connects to the
`fwd` broker (SMF on 55556, MQTT on 1884). Everything it publishes reaches the
`conus` broker over the DMR link.

```bash
python3 sim/generator.py
```

Watch it arrive on CONUS with one wildcard:

```bash
stm receive --topic "cop/>" --url ws://localhost:8008 --vpn cop --username cop-viewer --password cop-demo
```

Environment overrides: `SOLACE_HOST`, `SOLACE_USER`, `APP_PASSWORD`, `MQTT_HOST`,
`MQTT_PORT`, `MQTT_ENABLED=0` to skip the MQTT path.

## Run the external agency feeds

Seven agency systems, three protocols, one topic space. DIA, CENTCOM and DLA
POST straight to the broker's REST port; MDA (Maritime Domain Awareness), NGA and FAA publish MQTT; the
coalition partner uses the native API. The broker accepts all three and every
subscriber sees one hierarchy under `cop/intel/...`. No connectors.

```bash
python3 sim/agency_feeds.py
```

The alerting subscriber learns its IOC list from the DIA feed, so a
`[known IOC]` cyber alert is a live join between two sources on the mesh.

## Missiles and attack drones: the closed loop

Launch sites in Iran and Yemen fire ballistic and cruise missiles and Shahed-class
drone waves at friendly ships and bases — on cue, from the **exercise control page**
(http://localhost:3000/exercise.html, meant for a second monitor: US or coalition
target, drone wave / ballistic / cruise, a specific ship or base or a random one,
the launch site) or `scripts/inject.py`; `MISSILE_AUTO=1` on the generator brings
back a random tempo. The page publishes one `cop/c2/action/…/exercise/…/inject`
message as the viewer user; the generator subscribes to it like any other action. A
launch is a guaranteed `cop/air/missile-event/…/launch` (the alerting subscriber
raises a CRITICAL alert with time-to-impact); the flight is a direct
`cop/air/missile/…/{phase}` stream the map draws with its projected track. The
simulator also **subscribes** to `cop/c2/action/v1/>`: an ENGAGE or WEAPONS FREE
from the map (per track, per wave, or ENGAGE ALL) fires an interceptor from the
asset under attack. The interceptor is its own track on
`cop/air/interceptor/v1/{aor}/friend/{interceptor}/{phase}` — STANDBY on the
rail until the inbound is in range, LAUNCH, CLOSING with range to target, then
HIT or MISS; a miss is followed by a second shot while there is time
(shoot-look-shoot). Drones beyond SAM range are run down by the combat air
patrol (an F/A-18E from the carrier or an F-15E from the base), drawn as a
friendly aircraft track. Each shot is a guaranteed `interceptor-event` the alerting
service turns into an alert against the inbound. The inbound's own outcome,
`…/intercepted` or `…/impact`, carries the kill point. On the map the hostile
launch site, the flown path, the interceptor's flyout and the kill point stay
until the operator clears them; the INBOUND panel keeps the engagement history.
Assets that were not engaged by an operator defend themselves about half the
time (`mode: auto`). The decision an operator takes changes what the sensors
report next, and the map, the alerting service, the fusion service, the
dashboard and the SOAR all see the whole engagement through their own
subscriptions.

## Run the alerting subscriber

One plain subscriber on the CONUS broker turns raw streams into operator
alerts (fuel, readiness, hostile proximity, cyber IOC match, fire-control
radar, SATCOM jamming) and publishes them back onto the mesh as
`cop/alert/{type}/v1/{aor}/{severity}/{entity}/new`. Tracks arrive by direct
subscription; cyber, SIGINT and jamming reports arrive through the
`q.cop.alerting` queue so nothing is missed across a restart.

```bash
python3 services/alerting.py
stm receive --topic "cop/alert/>" --url ws://conus:8008 --vpn cop --username cop-viewer --password cop-demo
```

Re-run `python3 broker/bootstrap.py` once after pulling this to create the queue.

## Run the fusion service (enrichment in motion)

One subscriber replaces the stream-processing layer. It learns five reference
tables from the agency feeds (DIA country assessments, DIA threat actors, DIA
indicators, MDA (maritime) vessels of interest, CENTCOM readiness) and joins the live
streams against them, publishing the enriched record back onto the hierarchy
under `cop/fused/...`. Every consumer gets the same enriched picture once.

| Join | Output | Delivery |
|---|---|---|
| hostile air/sea track ⋈ DIA assessment | `cop/fused/air\|sea/v1/{aor}/hostile/{id}/{status}` | direct |
| ground track ⋈ CENTCOM readiness | `cop/fused/land/v1/{aor}/friend/{unit}/{status}` | direct |
| cyber incident ⋈ actor ⋈ IOC ⋈ country | `cop/fused/cyber/v1/{aor}/{facility}/{incident}/{severity}` | guaranteed |
| AIS contact ⋈ MDA (maritime) vessel of interest | `cop/fused/contact/v1/{aor}/voi/{mmsi}/{severity}` | first sighting guaranteed, updates direct |
| operator action ⋈ full target record ⋈ every enrichment ⋈ the alerts ⋈ timeline | **decision record** `cop/decision/record/v1/{aor}/{domain}/{target}/{action}` | guaranteed |

### The decision record

When an operator acts, the fusion service assembles one complete event: the
action, the full record of what it targeted, every piece of intel the mesh
holds on it (DIA actor profile, IOC, country assessment, facility, readiness),
the alerts that preceded it, a timeline (report → first alert → decision, with
both latencies), and the list of source systems and topics that contributed.
It goes out once on `cop/decision/record/v1/...` and three queues attract it:
`q.cop.decisions` for downstream consumers (an AI service, a dashboard, an
analytics store; non-exclusive so N of them share it), `q.cop.audit` for the
record, and `q.cop.soar` so the ticket closes with full context. No consumer
re-joins anything.

```bash
python3 scripts/tap_actions.py --full          # print each decision record as a downstream consumer receives it
python3 services/consumer.py --queue q.cop.decisions --name analytics-1
```
| coalition air/naval (SMF) | normalized into the COP track schema | direct |

```bash
python3 services/fusion.py
stm receive --topic "cop/fused/>" --url ws://conus:8008 --vpn cop --username cop-viewer --password cop-demo
```

The map merges `cop/fused/>` onto its own picture: hostile tracks gain the
DIA assessment in their popup, cyber incidents gain the actor profile and IOC
match, coalition aircraft and ships appear in green, and vessels of interest
get a ringed marker. In Broker Manager, **Clients → cop-fusion → Subscriptions**
is the join's input list; **Queues → q.cop.fusion** is its reference-data
input.

## The mesh console

http://localhost:3000/console.html is one more subscriber on `cop/#`. Left: every
source on one line — protocol (REST, MQTT, SMF), site (CONUS, forward site over
DMR, or a mesh participant), live rate. Middle: a few broker numbers (direct vs
guaranteed, how much crossed the DMR link, the fusion tables filling, the operator
actions). Right, the wide column: the **fused data** — one card per join with the
raw record and its source on the left and, on the right, only the fields the fusion
service added, as highlighted chips (threat level, affiliation, IOC match, readiness,
vessel-of-interest reason…), and the topic it was published on. The latest decision
record is pinned at the top: the action, the full target record, every source merged,
the timings and the agent's recommendation if there was one. The chips can be filtered
by join type. This is the view to leave on screen while talking about sources and
enrichment; the map is the view for operators.

## The mission threat board

http://localhost:3000/dashboard.html is the downstream application the decision
records exist for. It subscribes to `cop/decision/#`, `cop/alert/#`,
`cop/fused/#` and the track streams and computes a mission threat picture in
the browser: a threat index and THREATCON from the weighted active alerts, threat
by domain, which friendly assets are under threat and from what, hostile tracks
and vessels of interest in the window, the decision records as they are
produced with report-to-decision latency, and an alert stream. Nothing here
calls an API or reads a store; it is one more subscriber. Open it against the
forward broker with `?broker=ws://fwd:8001`.

## Agents on the mesh

Solace Agent Mesh runs on the same broker under its own username and ACL
(`docker compose --profile mesh up -d --build`, a local Ollama model, no cloud).
The agents are event-driven: the Event Mesh Gateway subscribes to the COP topics
and turns a launch at a **coalition partner's** ship or base into a task for the
**coalition liaison agent** (the partner's national authority is notified at
once, a US operator approves US forces engaging on their behalf, the partner is
told who is engaging and the outcome), a launch at a **US** asset into a task for
the **IAMD agent** (engagement for operator approval, host-nation notification,
NOTAM/NAVWARN), a ship-threat
alert into a task for the **ship self-defense agent** (bearing, range, time to
impact; the OOD and Damage Control Central get the 1MC options with the words
ready to say, on `ship.html`), and an impact or a busy launch site into a task
for the **targeting agent** (ISR tasking, target nomination). No agent fires:
approvals travel the same `cop/c2/action` topic a manual ENGAGE does, the ACL
forbids the agents that topic, and the decision record carries the
recommendation and the seconds the human took. A chat UI on :8010 lets the
watch floor ask the orchestrator what is going on. Details, timing on a CPU and
the venv alternative: `docs/agent-mesh.md`.

## Integrations inside the broker

Two directions, both visible in Broker Manager rather than in application code.

**In.** The seven agency feeds are ordinary clients on the **Clients** page,
one row per protocol: `feed-coalition-c2-smf` (SMF), `feed-mda-nga-faa-mqtt`
(MQTT), and the REST publishers (DIA, CENTCOM, DLA) as transient REST
clients. Each row shows protocol, message counts and subscriptions.

**Out.** A **REST Delivery Point** (`rdp-soar`) is configured in the broker by
`bootstrap.py`: it binds to `q.cop.soar` (every `cop/alert/>`) and POSTs each
alert to the `cop-sink` container, a plain HTTP server standing in for a SOAR
or ticketing system. No Solace client runs in the sink. See **Clients → REST
Delivery Points** for the RDP, its REST consumer and queue binding, with up/down
state and counters; the tickets are at http://localhost:3001.

Solace's catalogue connectors (Micro-Integrations) work the same way for
databases, message queues, files and SaaS APIs; this demo uses none because every
source speaks REST, MQTT or SMF natively.

## Run everything as containers

```bash
docker compose --profile apps up -d --build     # brokers, map, sink, generator, feeds, alerting, fusion
docker compose logs -f fusion alerting          # the same logs the terminals show
docker compose --profile apps down              # stop the apps too
```

The image is built once from `Dockerfile`; each service is the same image
with a different `command`.

## Run a queue consumer

`q.cop.audit`, `q.cop.cyber` and `q.cop.intel` fill up whether or not anyone is
listening; that is guaranteed messaging doing its job. `services/consumer.py`
binds to a queue, prints what arrives and acknowledges it.

```bash
python3 services/consumer.py                              # drain the audit queue
python3 services/consumer.py --queue q.cop.cyber --name soc-1   # terminal A
python3 services/consumer.py --queue q.cop.cyber --name soc-2   # terminal B: the two share the stream
python3 services/consumer.py --queue q.cop.audit --replay 10m   # a late consumer rewinds via the replay log
python3 services/consumer.py --slow 0.5                   # watch "Messages Queued" drain in Broker Manager
```

## Open the COP

The map is a subscriber. It connects to the CONUS broker over MQTT/WebSocket
(`ws://localhost:8000`), subscribes to `cop/#` once, and keeps the live
picture in the browser. Operator decisions (block IP, jam, scramble CAP…) are
published straight back to the broker as `cop/c2/action/v1/...` guaranteed
messages; the action log is fed by the subscription, so every open screen
sees every decision. There is no database and no API server.

The map is served by the `cop-map` container in `docker-compose.yml`
(nginx, port 3000), so it comes up with the brokers:

```bash
docker compose up -d
# open http://localhost:3000
```

Options in the URL: `?broker=ws://fwd:8001&name=FWD` opens a forward-site
screen against the forward broker, which keeps working when the backhaul is cut.

## The DDIL scene

```bash
scripts/link.sh down                # forward site loses its link
python3 scripts/smoke_mesh.py --publish-only -n 20
#   -> watch the DMR spool grow on the fwd broker
scripts/link.sh up                  # link returns
python3 scripts/smoke_mesh.py --consume-only
#   -> 20 messages arrive on conus, in order, no app code involved
```

## Layout

| Path | What |
|------|------|
| `docker-compose.yml` | the two brokers, map, SOAR sink, the `apps` profile and the `mesh` profile (Agent Mesh + Ollama) |
| `broker/bootstrap.py` | VPN, client profile, ACLs, usernames, queues, replay log, DMR cluster/link/bridge |
| `broker/export.py` | reads the live SEMP v2 configuration back from both brokers into `broker/export/<broker>/*.json` (VPN, ACLs with exceptions, client profiles, usernames, queues with subscriptions, replay log, DMR bridge and cluster/links, RDP) — the configuration as the brokers hold it, to diff against `bootstrap.py` or hand to a pipeline |
| `docs/topics.md` | topic hierarchy, subscriptions, QoS, ACL rationale |
| `docs/solace-docs-trace.md` | every step mapped to the Solace documentation page it follows |
| `docs/quick-boot.md` | just the commands, one tab at a time |
| `scripts/link.sh` | cut / restore the inter-site link |
| `scripts/up.sh` | one-shot start: compose up, wait, bootstrap, verify the link |
| `scripts/wait-for-brokers.sh` | blocks until SEMP answers on both brokers and the DMR link is up |
| `scripts/verify-dmr.sh` | `show cluster * link *` on both brokers, per the DMR multi-site example |
| `scripts/smoke_mesh.py` | end-to-end proof across the mesh |
| `scripts/demo-up.cmd` · `scripts/pane.sh` | one click on Windows: a Windows Terminal window with the six demo panes (brokers, generator, feeds, alerting, fusion, shell), each in WSL with the venv active, services waiting for the brokers |
| `scripts/inject.py` | exercise control from a shell: stage a ship attack, a probe, a launch or an active site on cue |
| `map/exercise.html` | exercise control for a second monitor: US / coalition target, drone wave / ballistic / cruise, target and site pickers, launch log |
| `scripts/tap_topic.py` | raw topic tap over MQTT, the way the browser sees the mesh |
| `scripts/mesh-check.sh` | Agent Mesh health: Ollama, the model, the agent host, the last LLM error |
| `scripts/tap_actions.py` | live tap of operator actions and the decision records built from them, with queue depths from SEMP; consumes nothing |
| `sim/generator.py` | the multi-domain simulator: air, sea, land, cyber, SIGINT, space, EW (SMF + MQTT) |
| `sim/solace_transport.py` | topic routing and QoS selection for every stream; the operator-action subscription |
| `sim/missile_threats.py` | missiles and one-way-attack UAS: launch sites, weapons, phases, intercepts; ENGAGE from the map closes the loop |
| `sim/agency_feeds.py` | seven external agency feeds over REST, MQTT and SMF |
| `services/alerting.py` | the alerting subscriber: six rules, alerts published as events |
| `services/fusion.py` | the fusion service: five reference tables, five joins, enriched events under `cop/fused/` |
| `services/sink/sink.py` | SOAR intake: target of the broker's REST Delivery Point |
| `services/consumer.py` | queue consumer: guaranteed delivery, round-robin scaling, replay |
| `Dockerfile` | one image for every Python participant; compose profile `apps` |
| `map/index.html` | the COP: Leaflet map subscribing over MQTT/WebSocket, publishing operator actions |
| `docs/architecture.html` | the architecture: end-to-end flow, where fusion happens, the decision record, the agentic flows, source-to-consumer mapping (open in a browser) |
| `mesh/desktop/desktop.env` | points Solace Agent Mesh Desktop (Linux build, run in WSL2) at the demo broker, so the vendor's own app lists and drives the demo's agents |
| `map/console.html` | the mesh console: sources, broker, and the fused data — raw in, fields fusion added, published once |
| `map/dashboard.html` | the mission threat board: a downstream consumer of decision records and alerts |
| `map/ship.html` | a ship's 1MC panel: threat geometry, the ship-defense agent's OOD/DCC notification, alarms and announcements |
| `docs/agent-mesh.md` | the agent workflows, topics, timing, how to run |
| `mesh/configs/` | Solace Agent Mesh: shared config (broker, LLM), four agents, the Event Mesh Gateway, the chat UI |
| `mesh/src/cop_tools/` | the agents' tools: live picture from the broker, recommendations, tasking, notifications |
| `mesh/Dockerfile` | the published Agent Mesh image plus the Event Mesh Gateway plugin; compose profile `mesh` (with Ollama) |

## Resource notes

Each broker container is started with `system_scaling_maxconnectioncount=1000`
(DMR requires at least the 1000-connection tier; 100 is refused) and 1 GB shm.
Two brokers fit in a 12 GB WSL2 allocation with room for the simulators and
the map. Scaling parameters are read at first boot, so changing them means
`docker compose down -v` and `up -d` again.

---
© 2026 Ron McNeely. Provided for evaluation; no license to reproduce or redistribute the documentation.
