# Quick boot

**One click:** double-click `scripts\demo-up.cmd` (or run it from a Windows prompt). It opens a Windows Terminal
window with six panes: pane 1 runs `up.sh`, recreates the agent mesh and warms the model; panes 2–5 wait for the
brokers to be bootstrapped and then start the generator, agency feeds, alerting and fusion; pane 6 is a spare shell.
Every pane is in the repo with the venv active, and drops back to a shell if its service is stopped. The tab-by-tab
version below is the same thing by hand.

Windows Terminal → Ubuntu tab. Every tab starts with the same two lines.

WSL quirk: after files in the repo are rewritten from the Windows side, a tab's working directory goes stale
(`FileNotFoundError … _path_abspath` on the next `python3`). The prompt still shows the path; re-enter it:
`cd <repo> && python3 sim/generator.py`.

## Tab 1 · brokers + checks

```bash
cd <repo>
source .venv/bin/activate
scripts/up.sh                  # compose up, wait, bootstrap (idempotent), verify link; 2-3 min
python3 scripts/smoke_mesh.py
```

## Tab 2 · generator

```bash
cd <repo>
source .venv/bin/activate
python3 sim/generator.py
```

## Tab 3 · agency feeds

```bash
cd <repo>
source .venv/bin/activate
python3 sim/agency_feeds.py
```

## Tab 4 · alerting

```bash
cd <repo>
source .venv/bin/activate
python3 services/alerting.py
```

## Tab 5 · fusion

```bash
cd <repo>
source .venv/bin/activate
python3 services/fusion.py
```

## Tab 6 · decision tap (for the action scene)

```bash
cd <repo>
source .venv/bin/activate
python3 scripts/tap_actions.py --full   # each action, then the full decision record; live depth of q.cop.actions / decisions / audit
```

## Tab 7 · consumer (on demand)

```bash
cd <repo>
source .venv/bin/activate
python3 services/consumer.py --queue q.cop.cyber --name soc-1
```

## Missile / UAS scene (no extra tab)

The generator (Tab 2) launches missiles and drone waves on its own and prints `[MSL] LAUNCH …`. The INBOUND panel (top right of the
map) lists what is in flight: **ENGAGE** one, **ENGAGE ×n** a drone wave, or **ENGAGE ALL**. The generator prints
`[IAMD] SM-6 (Aegis) away from …` (or `standing by` until the inbound is in range), the interceptor flies out in blue,
and the inbound ends as a green ✕ (INTERCEPTED) or red ✸ (IMPACT) with the launch site and both tracks left on the
map; the panel's ENGAGEMENTS list and CLEAR button manage the history. A miss (`[IAMD] MISS …`) is followed by a
second shot.
After pulling this version once: `python3 broker/bootstrap.py` (new queue subscriptions), then restart Tabs 2, 4, 5.

## Agent Mesh (optional, Tab 1)

```bash
python3 broker/bootstrap.py                      # once after pulling this version (agent-mesh user/ACL, queue subscriptions)
cp mesh/.env.example mesh/.env                   # once
docker compose --profile mesh up -d --build      # Ollama + Agent Mesh host
docker exec solace-ollama ollama pull qwen2.5:3b   # once; cached in a volume
docker compose logs -f agent-mesh
```
Restart Tabs 2 and 4 after the first pull (wave lead launches, ship-threat alerts). Agents answer in 20–40 s on the CPU model.

## Make it happen on cue (during the demo)

Launches are **on demand only** (no random tempo unless the generator runs with `MISSILE_AUTO=1`).
On the second monitor open **http://localhost:3000/exercise.html**: pick US asset or coalition
partner, drone wave / ballistic missile / cruise missile, a target (live US ships, bases, partner
ships and bases, or random), optionally the launch site, press LAUNCH. The launch log confirms each
launch with the site, target, flags and time-to-impact, then the outcome. Quick buttons stage the
ship attack, an active site and a hostile probe. The same from a shell:

```bash
python3 scripts/inject.py coalition-attack              # a wave or missile at a partner asset: partner MOD notified, you approve, partner told (flags on the cards)
python3 scripts/inject.py ship-attack --target DDG-89   # 3 drones then a missile at the US ship: IAMD + ship-defense agents, 1MC panel
python3 scripts/inject.py probe --target CG-70          # a hostile aircraft turns toward the cruiser: proximity + ship-threat
python3 scripts/inject.py missile --target FAC-010      # one missile at Ali Al Salem: IAMD agent, host-nation notification
python3 scripts/inject.py site-active --site LS-IR-02   # three quick launches from Shiraz: targeting agent, ISR, nomination
```

## Browser

| What | URL | Login |
|---|---|---|
| COP map (CONUS) | http://localhost:3000 | — |
| Exercise control (second monitor): stage launches | http://localhost:3000/exercise.html | — |
| Mesh console: sources, broker, fused data | http://localhost:3000/console.html | — |
| Solace Agent Mesh Desktop (Linux build in WSL: `solace-agent-mesh &`) | its own window · Agents page lists the demo agents · see docs/agent-mesh.md | — |
| Mission threat board (downstream dashboard) | http://localhost:3000/dashboard.html | — |
| Ship's 1MC panel (OOD / DCC) | http://localhost:3000/ship.html?ship=DDG-51 | — |
| Agent Mesh chat UI (orchestrator, A2A activity) | http://localhost:8010 | — |
| COP map (forward site) | http://localhost:3000/?broker=ws://fwd:8001&name=FWD | — |
| SOAR intake (tickets from the broker's RDP) | http://localhost:3001 | — |
| Broker Manager, CONUS | http://conus:8080 | admin / admin |
| Broker Manager, FWD | http://fwd:8081 | admin / admin |
| Try Me! client | in Broker Manager | cop-viewer / cop-demo |

## Or: everything as containers (instead of tabs 2–5)

```bash
docker compose --profile apps up -d --build
docker compose logs -f fusion alerting
```

## DDIL scene (Tab 1)

```bash
scripts/link.sh down
python3 scripts/smoke_mesh.py --publish-only -n 20
scripts/link.sh up
python3 scripts/smoke_mesh.py --consume-only
```

## Shut down

```bash
docker compose --profile mesh --profile apps down   # keeps broker config and the pulled model
docker compose down -v       # wipes it; scripts/up.sh rebuilds it either way
```

---
© 2026 Ron McNeely. Provided for evaluation; no license to reproduce or redistribute the documentation.
