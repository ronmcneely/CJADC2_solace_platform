# Agent Mesh on the COP

Solace Agent Mesh runs on the same CONUS broker as the sensors and the map, in the same
Message VPN, under its own client username and ACL. The agents are **event-driven**: nothing
polls, nobody types a prompt. The Event Mesh Gateway holds subscriptions on the COP topics,
turns each matching event into an A2A task for the right agent, and publishes the agent's
answer back onto the mesh. The agents' tools read the live picture and publish
recommendations, tasking and notifications on `cop/agent/…` and `cop/notify/…`.

No agent fires a weapon. An engagement recommendation becomes an action only when an
operator approves it on the COP (or has set the IAMD panel to AUTO), and the approval
travels the same `cop/c2/action` topic a manual ENGAGE does. The fusion service writes the
decision record with the recommendation attached and the seconds the human took to decide.

## Three workflows

Two attack workflows, told apart by whose asset is hit. The alerting service publishes one routing
alert per launch, `cop/alert/inbound/v1/{aor}/{sev}/{track}/{us|coalition}`; the last topic level is
the only thing the gateway looks at.

| Trigger (topic the gateway subscribes to) | Agent | What it does | Human in the loop |
|---|---|---|---|
| `cop/alert/inbound/v1/*/*/*/coalition` — a launch at a **coalition partner's** ship or base (RAF Akrotiri, French Forces UAE, JSDF Djibouti, Saudi and Emirati bases, partner ships from the coalition C2 feed) | **standing watch** (in the agent host, no model in the path) then **CoalitionLiaisonAgent** | the watch sends notification **#1** to the partner's national authority (UK PJHQ, French CPCO, ADF HQJOC, RSADF, UAE ADOC, …) the moment the launch is on the mesh; the agent then runs `handle_coalition_attack`: which US ships and bases are in reach, put on the **coalition event board** (the red panel) | the US watch officer decides on the board: **TARGET FROM US** (picks the US unit; `cop/c2/action/…/coalition/{track}/engage` with `shooter`, the generator fires from that unit, the gateway wakes the agent → `notify_partner_engagement` **#2**), **STANDBY & MONITOR** (`monitor`), **PARTNER ENGAGED** (`partner_engaged`: the partner's own defences fire), **END EVENT**; each decision is a notice to the partner, and the watch sends the **outcome** on INTERCEPTED / IMPACT |
| `cop/alert/inbound/v1/*/*/*/us` — a launch at a **US** asset | **standing watch** then **IamdAgent** | the watch runs `handle_launch` the moment the alert is on the mesh (no model in the path): reads the inbound → publishes the ENGAGE recommendation → FLASH to the host nation (Kuwait ADC, Qatar JOC, … or CTF-153/UKMTO for a ship) → NOTAM + NAVWARN for the corridor; the agent, woken by the same alert, calls the same tool, gets the watch's result and writes the narrative | APPROVE / REJECT on the map's AGENT MESH panel; AUTO approves engagements as they arrive |
| `cop/alert/ship-threat/v1/>` (an inbound aimed at a **US** ship, or a hostile track inside 25 nm of one) | **standing watch** then **ShipDefenseAgent** | `handle_ship_threat`, run by the watch at once and narrated by the agent: geometry (true and relative bearing, range, closing speed, time to impact, stage) → OOD and Damage Control Central notified with the 1MC options | one card on the COP with three steps: **1 SHIP NOTIFIED** (OOD / DCC, plus UKMTO, CTF-153, NOTAM, NAVWARN) → **2 AWAITING SHIP CONFIRMATION** — the OOD sounds the 1MC on `ship.html?ship=DDG-51` (General Quarters, Collision alarm, brace for shock, battle stations with bearing and range, CIWS warning), or the watch officer presses CONFIRM FOR SHIP (`cop/c2/action/…/ship/{hull}/confirm_ready`) → **3 ENGAGE ON THE SHIP'S BEHALF** (the IAMD recommendation, approved after the ship confirms; ENGAGE NOW overrides) |
| `cop/air/*-event/…/impact` and `cop/alert/launch-site/v1/>` (every third salvo from one site) | **TargetingAgent** | `handle_launch_site` / `handle_impact`: launch history → ISR tasking (imagery / BDA) → target nomination (joint target list) | APPROVE / REJECT the nomination on the map |

Each event-driven agent has **one** workflow tool that runs the whole procedure (`mesh/src/cop_tools`):
a local 7B model makes one tool call reliably and four sequential calls unreliably — the first
run showed the IAMD agent narrating "flash message sent" without calling anything. The
fine-grained tools (`recommend_engagement`, `notify_host_nation`, …) still exist for the
orchestrator and the chat UI. Even one call is not guaranteed from a 3B model, and a ballistic
missile does not wait for a slow one, so the time-critical workflows (US launch, ship threat,
partner notification) are also run by a **standing watch** inside the agent host the instant the
alert arrives — plain code subscribed to the same topic. The agent's later tool call returns the
watch's result (`already_handled`), so nothing is published twice; the model only adds the
narrative. `docker logs cop-agent-mesh | grep "standing watch"` shows each one; the targeting
workflow (ISR, nomination) is not time-critical and stays with the model.

Random tempo is wrong for a demo, so launches are staged on cue — from the exercise control page
(http://localhost:3000/exercise.html: US or coalition target, drone wave / ballistic / cruise, the
target and the site) or `scripts/inject.py` (`ship-attack`, `coalition-attack`, `probe`, `missile`,
`uas`, `site-active`). Either way it is one MQTT message on
`cop/c2/action/v1/centcom/exercise/{scenario}/inject` that the generator acts on. It is on the
audit queue like every other action; fusion ignores it. `MISSILE_AUTO=1` on the generator restores
the random tempo.

The **CopWatchOfficerAgent** sits behind the chat UI (http://localhost:8010) and Agent Mesh
Desktop: ask "what is inbound and what have we done about it?" and `get_threat_picture` builds
a Zulu-stamped brief grouped by event and delivers it as the answer (see *Timing on a CPU*).
(It is not named `OrchestratorAgent` because Agent Mesh Desktop brings its own agent of that
name onto the same namespace; see below.)

Ideas that fit the same pattern and are one agent file away: an interceptor-inventory agent
(magazine depth per ship after each shot, rearm/reposition recommendation from the DLA
feed), a cyber-correlation agent (a cyber incident on an air-defence facility within minutes
of a launch at it is a coordinated attack), an airspace-deconfliction agent (CAP and
civil traffic in the corridor), a post-engagement reporting agent (OPREP-3 draft from the
decision records).

## What flows where

```
sensors ──launch──▶ broker ──(Event Mesh Gateway: subscription)──▶ A2A task ──▶ IamdAgent
                      ▲                                                            │ tools
                      │  cop/agent/recommendation  cop/notify/host-nation  cop/notify/airspace
                      └────────────────────────────────────────────────────────────┘
   map AGENT MESH panel: APPROVE ──▶ cop/c2/action/…/engage {recommendation_id} ──▶ generator fires interceptor
                                                                                ──▶ fusion: decision record + agent_recommendation
                                                                                ──▶ q.cop.audit, q.cop.decisions, SOAR
```

Topics the agents publish (all guaranteed, all under the `cop/` hierarchy so the existing
queues, ACLs and consoles see them):

| Topic | Meaning |
|---|---|
| `cop/agent/recommendation/v1/{aor}/{agent}/{target}/{action}` | needs a human: engage / weapons_free / warn_asset / track / nominate-target |
| `cop/agent/task/v1/{aor}/{agent}/{target}/{task}` | tasking for other systems (ISR imagery, BDA) |
| `cop/agent/report/v1/{aor}/{agent}/{target}/summary` | the agent's narrative, written by the gateway when the task completes |
| `cop/notify/{channel}/v1/{aor}/{recipient}/{subject}/{priority}` | host-nation, airspace, ship (OOD/DCC) notifications; the SOAR files them as tickets |
| `solace-agent-mesh/a2a/v1/…` | the A2A control plane (agent cards, requests, responses, status) under Agent Mesh's own root, which the Desktop shares — visible in the mesh console as a source |

Operator decisions on recommendations are ordinary actions: an approval is the engage itself
carrying `recommendation_id`; a rejection is `cop/c2/action/v1/{aor}/agent/{rec}/reject_recommendation`.
The viewer's ACL still only allows `cop/c2/action/v1/>`.

## Running it

Everything is in the compose stack under the `mesh` profile: an Ollama container for the
LLM (local, air-gapped once the model is pulled) and the Agent Mesh host built from
`mesh/Dockerfile` (the published `solace/solace-agent-mesh` image plus the Event Mesh
Gateway plugin and this project's configs and tools).

```bash
python3 broker/bootstrap.py                                  # once: agent-mesh username/ACL, queue subscriptions
cp mesh/.env.example mesh/.env                               # once; defaults work as they are
docker compose --profile mesh up -d --build                  # ollama + agent-mesh
docker exec solace-ollama ollama pull qwen2.5:3b             # once, needs internet; the models live in a volume after that
docker compose logs -f agent-mesh                            # agents register, gateway subscribes, tools connect to the broker
```

Then restart the alerting service (new `ship-threat` and `launch-site` rules) and the generator
(wave members now publish `launch-member`, so one launch event per wave reaches the agents).

Open:

| Where | What to watch |
|---|---|
| http://localhost:3000 | red **COALITION PARTNER EVENTS** board (partner notified ✓, agent's US options, TARGET FROM US / STANDBY / PARTNER ENGAGED / END) and the blue **AGENT MESH** panel for US assets (IAMD cards with APPROVE / REJECT, ship defense, targeting) |
| http://localhost:3000/ship.html?ship=DDG-51 | the ship's 1MC panel: threat geometry, the agent's OOD/DCC notification, the alarms and announcements (audio + voice) |
| http://localhost:3000/console.html | `solace-agent-mesh/a2a/` as a live source next to the sensors: agent-to-agent traffic on the same broker |
| http://localhost:8010 | Agent Mesh chat UI: ask the orchestrator; the Activity view shows the A2A task tree |
| http://localhost:3001 | SOAR: NOTIFY tickets to Kuwait ADC, CTF-153, NOTAM desk, the ship's watch |
| Broker Manager → Clients | `agent-mesh-tools` and the SAM clients; Queues: `q.cop.audit` and `q.cop.soar` taking `cop/agent/>` and `cop/notify/>` |

### Agent Mesh Desktop on the same mesh

[Solace Agent Mesh Desktop](https://docs.solace.com/Agent-Mesh/Framework/installing/desktop.htm) is
the single-installer bundle (runtime + in-memory broker + UI). Pointed at this demo's CONUS broker it
becomes the vendor's own window onto the mesh: its **Agents** page lists the demo's agents next to its
built-in ones, the **Activity** view shows the A2A task trees as launches come in, and its Orchestrator
delegates to IamdAgent, ShipDefenseAgent, TargetingAgent and CoalitionLiaisonAgent over the broker —
the same discovery topic (`solace-agent-mesh/a2a/v1/discovery/agentcards`) the Docker agents publish on.

1. Install the **Linux** build in the WSL2 Ubuntu the demo already runs from (the Windows build's broker
   mode is marked experimental and did not connect to an external broker in this environment as of 2.455; WSL2
   shows the window on the Windows desktop through WSLg). From https://solace.com/downloads/ → Agent Mesh → Desktop → `Linux (Experimental)/
   modern-distros/`, save the `.deb` somewhere WSL can see (a folder under `C:\`), then in the Ubuntu tab:
   ```bash
   sudo apt install /mnt/c/<folder>/solace-agent-mesh*.deb           # installs /usr/bin/solace-agent-mesh
   mkdir -p ~/.config/sam && cp mesh/desktop/desktop.env ~/.config/sam/.env
   python3 broker/bootstrap.py                                      # once: username agent-mesh-desktop, ACL root, connection limit
   solace-agent-mesh &
   ```
   First launch: provider **Ollama**, endpoint `http://localhost:11434`, model `qwen2.5:3b` for both General and
   Planning (already pulled into the compose stack's Ollama; the 7B is a cold 5 GB load per idle period).
2. The namespace. The Desktop's A2A root is fixed at `solace-agent-mesh/` (it is also the root of its
   control and configuration topics), so the Docker agents join that root instead of inventing one:
   `NAMESPACE=solace-agent-mesh/` in `mesh/.env`, and `acl-agent-mesh` publishes and subscribes
   `solace-agent-mesh/>` beside `cop/agent/>` and `cop/notify/>`. `mesh/desktop/desktop.env` holds only
   the five variables the Desktop documents (`SOLACE_DEV_MODE=false`, `tcp://localhost:55555`, VPN `cop`,
   user `agent-mesh-desktop`); giving it a `NAMESPACE` makes it fail on artifact URIs.
3. Agents page: the five demo agents appear within a few seconds (they publish their cards every few
   seconds). Chat with **Orchestrator**: "what is inbound and what has been recommended?" — it calls
   CopWatchOfficerAgent / the specialists; the mesh console shows the A2A traffic under `solace-agent-mesh/a2a/`.

The desktop is an evaluation bundle (no production broker settings, no auth); on the panel it is the
"this is Solace's own tooling, on the same broker, seeing the same agents" moment. The Docker host keeps
running the event-driven agents and the standing watch; the desktop is a second client of the mesh.

### Timing on a CPU

All agents, the COP Watch Officer included, run on `qwen2.5:3b` (about 20–30 s per task on the
laptop's CPU: one tool call, a four-line answer). Ollama serves one request
at a time (`OLLAMA_NUM_PARALLEL=1`), so one launch at a ship — which triggers the IAMD and the
ship-defense agents together — takes about a minute to clear; the LLM timeout is 300 s and
LiteLLM does not retry a timeout. A ballistic missile is in flight 40–80 s, so the IAMD
recommendation can land after the asset's own fire control has already shot; drone waves
(minutes) are the convincing case for the approval workflow. Options, in `mesh/.env`:

- `ollama_chat/qwen2.5:7b` (`docker exec solace-ollama ollama pull qwen2.5:7b`) — better prose, 2–3× slower and a cold load after every idle period
- a hosted model (`LLM_SERVICE_ENDPOINT`, `LLM_SERVICE_API_KEY`, any LiteLLM `<provider>/<model>` string) — 3–8 s per turn, no air gap

**What a question costs on the CPU, and the demo procedure that follows from it.** The model
reads a prompt at 40–60 tokens/s and writes at 12–25 tokens/s (`num_thread: 6` in
`shared_config.yaml`; with all 10 vCPUs every token waits for the slowest thread and the map's
browser tabs drag it to 2–5 tokens/s mid-wave). Three things keep the prompt and the answer
small: the agent configs turn off everything optional (`enable_embed_resolution`,
`auto_inject_artifact_tools`, `inject_*`, `allow_list: []`, `RUN_BASED` sessions, no
auto-continuation); `mesh/src/cop_tools/slim_prompt.py` removes the ~3,500 tokens of generic
instructions the runtime otherwise adds to every call (`SAM_SLIM_PROMPT=0` restores them);
and the watch officer's `get_threat_picture` builds a finished four-section Markdown brief
(PICTURE AS OF / INBOUND / AGENT RECOMMENDATIONS with the operator's decision / OPERATOR ACTIONS
/ NOTIFIED, every line Zulu-stamped) and hands it to the task as the final answer itself, ending
the turn with `skip_summarization` so there is no second model call to paraphrase, cut or
embellish it (`COP_BRIEF_DIRECT=0` makes the model copy it instead). The only model call is the
one that decides to call the tool, so a question with nothing in flight answers in about 10 s. Ollama serves one request at a time, so a question asked while a
wave is in the air queues behind the IAMD and ship-defense agents' calls for that wave (two LLM
calls each, 10–20 s a call) and returns in 1½–2 min. So, in the demo: ask the Desktop *before*
a wave or once the AGENT MESH panel shows the agents' reports for it, and ask one warm-up
question before the audience arrives (the first call after an idle period also loads the model,
~20 s).

### Without Docker (a venv)

Agent Mesh needs Python 3.10–3.13 (not the project's 3.14 venv):

```bash
python3.12 -m venv .venv-sam && source .venv-sam/bin/activate
pip install -r mesh/requirements.txt
cd mesh && cp .env.example .env
# point the broker at localhost from the host:
sed -i 's#ws://conus:8008#ws://localhost:8008#; s#tcp://conus:55555#tcp://localhost:55555#; s#http://ollama:11434#http://localhost:11434#' .env
sam run configs/
```

## Why this is the mesh, not an integration

- The agents never connected to a sensor. They subscribed to topics; the sensors do not know they exist.
- The same ACL design applies: `acl-agent-mesh` may publish `solace-agent-mesh/>`, `cop/agent/>` and `cop/notify/>`, never `cop/c2/action/>`. The broker, not the prompt, is what keeps an agent from firing.
- The audit queue and the decision records needed no change to include the agents: `cop/agent/>` is one more subscription.
- The A2A control plane is ordinary broker traffic. The mesh console shows it beside the radar tracks.
- Swap the LLM, the agents' instructions, or the trigger topics by editing a YAML file; restart one container.

Sources: [Solace Agent Mesh docs](https://docs.solace.com/Agent-Mesh/agent-mesh.htm) ·
[Configuring Agent Mesh (LLM providers incl. Ollama)](https://docs.solace.com/Agent-Mesh/Framework/installing/configure.htm) ·
[solace-agent-mesh on PyPI](https://pypi.org/project/solace-agent-mesh/) ·
[SolaceLabs/solace-agent-mesh](https://github.com/SolaceLabs/solace-agent-mesh) ·
[solace/solace-agent-mesh on Docker Hub](https://hub.docker.com/r/solace/solace-agent-mesh) ·
[Getting started codelab](https://codelabs.solace.dev/codelabs/solace-agent-mesh/)

---
© 2026 Ron McNeely. Provided for evaluation; no license to reproduce or redistribute the documentation.
