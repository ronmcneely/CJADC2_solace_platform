# Topic design

```
cop/{domain}/{event}/v1/{aor}/{source|iff}/{entity}/{status}
```

Same shape as the Solace tutorial's `coffeeshop/order/details/v1/mobile/ca/437/44334`:
what happened on the left, routing keys in the middle, identifiers on the right.

| Level | Values | Why it has its own level |
|---|---|---|
| domain | air · sea · land · cyber · sigint · space · ew · intel · c2 · alert · fused · decision | one per source system; the first thing a consumer filters on |
| event | track · site · incident · intercept · image · jamming · missile · uas · interceptor · action · recommendation · task · report · (alert type) · (notify channel) | event type within the domain |
| v1 | schema version | v2 can run beside v1 while consumers migrate |
| aor | centcom · eucom · africom · indopacom · global | geographic routing; ACLs key on it (`global` for non-geographic intel such as IOCs) |
| source / iff | `friend` / `hostile` for tracks; collector, jammer or facility id for reports | provenance, and IFF filtering without parsing the payload |
| entity | tail, hull, unit, incident or track id | follow one thing |
| status | active · patrol · rtb · critical · high · … | filter on state at the broker |

Rule: most stable on the left, most specific on the right. Anything someone
filters on gets its own level. Eight levels, well under the 250-byte limit.

## Every stream

| Source system | Topic | Delivery |
|---|---|---|
| Aircraft tracker | `cop/air/track/v1/{aor}/{friend\|hostile}/{tail}/{status}` | direct |
| Naval tracker | `cop/sea/track/v1/{aor}/{friend\|hostile}/{hull}/{status}` | direct |
| Ground-force GPS | `cop/land/track/v1/{aor}/friend/{unit}/{status}` | direct, **also over MQTT** |
| Facilities | `cop/land/site/v1/{aor}/friend/{facility}/operational` | direct |
| EMBM emitter tracks | `cop/ew/track/v1/{aor}/{country}/{track}/{threat}` | direct |
| Missile warning | `cop/air/missile/v1/{aor}/hostile/{track}/{phase}` in flight; `cop/air/missile-event/v1/{aor}/hostile/{track}/{launch\|terminal\|intercepted\|impact}` | track direct; events guaranteed |
| C-UAS sensors | `cop/air/uas/v1/{aor}/hostile/{track}/{phase}`; `cop/air/uas-event/v1/…/{launch\|terminal\|intercepted\|impact}` | track direct; events guaranteed |
| IAMD fire control | `cop/air/interceptor/v1/{aor}/friend/{interceptor}/{standby\|launch\|closing\|hit\|miss}`; `cop/air/interceptor-event/v1/…/{launch\|hit\|miss}` | track direct; events guaranteed |
| Agent recommendations | `cop/agent/recommendation/v1/{aor}/{agent}/{target}/{action}` — needs a human on the COP | guaranteed |
| Agent tasking / reports | `cop/agent/task/v1/{aor}/{agent}/{target}/{task}`; `cop/agent/report/v1/{aor}/{agent}/{target}/summary` | guaranteed |
| Notifications | `cop/notify/{host-nation\|airspace\|ship}/v1/{aor}/{recipient}/{subject}/{priority}` | guaranteed |
| Agent Mesh control plane | `solace-agent-mesh/a2a/v1/…` (agent cards, requests, responses, status) — Agent Mesh's own root, shared with the Desktop | per SAM |
| Cyber incidents | `cop/cyber/incident/v1/{aor}/{target}/{incident}/{severity}` | guaranteed |
| SIGINT intercepts | `cop/sigint/intercept/v1/{aor}/{collector}/{intercept}/{threat}` | guaranteed |
| Satellite imagery | `cop/space/image/v1/{aor}/{sensor}/{image}/{type}` | guaranteed |
| JSIR jamming | `cop/ew/jamming/v1/{aor}/{jammer}/{incident}/{severity}` | guaranteed |
| DIA, CENTCOM, DLA (REST) | `cop/intel/{ioc\|report\|actor\|assessment\|readiness\|equipment\|supply\|shipment}/v1/{aor}/{agency}/{id}/{status}` | guaranteed |
| MDA (maritime), NGA (MQTT) | `cop/intel/{port\|voi\|geoint\|restriction}/v1/{aor}/{agency}/{id}/{status}`; AIS as `cop/sea/ais/v1/{aor}/mda/{mmsi}/{flag}` | guaranteed; AIS direct |
| Coalition C2 (SMF) | `cop/intel/{coalition\|coalition-air\|coalition-naval}/v1/{aor}/{nation}/{id}/{status}` | reports guaranteed; tracks direct |
| FAA ADS-B (MQTT) | `cop/air/adsb/v1/{aor}/faa/{hex}/{airline}` | direct |
| Operator actions (from the map) | `cop/c2/action/v1/{aor}/{domain}/{target}/{action}` | guaranteed |
| Alerting subscriber | `cop/alert/{type}/v1/{aor}/{severity}/{entity}/{state}` | guaranteed |
| Fusion service | `cop/fused/{air\|sea\|land\|cyber\|contact}/v1/{aor}/{iff\|facility\|voi}/{entity}/{status}` | tracks direct; cyber and first VOI sighting guaranteed |
| Decision records | `cop/decision/record/v1/{aor}/{domain}/{target}/{action}` — the action plus the full target record, every enrichment, the alerts and the timeline | guaranteed → `q.cop.decisions` (downstream), `q.cop.audit`, `q.cop.soar` |

## Subscriptions the demo shows

| Consumer | Subscription | What it proves |
|---|---|---|
| Full COP map | `cop/>` | one wildcard replaces every connector |
| Unified track feed | `cop/*/track/v1/>` | no "unified GPS stream" needed; the hierarchy does it |
| CENTCOM cell | `cop/*/*/v1/centcom/>` | AOR filtering at the broker |
| Hostile-only overlay | `cop/*/track/v1/*/hostile/>` | IFF filtering at the broker |
| Follow one ship | `cop/sea/track/v1/*/*/cvn-72/*` | entity filtering |
| Critical anything | `cop/*/*/v1/*/*/*/critical` | status filtering across every domain |
| Actions queue | `cop/c2/action/v1/>` (topic-to-queue) | guaranteed, ordered, one consumer |
| Audit queue | every guaranteed stream | every decision and report, once, for the record |
| Fusion service | `cop/*/track/v1/>`, `cop/sea/ais/v1/>`, `cop/intel/coalition-*/v1/>` + queue `q.cop.fusion` | joins at a subscriber, published back as events |
| SOAR via RDP | `cop/alert/>` → `q.cop.soar` → HTTP POST | an integration that lives in the broker |

## QoS by event type

| Events | Delivery | Why |
|---|---|---|
| air/sea/land/ew tracks, sites | **Direct**, eliding on | high rate; the next value supersedes the last; no disk |
| incidents, intercepts, imagery, jamming, actions, alerts, agency intel | **Guaranteed** | must survive link loss and consumer restarts |

Topic-to-queue mappings only attract the guaranteed streams, so the spool
never fills with track updates.

## ACLs ride the hierarchy

| Profile | Publish | Subscribe |
|---|---|---|
| acl-source-system | `cop/>` | `cop/c2/>`, `cop/intel/coalition-naval/>` |
| acl-cop-viewer | `cop/c2/action/v1/>` | `cop/>` |
| acl-alerting | `cop/alert/>` | `cop/>` |
| acl-fusion | `cop/fused/>`, `cop/decision/>` | `cop/>` |
| acl-agent-mesh | `solace-agent-mesh/>`, `cop/agent/>`, `cop/notify/>` — never `cop/c2/action/>`: an agent cannot fire | `cop/>`, `solace-agent-mesh/>` |
| acl-centcom-cell | `cop/*/*/v1/centcom/>` | `cop/*/*/v1/centcom/>`, `cop/c2/>` |

The CENTCOM cell physically cannot publish into another AOR, and nothing at
the application layer has to enforce it.

---
© 2026 Ron McNeely. Provided for evaluation; no license to reproduce or redistribute the documentation.
