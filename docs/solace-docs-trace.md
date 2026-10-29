# Traceability to Solace documentation

Every deployment and configuration step in this repo follows a published
Solace procedure. This table maps each step to the page it came from, so a
reviewer can check the work against the vendor's own instructions.

Starting point: [Get Started](https://docs.solace.com/Get-Started/get-started-lp.htm)
→ [Try Solace Event Brokers](https://docs.solace.com/Get-Started/Getting-Started-Try-Broker.htm)
→ [Next Steps](https://docs.solace.com/Get-Started/Next-steps.htm).

## 1. Deploy the brokers

| What we did | Where it's documented | Our file |
|---|---|---|
| `solace/solace-pubsub-standard` image, `--shm-size=1g`, ports 8080/55555/8008/1883/8000/5672/9000/2222, `username_admin_globalaccesslevel=admin`, `username_admin_password` | [Set Up Your First Event Broker](https://docs.solace.com/Get-Started/tutorial/event-broker-set-up.htm) | `docker-compose.yml` |
| `system_scaling_maxconnectioncount=1000` and a non-default admin password, per the tutorial's production note (1000 because DMR is refused at the 100-connection tier) | same page, "production" note | `docker-compose.yml`, `.env.example` |
| Log in to Broker Manager at `:8080` as `admin` | same page, "Open Broker Manager" | README |
| Run two brokers instead of one, each with its own `routername` | [Multi-Site Connectivity Configuration](https://docs.solace.com/Features/DMR/DMR-Examples-Multi-Site-Config.htm) (Broker-A / Broker-B pattern) | `docker-compose.yml` |

## 2. Verify messaging with the Try-Me CLI (tutorial steps 3–6)

| What we did | Where it's documented | Our file |
|---|---|---|
| Install `stm`, `stm config init`, `stm manage connection`, `stm manage semp-connection` | [Set Up the Solace Try-Me CLI Tool](https://docs.solace.com/Get-Started/tutorial/try-me-cli-tool.htm) | README "Verify with stm" |
| `stm receive --topic "cop/air/track/v1/*/>"` and `stm send --topic cop/air/...` | [Try a Publish-Subscribe Message Exchange Pattern](https://docs.solace.com/Get-Started/tutorial/pub-sub-pattern.htm) | README |
| Topic hierarchy shaped like the tutorial's `coffeeshop/order/details/v1/mobile/ca/437/44334`: domain / event / version / routing keys / ids | same page, plus [Best Practices](https://docs.solace.com/Get-Started/best-practices.htm) | `docs/topics.md` |
| Queues with non-exclusive access for consumer scaling | [Explore Consumer Scaling Patterns](https://docs.solace.com/Get-Started/tutorial/consumer-scaling-pattern.htm) | `broker/bootstrap.py` (`q.cop.logistics`) |

## 3. Build the event mesh with DMR

The multi-site example in the docs is written as CLI. We apply the same
objects through SEMP v2 so the configuration is code in the repo; the mapping
below shows the CLI line from the docs beside the SEMP object we create.

| Doc step (CLI from the multi-site example) | SEMP v2 object in `bootstrap.py` | Source page |
|---|---|---|
| `create cluster Cluster-A` → `authentication basic password …` → `no shutdown` | `POST /dmrClusters {dmrClusterName, nodeName, authenticationBasicEnabled, authenticationBasicPassword, enabled}` | [Configuring a DMR Cluster](https://docs.solace.com/Features/DMR/DMR-Mgmt-Cluster-Config.htm) |
| `create link Broker-B` → `span external` → `initiator local` → `connect-via <addr>` | `POST /dmrClusters/{c}/links {remoteNodeName, span:"external", initiator:"local"}` + `POST …/links/{n}/remoteAddresses {remoteAddress}` | [Configuring Cluster Links](https://docs.solace.com/Features/DMR/DMR-Mgmt-Cluster-Link-Config.htm) |
| On the peer: `create link Broker-A` → `span external` → `initiator remote` | same, `initiator:"remote"`, no remote address (only the initiating side needs `connect-via`) | same page, "Initiator rule" |
| `message-vpn red` → `dynamic-message-routing` → `no shutdown` | `MsgVpn.dmrEnabled: true` | [Configuring DMR on a Message VPN](https://docs.solace.com/Features/DMR/DMR-Mgmt-Msg-Vpn.htm) |
| `create dmr-bridge Broker-B` → `remote message-vpn red` | `POST /msgVpns/cop/dmrBridges {remoteNodeName, remoteMsgVpnName:"cop", enabled}` | same page |
| Verify: `show cluster * link *` → Admin and Operational both `Up` | `scripts/verify-dmr.sh` runs that command inside each container | [Multi-Site Connectivity Configuration](https://docs.solace.com/Features/DMR/DMR-Examples-Multi-Site-Config.htm), step 3 |
| Verify data channel: publish at one site, consume from a queue at the other | `scripts/smoke_mesh.py` | same page, step 5 |

Design notes taken from the docs:

- Each site is its own single-node cluster joined by an **external** link, which is the documented multi-site topology ([DMR Overview](https://docs.solace.com/Features/DMR/DMR-Overview.htm)). Internal links are for horizontal scaling inside one site.
- Cluster names must differ and only one external link is allowed between two clusters ([Configuring DMR External Links](https://docs.solace.com/Admin/Broker-Manager/DMR-external-links.htm)).
- Link transport defaults to plain on port 55555; TLS (`transport ssl`, port 55443) is the production setting and is called out on the architecture slide rather than enabled in the lab.

## 4. Message VPN, access control, queues, replay

| What we did | Where it's documented |
|---|---|
| A dedicated Message VPN `cop` as the messaging domain (and freeing MQTT/REST ports from `default`) | [Managing Message VPNs](https://docs.solace.com/Features/VPN/Managing-Message-VPNs.htm) |
| Client profile with guaranteed messaging and eliding enabled | [Features index](https://docs.solace.com/Get-Started/feature-index.htm) |
| ACL profiles with publish/subscribe topic exceptions per role | [Features index](https://docs.solace.com/Get-Started/feature-index.htm) → Access Control |
| Queues with topic-to-queue mappings, DMQ, max redelivery | [Features index](https://docs.solace.com/Get-Started/feature-index.htm) → Queues |
| Replay log | [Message Replay](https://docs.solace.com/Features/Message-Replay.htm) |
| Everything applied through SEMP v2 (REST management API) | [Configuring and Managing Event Brokers](https://docs.solace.com/Admin/config-manage-brokers-lp.htm) |
| The live configuration read back through SEMP v2 GET (`broker/export.py` → `broker/export/`) | [Using SEMP](https://docs.solace.com/Admin/SEMP/Using-SEMP.htm) |

## 5. Further reading used for the architecture slides

- [Event-Driven Integration basics](https://docs.solace.com/Get-Started/event-mesh-basics.htm)
- [Event Broker Redundancy (HA)](https://docs.solace.com/Features/HA-Redundancy/Redundancy-and-Fault-Tolerance-Overview.htm) and [Data Center Replication](https://docs.solace.com/Features/DR-Replication/Data-Center-Replication-Overview.htm): the production answer to "what about broker failure" on top of the DDIL story
- [Self-Managed Event Brokers](https://docs.solace.com/Admin/self-managed-lp.htm): the air-gapped deployment path
- [Micro-Integrations](https://docs.solace.com/Micro-Integrations/Micro-Integrations.htm)

## REST Delivery Point (outbound integration)

| Step in this repo | Solace documentation |
|---|---|
| `bootstrap.py` step 10: RDP `rdp-soar`, REST consumer `soar-intake` (host `cop-sink`, port 8088), queue binding `q.cop.soar` → `POST /alerts` | [Managing REST Delivery Points](https://docs.solace.com/Services/Managing-RDPs.htm) and [REST Consumers](https://docs.solace.com/Services/Managing-REST-Consumers.htm) |
| Created disabled, consumer and binding added, then enabled | the RDP page's order of operations: delivery point → consumer → queue binding |
| Queue `q.cop.soar` with `permission: consume` so the RDP's `#rdp/rdp-soar` client may bind | [Configuring Queues](https://docs.solace.com/Messaging/Guaranteed-Msg/Configuring-Queues.htm) |
| Verify: Broker Manager → Clients → REST Delivery Points → `rdp-soar` shows Up, consumer Up, binding Up | same page, "Monitoring RDPs" |

## Client names

Every SMF client sets `client_properties.NAME` (`sim-multidomain-smf`, `feed-coalition-c2-smf`, `cop-alerting`, `cop-fusion`) and every MQTT client a fixed client ID, so the Clients page reads like an integration list rather than a list of generated names. [Client names](https://docs.solace.com/Messaging/Client-Names.htm).

## Agent Mesh

| Step in this repo | Solace documentation |
|---|---|
| `mesh/configs/shared_config.yaml`: broker connection (`ws://conus:8008`, VPN `cop`, username `agent-mesh`), LLM as `ollama_chat/…` with an endpoint URL | [Configuring Agent Mesh — LLM providers (Ollama requires an endpoint URL) and broker settings](https://docs.solace.com/Agent-Mesh/Framework/installing/configure.htm) |
| Agents as YAML (`agent_name`, `instruction`, `tools` of `tool_type: python`, `agent_card` with skills, discovery enabled) | [solace-agent-mesh on PyPI](https://pypi.org/project/solace-agent-mesh/) and the [SolaceLabs/solace-agent-mesh](https://github.com/SolaceLabs/solace-agent-mesh) templates (`sam add agent`) |
| `mesh/configs/gateways/cop_events.yaml`: Event Mesh Gateway `event_handlers` (topic subscriptions → `input_expression` → `target_agent_name`) and `output_handlers` (`topic_expression`, `payload_expression`) | the `sam-event-mesh-gateway` plugin's user guide (installed with the package; README in the wheel) |
| `mesh/Dockerfile`: `FROM solace/solace-agent-mesh`, `solace-agent-mesh run configs/` | [solace/solace-agent-mesh on Docker Hub](https://hub.docker.com/r/solace/solace-agent-mesh) |
| A2A topics generated per agent: `{namespace}a2a/v1/agent/request/{Agent}`, `…/discovery/agentcards`, `…/agent/response/{Agent}/>` — seen in the mesh console under `cop/mesh/a2a/` | logged by the agent host at startup ("Automatically generated subscriptions for Agent …") |
| ACL `acl-agent-mesh`: publish `solace-agent-mesh/>`, `cop/agent/>`, `cop/notify/>`; subscribe `cop/>`, `solace-agent-mesh/>` | [ACL Profiles](https://docs.solace.com/Security/Granting-Clients-Access.htm) — the broker, not the prompt, keeps an agent off `cop/c2/action/>` |

---
© 2026 Ron McNeely. Provided for evaluation; no license to reproduce or redistribute the documentation.
