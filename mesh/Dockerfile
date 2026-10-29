# Solace Agent Mesh host for the COP: the published image plus the Event Mesh Gateway plugin and
# this project's agents, gateways and tools. Runs every YAML under configs/ in one process.
FROM solace/solace-agent-mesh:latest
USER root
RUN pip install --no-cache-dir sam-event-mesh-gateway==1.1.2 && mkdir -p /mesh/data && chown -R solaceai:solaceai /mesh
WORKDIR /mesh
COPY --chown=solaceai:solaceai configs ./configs
COPY --chown=solaceai:solaceai src ./src
USER solaceai
ENV NAMESPACE=cop/mesh/ SOLACE_DEV_MODE=false PYTHONUNBUFFERED=1
ENTRYPOINT ["solace-agent-mesh"]
CMD ["run", "configs/"]
