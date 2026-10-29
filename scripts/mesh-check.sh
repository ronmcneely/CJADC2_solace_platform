#!/usr/bin/env bash
# Is the agent mesh healthy? Ollama up, model pulled, the agent host connected, the agents discovered.
set -u
echo "== ollama";        docker exec solace-ollama ollama list 2>&1 | sed 's/^/   /'
echo "== reachable from the agent host"; docker exec cop-agent-mesh python -c "import urllib.request,json;print('   ', [m['name'] for m in json.load(urllib.request.urlopen('http://ollama:11434/api/tags',timeout=5))['models']])" 2>&1 | tail -1
echo "== agent host";    docker logs cop-agent-mesh 2>&1 | grep -E "cop_tools\] (LLM|connected)|Added new agent via discovery|DataPlane.*(started|Failed)|Subscribed|ERROR" | tail -12 | sed 's/^/   /'
echo "== last LLM error (if any)"; docker logs cop-agent-mesh 2>&1 | grep -iE "APIConnectionError|litellm\.|ollama" | grep -v INFO | tail -5 | cut -c1-300 | sed 's/^/   /'
