"""Trim the Agent Mesh system prompt for a small local model.

The Agent Mesh runtime (solace-agent-mesh 1.28) adds roughly 3,500 tokens of generic
instructions to EVERY LLM call, regardless of configuration: parallel-tool planning
guidance, the ``«««save_artifact``/``template_liquid`` fenced-block syntax, the
fenced-block rules and five worked examples. On a hosted model that is free; on a
3B model running on a laptop CPU at ~40 tokens/s of prompt evaluation it is about
90 seconds per call, several times per question, serialized behind every other
agent's call on the same Ollama.

Nothing in this demo uses artifacts, embeds or Liquid templates (every agent sets
``enable_embed_resolution: false``, ``artifact_handling_mode: ignore`` and
``auto_inject_artifact_tools: false``), so those blocks are dead weight. This
module wraps the runtime's ``inject_dynamic_instructions_callback`` and removes
only those generic blocks, keeping everything else the runtime adds (the agent's
own instruction, peer-agent and built-in tool instructions, workflow guidance).

Enabled by default; set ``SAM_SLIM_PROMPT=0`` to run the stock prompt.
Imported for its side effect from ``cop_tools/__init__.py``, which every agent
config loads before the ADK agent is built.
"""

import logging
import os

log = logging.getLogger(__name__)

# First characters of each generic block, as the runtime emits them (1.28.x).
_GENERIC_PREFIXES = (
    "Parallel Tool Calling:",
    "**Your LLM Identity:**",
    "**Creating Text-Based Artifacts",
    "**Inline Liquid Templates",
    "**Fenced Block Syntax Rules",
    "Example 1:",
)
_SEPARATOR = "\n\n---\n\n"
_REPLACEMENT = (
    "Reply in plain text only: no fenced blocks, embeds, artifacts or templates."
)


def _is_generic(block: str) -> bool:
    head = block.strip()
    return any(head.startswith(p) for p in _GENERIC_PREFIXES)


def install() -> None:
    if os.environ.get("SAM_SLIM_PROMPT", "1") in ("0", "false", "no"):
        log.info("slim_prompt disabled by SAM_SLIM_PROMPT=0")
        return
    try:
        from solace_agent_mesh.agent.adk import callbacks as cb
    except Exception as e:  # pragma: no cover - runtime not present (unit tests)
        log.warning("slim_prompt: Agent Mesh runtime not importable (%s)", e)
        return

    original = cb.inject_dynamic_instructions_callback
    if getattr(original, "_cop_slim", False):
        return

    def slim_inject(callback_context, llm_request, host_component, active_builtin_tools):
        before = ""
        if llm_request.config is not None and llm_request.config.system_instruction:
            before = llm_request.config.system_instruction
        result = original(
            callback_context, llm_request, host_component, active_builtin_tools
        )
        try:
            cfg = llm_request.config
            if cfg is None or not cfg.system_instruction:
                return result
            after = cfg.system_instruction
            added = after[len(before):] if after.startswith(before) else after
            kept = [b for b in added.split(_SEPARATOR) if b.strip() and not _is_generic(b)]
            kept.append(_REPLACEMENT)
            cfg.system_instruction = (before.rstrip() + "\n\n" + _SEPARATOR.join(kept)).strip()
            log.debug(
                "slim_prompt: system prompt %d -> %d chars",
                len(after), len(cfg.system_instruction),
            )
        except Exception as e:  # never break the call over prompt trimming
            log.warning("slim_prompt: left prompt untouched (%s)", e)
        return result

    slim_inject._cop_slim = True
    cb.inject_dynamic_instructions_callback = slim_inject
    log.info("slim_prompt: generic Agent Mesh prompt blocks will be removed (SAM_SLIM_PROMPT=0 to disable)")


install()
