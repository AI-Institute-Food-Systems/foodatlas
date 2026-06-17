"""Tool-driven Claude agent that scores one sentence's KG triples.

The agent is given the sentence and the model's final triples, plus tools to
inspect the paper (``search_paper`` / ``read_section``). It investigates as
needed — concentrations in particular often depend on units and measurement
basis stated elsewhere — then calls ``submit_verdict`` with the classification.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import anthropic

from .cost import Usage, usage_from_response
from .prompts import JUDGE_SYSTEM, JUDGE_USER, format_pairs
from .schema import Adjudication

if TYPE_CHECKING:
    from .papers import Paper

logger = logging.getLogger(__name__)

_MAX_TURNS = 12

_PAIR_SCHEMA = {
    "type": "object",
    "properties": {
        "food": {"type": "string"},
        "chemical": {"type": "string"},
        "concentration": {"type": "string"},
    },
    "required": ["food", "chemical", "concentration"],
    "additionalProperties": False,
}
_MATCH_SCHEMA = {
    "type": "object",
    "properties": {
        "food": {"type": "string"},
        "chemical": {"type": "string"},
        "conc_agree": {"type": "boolean"},
    },
    "required": ["food", "chemical", "conc_agree"],
    "additionalProperties": False,
}
_TOOLS = [
    {
        "name": "search_paper",
        "description": (
            "Find passages in the paper containing a phrase — a food, chemical, "
            "value, unit, or term like 'dry weight'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
        },
    },
    {
        "name": "read_section",
        "description": (
            "Read a section of the paper, e.g. ABSTRACT, METHODS, RESULTS, TABLE."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"section_type": {"type": "string"}},
            "required": ["section_type"],
        },
    },
    {
        "name": "submit_verdict",
        "description": "Submit the final classification of every model triple.",
        "input_schema": {
            "type": "object",
            "properties": {
                "matches": {"type": "array", "items": _MATCH_SCHEMA},
                "model_only": {"type": "array", "items": _PAIR_SCHEMA},
                "gold_only": {"type": "array", "items": _PAIR_SCHEMA},
            },
            "required": ["matches", "model_only", "gold_only"],
            "additionalProperties": False,
        },
    },
]


class Judge:
    """One Claude agent that adjudicates a sentence's extractions against its paper."""

    def __init__(self, model: str = "claude-opus-4-8", max_tokens: int = 12000) -> None:
        self._client = anthropic.Anthropic()
        self._model = model
        self._max_tokens = max_tokens

    def judge(
        self,
        paper: Paper,
        sentence: str,
        model_pairs: list[tuple[str, str, str]],
    ) -> tuple[Adjudication, Usage]:
        """Run the agent loop, returning (verdict, token usage).

        Raises if the agent never calls submit_verdict.
        """
        opening = self._opening(paper, sentence, model_pairs)
        messages: list[dict] = [{"role": "user", "content": opening}]
        usage = Usage()

        for turn in range(_MAX_TURNS):
            response = self._client.messages.create(
                model=self._model,
                max_tokens=self._max_tokens,
                thinking={"type": "adaptive"},
                system=JUDGE_SYSTEM,
                tools=_TOOLS,
                messages=messages,
            )
            usage += usage_from_response(response)
            messages.append({"role": "assistant", "content": response.content})

            tool_uses = [b for b in response.content if b.type == "tool_use"]
            if not tool_uses:
                messages.append({"role": "user", "content": "Call submit_verdict now."})
                continue

            nudge = turn >= _MAX_TURNS - 3
            results, verdict = self._handle_tools(tool_uses, paper, nudge=nudge)
            if verdict is not None:
                return verdict, usage
            messages.append({"role": "user", "content": results})

        msg = "Judge exhausted turns without calling submit_verdict"
        raise RuntimeError(msg)

    def _handle_tools(
        self,
        tool_uses: list,
        paper: Paper,
        *,
        nudge: bool,
    ) -> tuple[list[dict], Adjudication | None]:
        results: list[dict] = []
        verdict: Adjudication | None = None
        for tu in tool_uses:
            if tu.name == "submit_verdict":
                verdict = Adjudication.model_validate(tu.input)
                content = "Verdict recorded."
            elif tu.name == "search_paper":
                content = paper.search(str(tu.input.get("query", "")))
            elif tu.name == "read_section":
                content = paper.section(str(tu.input.get("section_type", "")))
            else:
                content = f"Unknown tool: {tu.name}"
            results.append(
                {"type": "tool_result", "tool_use_id": tu.id, "content": content}
            )
        if nudge and verdict is None:
            # A text block alongside the tool_result blocks in the same user turn.
            results.append(
                {
                    "type": "text",
                    "text": "You have investigated enough; call submit_verdict now.",
                }
            )
        return results, verdict

    @staticmethod
    def _opening(
        paper: Paper, sentence: str, model_pairs: list[tuple[str, str, str]]
    ) -> str:
        return JUDGE_USER.format(
            sentence=sentence,
            model_pairs=format_pairs(model_pairs),
            pmcid=paper.pmcid,
            title=paper.title,
            sections=", ".join(paper.sections()) or "(none)",
        )
