"""Versioned judge prompt for the extraction-quality evaluation.

A single agent is given a sentence and the model's final KG triples for it, plus
tools to inspect the paper (search passages, read sections). It investigates the
paper — especially to verify concentrations — then submits a verdict.

PROMPT_VERSION is stamped into the metrics output so scores are comparable
across runs. Bump it whenever a prompt changes.
"""

from __future__ import annotations

PROMPT_VERSION = "v1"

JUDGE_SYSTEM = """\
You evaluate an information-extraction model against a scientific paper, one
sentence at a time. You are given a sentence the model processed and the model's
final (food, chemical, concentration) triples for it — the triples that made it
into the knowledge graph. You have tools to inspect the paper; use them instead
of guessing.

Tools:
- search_paper(query): find passages mentioning a food, chemical, value, unit,
  or term (e.g. "dry weight", a table caption, the units of a measurement).
- read_section(section_type): read a section (e.g. ABSTRACT, METHODS, RESULTS,
  TABLE, TITLE) for context.

How to judge:
1. Decide what (food, chemical, concentration) facts the SENTENCE itself states.
   Use the tools to interpret it correctly — concentrations especially depend on
   context elsewhere in the paper: the units, the measurement basis (dry vs
   fresh weight), and which food/sample a value belongs to. The paper is for
   correct interpretation, NOT for importing facts from other sentences.
2. Align the model's triples to those facts by MEANING (synonyms are the same
   fact: "vitamin C" = "ascorbic acid", "tomatoes" = "tomato").

When done, call submit_verdict exactly once. Every model triple goes in exactly
one of:
- matches: the sentence states this fact. Set conc_agree = true iff the model's
  concentration agrees with the sentence's amount within an order of magnitude
  (convert units using the paper's basis) OR both are absent.
- model_only: the sentence does NOT support this triple (hallucination, wrong
  food/chemical, or concentration attributed to the wrong food).
- gold_only: a fact the sentence states that the model did NOT extract.\
"""

JUDGE_USER = """\
=== SENTENCE UNDER EVALUATION ===
{sentence}

=== MODEL EXTRACTIONS (final KG triples for this sentence) ===
{model_pairs}

=== PAPER ===
PMCID {pmcid}: {title}
Sections available: {sections}

Investigate the paper with the tools as needed (verify each concentration),
then call submit_verdict.\
"""


def format_pairs(pairs: list[tuple[str, str, str]]) -> str:
    """Render (food, chemical, concentration) triples as a numbered list."""
    if not pairs:
        return "(none)"
    lines = []
    for i, (food, chemical, conc) in enumerate(pairs, 1):
        conc_part = f" @ {conc}" if conc else ""
        lines.append(f"{i}. {food} -> {chemical}{conc_part}")
    return "\n".join(lines)
