"""The committed cdk.json context alone must give prod an HTTPS API."""

from __future__ import annotations

import json
from pathlib import Path

from tests.test_api_stack import _synth

_CONTEXT = json.loads((Path(__file__).parents[1] / "cdk.json").read_text())["context"]


def _listener_protocols(name_suffix: str) -> set[str]:
    template = _synth(context=_CONTEXT, name_suffix=name_suffix)
    listeners = template.find_resources("AWS::ElasticLoadBalancingV2::Listener")
    return {r["Properties"]["Protocol"] for r in listeners.values()}


def test_prod_api_is_https_from_a_clean_checkout() -> None:
    assert "HTTPS" in _listener_protocols("")


def test_staging_does_not_inherit_the_prod_cert() -> None:
    assert _listener_protocols("-staging") == {"HTTP"}
