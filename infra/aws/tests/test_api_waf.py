"""Snapshot tests for the WAF WebACL on the API's ALB."""

from __future__ import annotations

from typing import Any

import aws_cdk as cdk
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_elasticloadbalancingv2 as elbv2
from aws_cdk.assertions import Match, Template

from stacks.api_waf import (
    COUNT_ONLY_COMMON_RULES,
    PUBLIC_API_LIMIT_PER_IP,
    SITE_LIMIT_PER_IP,
    ApiWaf,
)


def _synth() -> Template:
    app = cdk.App()
    stack = cdk.Stack(app, "TestWafStack")
    vpc = ec2.Vpc(stack, "Vpc", max_azs=2, nat_gateways=0)
    alb = elbv2.ApplicationLoadBalancer(stack, "Alb", vpc=vpc, internet_facing=True)
    ApiWaf(stack, "ApiWaf", load_balancer=alb)
    return Template.from_stack(stack)


def _rules() -> dict[str, dict[str, Any]]:
    acls = _synth().find_resources("AWS::WAFv2::WebACL")
    assert len(acls) == 1
    rules = next(iter(acls.values()))["Properties"]["Rules"]
    return {r["Name"]: r for r in rules}


def test_web_acl_is_regional_and_allows_by_default() -> None:
    _synth().has_resource_properties(
        "AWS::WAFv2::WebACL",
        Match.object_like({"Scope": "REGIONAL", "DefaultAction": {"Allow": {}}}),
    )


def test_web_acl_is_associated_with_the_alb() -> None:
    template = _synth()
    template.resource_count_is("AWS::WAFv2::WebACLAssociation", 1)
    alb_id = next(
        iter(template.find_resources("AWS::ElasticLoadBalancingV2::LoadBalancer"))
    )
    template.has_resource_properties(
        "AWS::WAFv2::WebACLAssociation",
        {"ResourceArn": {"Ref": alb_id}, "WebACLArn": Match.any_value()},
    )


def test_rule_order() -> None:
    rules = _rules()
    order = sorted(rules, key=lambda name: rules[name]["Priority"])
    assert order == [
        "RateLimitPublicApiPerIp",
        "RateLimitSitePerIp",
        "AWSManagedRulesCommonRuleSet",
    ]


def test_public_api_rate_rule_is_per_ip_on_v1_paths() -> None:
    rule = _rules()["RateLimitPublicApiPerIp"]
    rate = rule["Statement"]["RateBasedStatement"]
    assert rate["AggregateKeyType"] == "IP"
    assert rate["Limit"] == PUBLIC_API_LIMIT_PER_IP
    assert rate["EvaluationWindowSec"] == 300
    match = rate["ScopeDownStatement"]["ByteMatchStatement"]
    assert match["SearchString"] == "/v1/"
    assert match["PositionalConstraint"] == "STARTS_WITH"


def test_site_rate_rule_covers_everything_except_v1() -> None:
    rule = _rules()["RateLimitSitePerIp"]
    rate = rule["Statement"]["RateBasedStatement"]
    assert rate["Limit"] == SITE_LIMIT_PER_IP
    negated = rate["ScopeDownStatement"]["NotStatement"]["Statement"]
    assert negated["ByteMatchStatement"]["SearchString"] == "/v1/"


def test_rate_rules_answer_429() -> None:
    rules = _rules()
    for name in ("RateLimitPublicApiPerIp", "RateLimitSitePerIp"):
        block = rules[name]["Action"]["Block"]
        assert block["CustomResponse"]["ResponseCode"] == 429


def test_site_limit_is_above_public_api_limit() -> None:
    assert SITE_LIMIT_PER_IP > PUBLIC_API_LIMIT_PER_IP


def test_common_rule_set_blocks_except_count_only_rules() -> None:
    rule = _rules()["AWSManagedRulesCommonRuleSet"]
    assert rule["OverrideAction"] == {"None": {}}
    group = rule["Statement"]["ManagedRuleGroupStatement"]
    assert group["VendorName"] == "AWS"
    overrides = {o["Name"]: o["ActionToUse"] for o in group["RuleActionOverrides"]}
    assert overrides == {name: {"Count": {}} for name in COUNT_ONLY_COMMON_RULES}


def test_every_rule_emits_metrics_and_samples() -> None:
    for rule in _rules().values():
        visibility = rule["VisibilityConfig"]
        assert visibility["CloudWatchMetricsEnabled"] is True
        assert visibility["SampledRequestsEnabled"] is True
