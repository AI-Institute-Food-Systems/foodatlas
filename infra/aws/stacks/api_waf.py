"""Regional WAF WebACL attached to the API's ALB.

``api.foodatlas.ai`` resolves straight to the ALB, so this is the only
filter in front of the API. Three rules, evaluated in order:

1. ``/v1/`` per-IP rate limit. The app already throttles each API key to
   60 req/min per task (``backend/api/src/rate_limit.py``), but it cannot
   throttle requests with an invalid or missing key. This rule does.
2. Per-IP rate limit for every other path. These routes serve the website:
   Vercel's server functions call them with the internal key, so one
   Vercel egress IP carries many visitors. The limit sits well above the
   busiest 5-minute window the ALB has seen and only stops floods.
3. AWS managed common rule set (bad inputs, LFI/RFI, XSS, oversized URIs).
   ``NoUserAgent_HEADER`` only counts: server-to-server clients may omit
   the header and the API has no reason to reject them.

Rate-limited requests get 429, the same status the app limiter returns.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from aws_cdk import aws_wafv2 as wafv2
from constructs import Construct

if TYPE_CHECKING:
    from aws_cdk import aws_elasticloadbalancingv2 as elbv2

RATE_WINDOW_SECONDS = 300
# Busiest /v1 client in 30 days: 2,059 req/5 min, all keyless 401s
# (2026-09-20). Keyed traffic is capped near 600/5 min by the app limiter.
PUBLIC_API_LIMIT_PER_IP = 1500
# Busiest 5-minute window on the whole ALB in 30 days: 16,308 requests, during
# the 2026-09-20 crawler incident. One IP above that is a flood by definition.
SITE_LIMIT_PER_IP = 20000
COUNT_ONLY_COMMON_RULES = ("NoUserAgent_HEADER",)


def _visibility(metric_name: str) -> wafv2.CfnWebACL.VisibilityConfigProperty:
    return wafv2.CfnWebACL.VisibilityConfigProperty(
        cloud_watch_metrics_enabled=True,
        metric_name=metric_name,
        sampled_requests_enabled=True,
    )


def _v1_path_match() -> wafv2.CfnWebACL.StatementProperty:
    return wafv2.CfnWebACL.StatementProperty(
        byte_match_statement=wafv2.CfnWebACL.ByteMatchStatementProperty(
            field_to_match=wafv2.CfnWebACL.FieldToMatchProperty(uri_path={}),
            positional_constraint="STARTS_WITH",
            search_string="/v1/",
            text_transformations=[
                wafv2.CfnWebACL.TextTransformationProperty(
                    priority=0, type="LOWERCASE"
                ),
            ],
        ),
    )


def _rate_rule(
    name: str,
    *,
    priority: int,
    limit: int,
    scope_down: wafv2.CfnWebACL.StatementProperty,
) -> wafv2.CfnWebACL.RuleProperty:
    return wafv2.CfnWebACL.RuleProperty(
        name=name,
        priority=priority,
        action=wafv2.CfnWebACL.RuleActionProperty(
            block=wafv2.CfnWebACL.BlockActionProperty(
                custom_response=wafv2.CfnWebACL.CustomResponseProperty(
                    response_code=429,
                ),
            ),
        ),
        statement=wafv2.CfnWebACL.StatementProperty(
            rate_based_statement=wafv2.CfnWebACL.RateBasedStatementProperty(
                aggregate_key_type="IP",
                limit=limit,
                evaluation_window_sec=RATE_WINDOW_SECONDS,
                scope_down_statement=scope_down,
            ),
        ),
        visibility_config=_visibility(name),
    )


def _common_rule_set(priority: int) -> wafv2.CfnWebACL.RuleProperty:
    name = "AWSManagedRulesCommonRuleSet"
    return wafv2.CfnWebACL.RuleProperty(
        name=name,
        priority=priority,
        override_action=wafv2.CfnWebACL.OverrideActionProperty(none={}),
        statement=wafv2.CfnWebACL.StatementProperty(
            managed_rule_group_statement=wafv2.CfnWebACL.ManagedRuleGroupStatementProperty(
                vendor_name="AWS",
                name=name,
                rule_action_overrides=[
                    wafv2.CfnWebACL.RuleActionOverrideProperty(
                        name=rule,
                        action_to_use=wafv2.CfnWebACL.RuleActionProperty(count={}),
                    )
                    for rule in COUNT_ONLY_COMMON_RULES
                ],
            ),
        ),
        visibility_config=_visibility(name),
    )


class ApiWaf(Construct):
    """WebACL with per-IP rate limits and the AWS common rule set."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        load_balancer: elbv2.IApplicationLoadBalancer,
    ) -> None:
        super().__init__(scope, construct_id)

        self.web_acl = wafv2.CfnWebACL(
            self,
            "WebAcl",
            scope="REGIONAL",
            default_action=wafv2.CfnWebACL.DefaultActionProperty(allow={}),
            visibility_config=_visibility("FoodAtlasApiWaf"),
            rules=[
                _rate_rule(
                    "RateLimitPublicApiPerIp",
                    priority=0,
                    limit=PUBLIC_API_LIMIT_PER_IP,
                    scope_down=_v1_path_match(),
                ),
                _rate_rule(
                    "RateLimitSitePerIp",
                    priority=1,
                    limit=SITE_LIMIT_PER_IP,
                    scope_down=wafv2.CfnWebACL.StatementProperty(
                        not_statement=wafv2.CfnWebACL.NotStatementProperty(
                            statement=_v1_path_match(),
                        ),
                    ),
                ),
                _common_rule_set(priority=2),
            ],
        )

        wafv2.CfnWebACLAssociation(
            self,
            "AlbAssociation",
            resource_arn=load_balancer.load_balancer_arn,
            web_acl_arn=self.web_acl.attr_arn,
        )
