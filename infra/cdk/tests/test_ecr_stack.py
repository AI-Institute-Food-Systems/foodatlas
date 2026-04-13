"""Snapshot tests for EcrStack."""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk.assertions import Match, Template

from stacks.ecr_stack import EcrStack


def _synth() -> Template:
    app = cdk.App()
    stack = EcrStack(app, "TestEcrStack")
    return Template.from_stack(stack)


def test_repository_exists_with_expected_name() -> None:
    template = _synth()
    template.resource_count_is("AWS::ECR::Repository", 1)
    template.has_resource_properties(
        "AWS::ECR::Repository",
        Match.object_like({"RepositoryName": "foodatlas-api"}),
    )


def test_image_scan_on_push_enabled() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::ECR::Repository",
        Match.object_like({"ImageScanningConfiguration": {"ScanOnPush": True}}),
    )


def test_lifecycle_rule_keeps_last_ten_images() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::ECR::Repository",
        Match.object_like(
            {
                "LifecyclePolicy": Match.object_like(
                    {
                        "LifecyclePolicyText": Match.string_like_regexp(
                            r".*\"countNumber\":\s*10.*",
                        ),
                    },
                ),
            },
        ),
    )
