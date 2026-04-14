"""Snapshot tests for EcrStack."""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk.assertions import Match, Template

from stacks.ecr_stack import EcrStack


def _synth() -> Template:
    app = cdk.App()
    stack = EcrStack(app, "TestEcrStack")
    return Template.from_stack(stack)


def test_two_repositories_exist() -> None:
    template = _synth()
    template.resource_count_is("AWS::ECR::Repository", 2)


def test_api_repository_named_foodatlas_api() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::ECR::Repository",
        Match.object_like({"RepositoryName": "foodatlas-api"}),
    )


def test_db_repository_named_foodatlas_db() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::ECR::Repository",
        Match.object_like({"RepositoryName": "foodatlas-db"}),
    )


def test_image_scan_on_push_enabled_on_all_repos() -> None:
    template = _synth()
    repos = template.find_resources("AWS::ECR::Repository")
    assert len(repos) == 2
    for repo in repos.values():
        assert repo["Properties"]["ImageScanningConfiguration"]["ScanOnPush"] is True


def test_lifecycle_rule_keeps_last_ten_images_on_all_repos() -> None:
    template = _synth()
    repos = template.find_resources("AWS::ECR::Repository")
    assert len(repos) == 2
    for repo in repos.values():
        policy_text = repo["Properties"]["LifecyclePolicy"]["LifecyclePolicyText"]
        assert '"countNumber":10' in policy_text
