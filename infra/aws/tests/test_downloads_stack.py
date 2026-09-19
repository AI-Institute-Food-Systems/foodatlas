"""Snapshot tests for DownloadsStack."""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk.assertions import Match, Template

from stacks.downloads_stack import DownloadsStack


def _synth() -> Template:
    app = cdk.App()
    stack = DownloadsStack(app, "TestDownloadsStack")
    return Template.from_stack(stack)


def test_bucket_exists() -> None:
    template = _synth()
    template.resource_count_is("AWS::S3::Bucket", 1)


def test_bucket_is_not_versioned() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::S3::Bucket",
        Match.object_like({"VersioningConfiguration": Match.absent()}),
    )


def test_bucket_has_sse_encryption() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::S3::Bucket",
        Match.object_like(
            {
                "BucketEncryption": {
                    "ServerSideEncryptionConfiguration": Match.array_with(
                        [
                            Match.object_like(
                                {
                                    "ServerSideEncryptionByDefault": {
                                        "SSEAlgorithm": "AES256",
                                    },
                                },
                            ),
                        ],
                    ),
                },
            },
        ),
    )


def test_bucket_allows_public_policy() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::S3::Bucket",
        Match.object_like(
            {
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True,
                    "IgnorePublicAcls": True,
                    "BlockPublicPolicy": False,
                    "RestrictPublicBuckets": False,
                },
            },
        ),
    )


def _anonymous_get_object_resources(template: Template) -> list[str]:
    policies = template.find_resources("AWS::S3::BucketPolicy")
    (policy,) = policies.values()
    for stmt in policy["Properties"]["PolicyDocument"]["Statement"]:
        if stmt.get("Principal") == {"AWS": "*"} and stmt["Action"] == "s3:GetObject":
            # Each resource is an Fn::Join over the bucket ARN + a suffix.
            return [
                "".join(part for part in r["Fn::Join"][1] if isinstance(part, str))
                for r in stmt["Resource"]
            ]
    return []


def test_manifest_and_summaries_are_public() -> None:
    resources = _anonymous_get_object_resources(_synth())
    assert any(r.endswith("/bundles/index.json") for r in resources)
    assert any(r.endswith("/bundles/*/*.md") for r in resources)


def test_bundle_zips_are_not_public() -> None:
    # Downloads are gated like the API: no anonymous read on `*` or `*.zip`.
    resources = _anonymous_get_object_resources(_synth())
    assert not any(r.endswith("/*") for r in resources)
    assert not any(".zip" in r for r in resources)


def test_bucket_has_cors_for_browser_downloads() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::S3::Bucket",
        Match.object_like(
            {
                "CorsConfiguration": {
                    "CorsRules": Match.array_with(
                        [
                            Match.object_like(
                                {
                                    "AllowedMethods": Match.array_with(["GET"]),
                                    "AllowedOrigins": ["*"],
                                },
                            ),
                        ],
                    ),
                },
            },
        ),
    )
