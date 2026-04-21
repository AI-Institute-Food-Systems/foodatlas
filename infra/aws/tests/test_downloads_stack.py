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


def test_bucket_has_public_read_policy() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::S3::BucketPolicy",
        Match.object_like(
            {
                "PolicyDocument": Match.object_like(
                    {
                        "Statement": Match.array_with(
                            [
                                Match.object_like(
                                    {
                                        "Effect": "Allow",
                                        "Principal": {"AWS": "*"},
                                        "Action": "s3:GetObject",
                                    },
                                ),
                            ],
                        ),
                    },
                ),
            },
        ),
    )


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
