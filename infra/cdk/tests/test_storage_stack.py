"""Snapshot tests for StorageStack."""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk.assertions import Match, Template

from stacks.storage_stack import StorageStack


def _synth() -> Template:
    app = cdk.App()
    stack = StorageStack(app, "TestStorageStack")
    return Template.from_stack(stack)


def test_bucket_exists() -> None:
    template = _synth()
    template.resource_count_is("AWS::S3::Bucket", 1)


def test_bucket_is_versioned() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::S3::Bucket",
        Match.object_like({"VersioningConfiguration": {"Status": "Enabled"}}),
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


def test_bucket_blocks_public_access() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::S3::Bucket",
        Match.object_like(
            {
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True,
                    "BlockPublicPolicy": True,
                    "IgnorePublicAcls": True,
                    "RestrictPublicBuckets": True,
                },
            },
        ),
    )


def test_bucket_has_lifecycle_rule_for_old_versions() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::S3::Bucket",
        Match.object_like(
            {
                "LifecycleConfiguration": {
                    "Rules": Match.array_with(
                        [
                            Match.object_like(
                                {"NoncurrentVersionExpiration": {"NoncurrentDays": 30}},
                            ),
                        ],
                    ),
                },
            },
        ),
    )
