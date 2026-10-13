"""Snapshot tests for AuditStack."""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk.assertions import Match, Template

from stacks.audit_stack import LOG_RETENTION_DAYS, AuditStack


def _synth() -> Template:
    app = cdk.App()
    stack = AuditStack(app, "TestAuditStack")
    return Template.from_stack(stack)


def test_one_trail_exists() -> None:
    template = _synth()
    template.resource_count_is("AWS::CloudTrail::Trail", 1)


def test_trail_is_multi_region_with_global_events() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::CloudTrail::Trail",
        Match.object_like(
            {
                "IsMultiRegionTrail": True,
                "IncludeGlobalServiceEvents": True,
                "IsLogging": True,
            }
        ),
    )


def test_trail_has_log_file_validation() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::CloudTrail::Trail",
        Match.object_like({"EnableLogFileValidation": True}),
    )


def test_trail_records_all_management_events() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::CloudTrail::Trail",
        Match.object_like(
            {
                "EventSelectors": [
                    Match.object_like(
                        {"IncludeManagementEvents": True, "ReadWriteType": "All"}
                    )
                ]
            }
        ),
    )


def test_bucket_is_private_and_encrypted() -> None:
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
                "BucketEncryption": {
                    "ServerSideEncryptionConfiguration": [
                        {"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
                    ]
                },
            }
        ),
    )


def test_bucket_is_retained_on_stack_delete() -> None:
    template = _synth()
    template.has_resource(
        "AWS::S3::Bucket",
        {"DeletionPolicy": "Retain", "UpdateReplacePolicy": "Retain"},
    )


def test_bucket_expires_logs_after_retention_window() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::S3::Bucket",
        Match.object_like(
            {
                "LifecycleConfiguration": {
                    "Rules": Match.array_with(
                        [Match.object_like({"ExpirationInDays": LOG_RETENTION_DAYS})]
                    )
                }
            }
        ),
    )


def test_bucket_policy_lets_cloudtrail_write() -> None:
    template = _synth()
    policies = template.find_resources("AWS::S3::BucketPolicy")
    assert len(policies) == 1
    statements = next(iter(policies.values()))["Properties"]["PolicyDocument"][
        "Statement"
    ]
    actions = {
        s["Action"]
        for s in statements
        if s.get("Principal", {}).get("Service") == "cloudtrail.amazonaws.com"
    }
    assert {"s3:GetBucketAcl", "s3:PutObject"} <= actions
