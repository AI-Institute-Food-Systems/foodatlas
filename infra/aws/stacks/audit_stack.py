"""Audit stack: account-wide CloudTrail trail for management events.

One multi-region trail records every management API call in the account
(console sign-ins, IAM changes, stack deploys, secret reads) to a private,
encrypted S3 bucket. Log-file validation is on, so ``aws cloudtrail
validate-logs`` can prove that no delivered file was changed or deleted.

Data events (S3 object reads, Lambda invokes) are off: they are billed per
event and the account has no use for them yet. The first copy of
management events is free; the only cost is S3 storage.

The bucket is retained when the stack is deleted, so the audit record
outlives the infrastructure it describes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import aws_cdk as cdk
from aws_cdk import Duration
from aws_cdk import aws_cloudtrail as cloudtrail
from aws_cdk import aws_s3 as s3

if TYPE_CHECKING:
    from constructs import Construct

LOG_TRANSITION_DAYS = 90
LOG_RETENTION_DAYS = 730


def _build_trail_bucket(scope: cdk.Stack, construct_id: str) -> s3.Bucket:
    return s3.Bucket(
        scope,
        construct_id,
        encryption=s3.BucketEncryption.S3_MANAGED,
        versioned=True,
        block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        enforce_ssl=True,
        object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
        removal_policy=cdk.RemovalPolicy.RETAIN,
        lifecycle_rules=[
            s3.LifecycleRule(
                id="archive-then-expire",
                transitions=[
                    s3.Transition(
                        storage_class=s3.StorageClass.GLACIER_INSTANT_RETRIEVAL,
                        transition_after=Duration.days(LOG_TRANSITION_DAYS),
                    ),
                ],
                expiration=Duration.days(LOG_RETENTION_DAYS),
                noncurrent_version_expiration=Duration.days(30),
                abort_incomplete_multipart_upload_after=Duration.days(7),
            ),
        ],
    )


class AuditStack(cdk.Stack):
    """Multi-region CloudTrail trail writing to a private S3 bucket."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.trail_bucket = _build_trail_bucket(self, "TrailBucket")

        self.trail = cloudtrail.Trail(
            self,
            "AccountTrail",
            bucket=self.trail_bucket,
            is_multi_region_trail=True,
            include_global_service_events=True,
            enable_file_validation=True,
            management_events=cloudtrail.ReadWriteType.ALL,
        )

        cdk.CfnOutput(
            self,
            "TrailBucketName",
            value=self.trail_bucket.bucket_name,
            description="S3 bucket holding CloudTrail management-event logs",
        )
        cdk.CfnOutput(
            self,
            "TrailArn",
            value=self.trail.trail_arn,
            description="ARN of the account-wide CloudTrail trail",
        )
