"""Storage stack: S3 bucket for KGC parquet outputs.

The bucket holds the output of the local KGC pipeline (entities.parquet,
triplets.parquet, evidence.parquet, attestations.parquet, etc.). The monthly
pipeline script syncs its outputs directory here, and the ECS task reads
from this bucket when running the DB load step.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import aws_cdk as cdk
from aws_cdk import Duration
from aws_cdk import aws_s3 as s3

if TYPE_CHECKING:
    from constructs import Construct


class StorageStack(cdk.Stack):
    """S3 bucket for pipeline artifacts."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs: object,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.parquet_bucket = s3.Bucket(
            self,
            "ParquetBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="expire-old-versions",
                    noncurrent_version_expiration=Duration.days(30),
                    abort_incomplete_multipart_upload_after=Duration.days(7),
                ),
            ],
        )

        cdk.CfnOutput(
            self,
            "ParquetBucketName",
            value=self.parquet_bucket.bucket_name,
            description="S3 bucket holding KGC parquet outputs",
        )
