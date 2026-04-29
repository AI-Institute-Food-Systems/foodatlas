"""Storage stack: S3 bucket for KGC source data and pipeline artifacts.

The bucket holds two top-level prefixes:

- ``data/<UTC-ts>/`` — versioned snapshots of the source ontologies and
  databases under ``backend/kgc/data/`` (excluding ``PreviousFAKG``, which
  is itself derived from a previous KGC run). Updated when source
  registries refresh.
- ``outputs/<UTC-ts>/`` — versioned snapshots of the full ``backend/kgc/
  outputs/`` tree, including the loadable ``kg/*.parquet`` files plus
  ``checkpoints/``, ``diagnostics/``, ``intermediate/`` and ``ingest/``.
  Updated per KGC pipeline run.

Each prefix has its own ``LATEST`` pointer file so downstream tooling can
resolve "current" without listing the bucket.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import aws_cdk as cdk
from aws_cdk import Duration
from aws_cdk import aws_s3 as s3

if TYPE_CHECKING:
    from constructs import Construct


def _build_kgc_bucket(scope: cdk.Stack, construct_id: str) -> s3.Bucket:
    return s3.Bucket(
        scope,
        construct_id,
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


class StorageStack(cdk.Stack):
    """S3 bucket for KGC source data and pipeline artifacts."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.kgc_bucket = _build_kgc_bucket(self, "KgcBucket")

        cdk.CfnOutput(
            self,
            "KgcBucketName",
            value=self.kgc_bucket.bucket_name,
            description="S3 bucket holding KGC source data and pipeline artifacts",
        )
