"""Downloads stack: public-read S3 bucket for released data bundles.

Holds two things:

- ``bundles/<version>/<filename>`` — released FoodAtlas data bundles
  (e.g. parquet zips). Uploaded via ``backend/kgc/scripts/publish-bundle.sh``.
- ``bundles/index.json`` — manifest describing all released bundles
  (version, release date, changelog, file size, download link). The API
  reads this manifest over HTTPS to populate the downloads page.

Unlike :class:`stacks.storage_stack.StorageStack` (private KGC
artifacts), this bucket grants anonymous ``s3:GetObject`` so that
anyone can fetch a bundle directly via its public S3 URL. Listing is
not granted — callers must know the key (they learn it from the
manifest).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import aws_cdk as cdk
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3

if TYPE_CHECKING:
    from constructs import Construct


def _build_downloads_bucket(scope: cdk.Stack, construct_id: str) -> s3.Bucket:
    bucket = s3.Bucket(
        scope,
        construct_id,
        encryption=s3.BucketEncryption.S3_MANAGED,
        versioned=False,
        block_public_access=s3.BlockPublicAccess(
            block_public_acls=True,
            ignore_public_acls=True,
            block_public_policy=False,
            restrict_public_buckets=False,
        ),
        enforce_ssl=True,
        removal_policy=cdk.RemovalPolicy.RETAIN,
        cors=[
            s3.CorsRule(
                allowed_methods=[s3.HttpMethods.GET, s3.HttpMethods.HEAD],
                allowed_origins=["*"],
                allowed_headers=["*"],
                max_age=3600,
            ),
        ],
    )
    bucket.add_to_resource_policy(
        iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            principals=[iam.AnyPrincipal()],
            actions=["s3:GetObject"],
            resources=[bucket.arn_for_objects("*")],
        ),
    )
    return bucket


class DownloadsStack(cdk.Stack):
    """Public-read S3 bucket for released FoodAtlas data bundles."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.downloads_bucket = _build_downloads_bucket(self, "DownloadsBucket")

        cdk.CfnOutput(
            self,
            "DownloadsBucketName",
            value=self.downloads_bucket.bucket_name,
            description="Public S3 bucket for released FoodAtlas data bundles",
        )
