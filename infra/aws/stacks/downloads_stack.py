"""Downloads stack: S3 bucket for released data bundles, zips gated.

Holds two things:

- ``bundles/<version>/<filename>`` — released FoodAtlas data bundles
  (parquet zips) plus a ``SUMMARY.md`` per version. Uploaded via
  ``backend/kgc/scripts/publish-bundle.sh``.
- ``bundles/index.json`` — manifest describing all released bundles
  (version, release date, file size, links). The API reads this over
  HTTPS to populate the downloads page.

Access is split by object type. The manifest and the ``*.md`` summaries
are anonymously readable so the downloads page renders for everyone. The
zips are **not**: downloads are gated exactly like the API — a key
holder calls ``/v1/bundles/{version}/download`` and the API, whose task
role has ``s3:GetObject`` here, answers with a short-lived pre-signed
URL. Listing is never granted.
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
    # Anonymous read for the manifest and the summaries only. The zips fall
    # through to the account default (private) and are reached solely via
    # the API's pre-signed URLs.
    bucket.add_to_resource_policy(
        iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            principals=[iam.AnyPrincipal()],
            actions=["s3:GetObject"],
            resources=[
                bucket.arn_for_objects("bundles/index.json"),
                bucket.arn_for_objects("bundles/*/*.md"),
            ],
        ),
    )
    return bucket


class DownloadsStack(cdk.Stack):
    """S3 bucket for released FoodAtlas data bundles (manifest public, zips gated)."""

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
            description="S3 bucket for released FoodAtlas data bundles (zips gated)",
        )
