"""ECR stack: container repositories for FoodAtlas backend images.

Holds two repositories:

- ``foodatlas-api``  — the long-running FastAPI service image (ApiStack)
- ``foodatlas-db``   — the ad-hoc one-off jobs image used for Alembic
  migrations and ETL data loads (JobsStack)

Split from the consuming stacks to break the chicken-and-egg at first
deploy: ECS cannot start a task until an image already exists in the
repository, so the repos must be created and populated before the stacks
that reference them deploy. The deploy sequence is:

1. ``cdk deploy FoodAtlasEcrStack``           -- empty repos exist
2. ``docker build`` + ``docker push``         -- populate each repo
3. ``cdk deploy FoodAtlasApiStack``           -- ECS pulls the API image
4. ``cdk deploy FoodAtlasJobsStack``          -- task definition references db image
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import aws_cdk as cdk
from aws_cdk import RemovalPolicy
from aws_cdk import aws_ecr as ecr

if TYPE_CHECKING:
    from constructs import Construct


def _build_repository(scope: cdk.Stack, construct_id: str, name: str) -> ecr.Repository:
    return ecr.Repository(
        scope,
        construct_id,
        repository_name=name,
        image_scan_on_push=True,
        removal_policy=RemovalPolicy.RETAIN,
        lifecycle_rules=[
            ecr.LifecycleRule(
                description="Keep last 10 images",
                max_image_count=10,
                rule_priority=1,
            ),
        ],
    )


class EcrStack(cdk.Stack):
    """ECR repositories for the FoodAtlas backend images."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs: object,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.api_repository = _build_repository(self, "ApiRepository", "foodatlas-api")
        self.db_repository = _build_repository(self, "DbRepository", "foodatlas-db")

        cdk.CfnOutput(
            self,
            "ApiRepositoryUri",
            value=self.api_repository.repository_uri,
            description="ECR repository URI for pushing the API image",
        )
        cdk.CfnOutput(
            self,
            "DbRepositoryUri",
            value=self.db_repository.repository_uri,
            description="ECR repository URI for pushing the db jobs image",
        )
