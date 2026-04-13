"""ECR stack: repository for the FastAPI backend image.

Split from ApiStack to break the chicken-and-egg at first deploy: ApiStack's
ECS service cannot start until an image exists in the repository, but the
repository itself used to be created by ApiStack. Keeping the repository in a
standalone stack lets the deploy sequence be:

1. ``cdk deploy FoodAtlasEcrStack``           -- empty repo exists
2. ``docker build`` + ``docker push``         -- populate the repo
3. ``cdk deploy FoodAtlasApiStack``           -- ECS pulls the image
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import aws_cdk as cdk
from aws_cdk import RemovalPolicy
from aws_cdk import aws_ecr as ecr

if TYPE_CHECKING:
    from constructs import Construct


class EcrStack(cdk.Stack):
    """ECR repository for the FoodAtlas API image."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs: object,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.repository = ecr.Repository(
            self,
            "ApiRepository",
            repository_name="foodatlas-api",
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

        cdk.CfnOutput(
            self,
            "EcrRepositoryUri",
            value=self.repository.repository_uri,
            description="ECR repository URI for pushing the API image",
        )
