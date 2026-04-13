"""CDK app entry point.

Instantiates the five stacks that make up the FoodAtlas AWS infrastructure:

- NetworkStack: VPC, subnets, security groups
- StorageStack: S3 bucket for KGC parquet outputs
- EcrStack: ECR repository for the FastAPI backend image
- DatabaseStack: RDS PostgreSQL + Secrets Manager
- ApiStack: ECS Fargate + ALB hosting the FastAPI backend

EcrStack is separate from ApiStack so the repo can be populated with a
Docker image before the ECS service tries to pull on first deploy.

Dependencies between stacks are wired via constructor arguments, so CDK
resolves deploy order automatically.
"""

from __future__ import annotations

import os

import aws_cdk as cdk

from stacks.api_stack import ApiStack
from stacks.database_stack import DatabaseStack
from stacks.ecr_stack import EcrStack
from stacks.network_stack import NetworkStack
from stacks.storage_stack import StorageStack

app = cdk.App()

env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION", "us-west-1"),
)

network = NetworkStack(
    app,
    "FoodAtlasNetworkStack",
    env=env,
)

storage = StorageStack(
    app,
    "FoodAtlasStorageStack",
    env=env,
)

ecr_stack = EcrStack(
    app,
    "FoodAtlasEcrStack",
    env=env,
)

database = DatabaseStack(
    app,
    "FoodAtlasDatabaseStack",
    vpc=network.vpc,
    env=env,
)

ApiStack(
    app,
    "FoodAtlasApiStack",
    vpc=network.vpc,
    repository=ecr_stack.repository,
    db_instance=database.db_instance,
    db_secret=database.db_secret,
    parquet_bucket=storage.parquet_bucket,
    env=env,
)

app.synth()
