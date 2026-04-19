"""CDK app entry point.

Instantiates the six stacks that make up the FoodAtlas AWS infrastructure:

- NetworkStack: VPC, subnets, security groups
- StorageStack: S3 bucket for KGC source data and pipeline artifacts
- EcrStack: ECR repositories for the API and db jobs images
- DatabaseStack: RDS PostgreSQL + Secrets Manager
- ApiStack: ECS Fargate + ALB hosting the FastAPI backend
- JobsStack: one-off Fargate task definition for ETL data loads

EcrStack is separate from the consuming stacks so the repos can be populated
with Docker images before ECS tries to pull them on first deploy. JobsStack
reuses the ECS cluster from ApiStack to keep the operational surface small.

Dependencies between stacks are wired via constructor arguments, so CDK
resolves deploy order automatically.
"""

from __future__ import annotations

import os

import aws_cdk as cdk

from stacks.api_stack import ApiStack
from stacks.database_stack import DatabaseStack
from stacks.ecr_stack import EcrStack
from stacks.jobs_stack import JobsStack
from stacks.network_stack import NetworkStack
from stacks.storage_stack import StorageStack

app = cdk.App()

_account = os.environ.get("CDK_DEFAULT_ACCOUNT")
env = (
    cdk.Environment(
        account=_account,
        region=os.environ.get("CDK_DEFAULT_REGION", "us-west-1"),
    )
    if _account and _account != "000000000000"
    else None
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

api_stack = ApiStack(
    app,
    "FoodAtlasApiStack",
    vpc=network.vpc,
    repository=ecr_stack.api_repository,
    db_instance=database.db_instance,
    db_secret=database.db_secret,
    kgc_bucket=storage.kgc_bucket,
    env=env,
)

JobsStack(
    app,
    "FoodAtlasJobsStack",
    vpc=network.vpc,
    cluster=api_stack.cluster,
    repository=ecr_stack.db_repository,
    db_instance=database.db_instance,
    db_secret=database.db_secret,
    kgc_bucket=storage.kgc_bucket,
    env=env,
)

app.synth()
