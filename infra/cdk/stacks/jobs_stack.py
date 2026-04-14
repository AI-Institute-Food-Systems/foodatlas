"""Jobs stack: ad-hoc one-off task definition for the foodatlas-db image.

Hosts a Fargate task definition (no service) that runs Alembic migrations
and ETL data loads on demand. Invoked via ``aws ecs run-task`` with command
overrides — see ``infra/cdk/scripts/run-migration.sh`` and
``infra/cdk/scripts/run-data-load.sh``.

The stack reuses the ECS cluster from ApiStack so we don't pay for or
manage a second cluster (clusters themselves are free, but consolidating
them keeps the operational surface smaller).

Outputs cluster name, task definition ARN, subnet IDs, and security group
ID — everything needed to construct an ``aws ecs run-task`` invocation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import aws_cdk as cdk
from aws_cdk import RemovalPolicy
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_logs as logs
from aws_cdk import aws_rds as rds
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secretsmanager

if TYPE_CHECKING:
    from constructs import Construct


class JobsStack(cdk.Stack):
    """One-off Fargate task definition for migrations and ETL loads."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        vpc: ec2.IVpc,
        cluster: ecs.ICluster,
        repository: ecr.IRepository,
        db_instance: rds.IDatabaseInstance,
        db_secret: secretsmanager.ISecret,
        kgc_bucket: s3.IBucket,
        **kwargs: object,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        image_tag = self.node.try_get_context("db_image_tag") or "latest"

        log_group = logs.LogGroup(
            self,
            "JobsLogGroup",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Sized for the ETL load. The materializer holds the full KG in
        # pandas DataFrames and joins them in memory, and earlier 4 GB runs
        # OOM'd. Generous headroom: 4 vCPU + 16 GB keeps the per-month
        # one-off run cheap (per-second billing) while making OOMs unlikely
        # at current data scale. Migrations only need a fraction of this
        # but reuse the same task definition.
        self.task_definition = ecs.FargateTaskDefinition(
            self,
            "JobsTaskDefinition",
            cpu=4096,
            memory_limit_mib=16384,
            runtime_platform=ecs.RuntimePlatform(
                cpu_architecture=ecs.CpuArchitecture.X86_64,
                operating_system_family=ecs.OperatingSystemFamily.LINUX,
            ),
        )

        self.task_definition.add_container(
            "JobsContainer",
            image=ecs.ContainerImage.from_ecr_repository(
                repository,
                tag=image_tag,
            ),
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="foodatlas-db",
                log_group=log_group,
            ),
            environment={
                "DB_HOST": db_instance.db_instance_endpoint_address,
                "DB_PORT": db_instance.db_instance_endpoint_port,
                "DB_NAME": "foodatlas",
                "KGC_BUCKET": kgc_bucket.bucket_name,
                "AWS_DEFAULT_REGION": cdk.Stack.of(self).region,
            },
            secrets={
                "DB_USER": ecs.Secret.from_secrets_manager(db_secret, "username"),
                "DB_PASSWORD": ecs.Secret.from_secrets_manager(db_secret, "password"),
            },
        )

        # ETL reads parquet from S3; migrations don't need it but the same
        # task role is reused for both invocation paths.
        kgc_bucket.grant_read(self.task_definition.task_role)

        # Tasks need outbound HTTPS to reach ECR (image pull), Secrets
        # Manager (credential fetch), S3 (parquet download), and RDS
        # (Postgres on 5432, allowed by RDS SG from VPC CIDR).
        self.task_security_group = ec2.SecurityGroup(
            self,
            "JobsTaskSecurityGroup",
            vpc=vpc,
            description="FoodAtlas one-off jobs Fargate task egress",
            allow_all_outbound=True,
        )

        public_subnets = vpc.select_subnets(subnet_type=ec2.SubnetType.PUBLIC)

        cdk.CfnOutput(
            self,
            "JobsClusterName",
            value=cluster.cluster_name,
            description="ECS cluster name (shared with ApiStack)",
        )
        cdk.CfnOutput(
            self,
            "JobsTaskDefinitionArn",
            value=self.task_definition.task_definition_arn,
            description="ECS task definition ARN for one-off jobs",
        )
        cdk.CfnOutput(
            self,
            "JobsTaskSubnetIds",
            value=",".join(public_subnets.subnet_ids),
            description="Public subnet IDs to launch one-off tasks in",
        )
        cdk.CfnOutput(
            self,
            "JobsTaskSecurityGroupId",
            value=self.task_security_group.security_group_id,
            description="Security group ID for one-off task ENIs",
        )
        cdk.CfnOutput(
            self,
            "JobsLogGroupName",
            value=log_group.log_group_name,
            description="CloudWatch log group for one-off task output",
        )
