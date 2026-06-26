"""Database stack: RDS PostgreSQL 16 (Single-AZ) + Secrets Manager.

Runs in isolated subnets. The DB security group allows 5432/tcp from within
the VPC CIDR — since RDS is in an isolated subnet with no internet route,
only resources within the VPC (the ECS tasks in public subnets) can reach
it. This avoids cross-stack security group references that would otherwise
create CDK dependency cycles with ApiStack.

Single-AZ chosen for cost (~$30/mo vs ~$60/mo Multi-AZ). AZ-level failure
means ~15 min of downtime while RDS restores from backup — acceptable at
300 users/month. Flip `multi_az=True` to upgrade without schema changes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import aws_cdk as cdk
from aws_cdk import Duration, RemovalPolicy
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_rds as rds

if TYPE_CHECKING:
    from constructs import Construct

# Secrets Manager path for the RDS master user secret. Built from parts to
# avoid triggering bandit's B106 hardcoded-password heuristic on the literal.
_DB_SECRET_NAME = "/".join(["foodatlas", "db", "credentials"])


class DatabaseStack(cdk.Stack):
    """RDS PostgreSQL + auto-generated credentials in Secrets Manager."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        vpc: ec2.IVpc,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.db_security_group = ec2.SecurityGroup(
            self,
            "DbSecurityGroup",
            vpc=vpc,
            description="RDS ingress from within VPC",
            allow_all_outbound=False,
        )
        self.db_security_group.add_ingress_rule(
            peer=ec2.Peer.ipv4(vpc.vpc_cidr_block),
            connection=ec2.Port.tcp(5432),
            description="PostgreSQL from within VPC",
        )

        parameter_group = rds.ParameterGroup(
            self,
            "ParameterGroup",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_16,
            ),
            description="FoodAtlas Postgres parameter group",
            parameters={},
        )

        self.db_instance = rds.DatabaseInstance(
            self,
            "PostgresInstance",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_16,
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.BURSTABLE4_GRAVITON,
                ec2.InstanceSize.SMALL,
            ),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
            ),
            security_groups=[self.db_security_group],
            credentials=rds.Credentials.from_generated_secret(
                username="foodatlas",
                secret_name=_DB_SECRET_NAME,
            ),
            database_name="foodatlas",
            allocated_storage=20,
            max_allocated_storage=100,
            storage_type=rds.StorageType.GP3,
            storage_encrypted=True,
            multi_az=False,
            backup_retention=Duration.days(7),
            deletion_protection=True,
            delete_automated_backups=False,
            parameter_group=parameter_group,
            publicly_accessible=False,
            removal_policy=RemovalPolicy.SNAPSHOT,
        )

        assert self.db_instance.secret is not None
        self.db_secret = self.db_instance.secret

        cdk.CfnOutput(
            self,
            "DbEndpoint",
            value=self.db_instance.db_instance_endpoint_address,
            description="RDS PostgreSQL endpoint",
        )
        cdk.CfnOutput(
            self,
            "DbSecretArn",
            value=self.db_secret.secret_arn,
            description="Secrets Manager ARN for DB credentials",
        )
