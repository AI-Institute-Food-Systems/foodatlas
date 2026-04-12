"""API stack: ECR repository + ECS Fargate service behind an ALB.

The Fargate task runs the FastAPI backend image from ECR. Secrets Manager
supplies DB credentials at task start; the ALB listens on port 80 (HTTPS
upgrade pending ACM cert) and forwards to the task on port 8000.

Networking: tasks are placed in public subnets with public IPs so they can
pull from ECR and reach Secrets Manager without a NAT gateway. Security
groups for the ALB and the Fargate task are auto-created by the
`ApplicationLoadBalancedFargateService` pattern — keeping them within this
stack avoids cross-stack SG dependency cycles with NetworkStack.

**Before first deploy** an image must exist in the ECR repository — otherwise
the service will fail to start. Build + push the API image from
`backend/api/` to the created repository, then deploy this stack.

The stack uses a build-time context variable `api_image_tag` (default `latest`)
to pick the image tag. Override with `cdk deploy -c api_image_tag=<sha>`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import aws_cdk as cdk
from aws_cdk import Duration, RemovalPolicy
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_ecs_patterns as ecs_patterns
from aws_cdk import aws_logs as logs
from aws_cdk import aws_rds as rds
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secretsmanager

if TYPE_CHECKING:
    from constructs import Construct


class ApiStack(cdk.Stack):
    """ECR + ECS Fargate + ALB hosting the FastAPI backend."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        vpc: ec2.IVpc,
        db_instance: rds.IDatabaseInstance,
        db_secret: secretsmanager.ISecret,
        parquet_bucket: s3.IBucket,
        **kwargs: object,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        image_tag = self.node.try_get_context("api_image_tag") or "latest"

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

        cluster = ecs.Cluster(
            self,
            "ApiCluster",
            vpc=vpc,
        )

        log_group = logs.LogGroup(
            self,
            "ApiLogGroup",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )

        task_definition = ecs.FargateTaskDefinition(
            self,
            "ApiTaskDefinition",
            cpu=256,
            memory_limit_mib=512,
            runtime_platform=ecs.RuntimePlatform(
                cpu_architecture=ecs.CpuArchitecture.X86_64,
                operating_system_family=ecs.OperatingSystemFamily.LINUX,
            ),
        )

        task_definition.add_container(
            "ApiContainer",
            image=ecs.ContainerImage.from_ecr_repository(
                self.repository,
                tag=image_tag,
            ),
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="foodatlas-api",
                log_group=log_group,
            ),
            environment={
                # API_HOST is set via the Dockerfile ENV to bind all
                # interfaces inside the container; no need to duplicate here.
                "API_DEBUG": "False",
                "DB_HOST": db_instance.db_instance_endpoint_address,
                "DB_PORT": db_instance.db_instance_endpoint_port,
                "DB_NAME": "foodatlas",
                "PARQUET_BUCKET": parquet_bucket.bucket_name,
            },
            secrets={
                "DB_USER": ecs.Secret.from_secrets_manager(db_secret, "username"),
                "DB_PASSWORD": ecs.Secret.from_secrets_manager(db_secret, "password"),
            },
            port_mappings=[
                ecs.PortMapping(container_port=8000, protocol=ecs.Protocol.TCP),
            ],
        )

        # Task role needs read access to parquet bucket for the DB load step
        parquet_bucket.grant_read(task_definition.task_role)

        self.service = ecs_patterns.ApplicationLoadBalancedFargateService(
            self,
            "ApiService",
            cluster=cluster,
            task_definition=task_definition,
            desired_count=1,
            public_load_balancer=True,
            assign_public_ip=True,
            task_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            listener_port=80,  # TODO: switch to 443 after ACM cert is added
            health_check_grace_period=Duration.seconds(60),
        )

        self.service.target_group.configure_health_check(
            path="/health",
            healthy_http_codes="200",
            interval=Duration.seconds(30),
            timeout=Duration.seconds(5),
            healthy_threshold_count=2,
            unhealthy_threshold_count=3,
        )

        cdk.CfnOutput(
            self,
            "ApiUrl",
            value=f"http://{self.service.load_balancer.load_balancer_dns_name}",
            description="Public API URL (ALB DNS)",
        )
        cdk.CfnOutput(
            self,
            "EcrRepositoryUri",
            value=self.repository.repository_uri,
            description="ECR repository URI for pushing the API image",
        )
