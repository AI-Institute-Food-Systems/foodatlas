"""API stack: ECS Fargate service behind an ALB.

The Fargate task runs the FastAPI backend image from the ECR repository
provided by :class:`stacks.ecr_stack.EcrStack`. Secrets Manager supplies DB
credentials at task start; the ALB forwards to the task on port 8000.

When the context variable ``api_cert_arn`` is set, the ALB listens on 443
with HTTPS (cert imported from ACM by ARN) and redirects port 80 → 443.
Without the context variable, the ALB falls back to plain HTTP on port 80
so that local ``cdk synth`` and snapshot tests don't require a real cert.

Networking: tasks are placed in public subnets with public IPs so they can
pull from ECR and reach Secrets Manager without a NAT gateway. Security
groups for the ALB and the Fargate task are auto-created by the
`ApplicationLoadBalancedFargateService` pattern — keeping them within this
stack avoids cross-stack SG dependency cycles with NetworkStack.

**Before first deploy** an image must exist in the ECR repository — otherwise
the service will fail to start. Deploy :class:`EcrStack`, then build + push
the API image from ``backend/api/`` to that repository, then deploy this
stack.

Context variables:
- ``api_image_tag`` (default ``latest``): image tag in ECR to deploy.
- ``api_cert_arn`` (optional): ACM certificate ARN in the same region as
  the ALB. When set, enables HTTPS termination on port 443.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import aws_cdk as cdk
from aws_cdk import Duration, RemovalPolicy
from aws_cdk import aws_certificatemanager as acm
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_ecs_patterns as ecs_patterns
from aws_cdk import aws_elasticloadbalancingv2 as elbv2
from aws_cdk import aws_logs as logs
from aws_cdk import aws_rds as rds
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secretsmanager

if TYPE_CHECKING:
    from constructs import Construct


class ApiStack(cdk.Stack):
    """ECS Fargate + ALB hosting the FastAPI backend."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        vpc: ec2.IVpc,
        repository: ecr.IRepository,
        db_instance: rds.IDatabaseInstance,
        db_secret: secretsmanager.ISecret,
        kgc_bucket: s3.IBucket,
        downloads_bucket: s3.IBucket,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        image_tag = self.node.try_get_context("api_image_tag") or "latest"

        self.cluster = ecs.Cluster(
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

        api_key_secret = secretsmanager.Secret(
            self,
            "ApiKeySecret",
            description="Bearer token for the FoodAtlas API (polite gate).",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                password_length=32,
                exclude_punctuation=True,
            ),
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
                repository,
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
                "API_CORS_ORIGINS": ",".join(
                    [
                        "https://foodatlas.ai",
                        "https://www.foodatlas.ai",
                        "https://dev.foodatlas.ai",
                        "http://localhost:3000",
                    ],
                ),
                "DB_HOST": db_instance.db_instance_endpoint_address,
                "DB_PORT": db_instance.db_instance_endpoint_port,
                "DB_NAME": "foodatlas",
                "KGC_BUCKET": kgc_bucket.bucket_name,
                "API_DOWNLOADS_BUCKET": downloads_bucket.bucket_name,
                "API_DOWNLOADS_REGION": cdk.Stack.of(self).region,
            },
            secrets={
                "DB_USER": ecs.Secret.from_secrets_manager(db_secret, "username"),
                "DB_PASSWORD": ecs.Secret.from_secrets_manager(db_secret, "password"),
                "API_KEY": ecs.Secret.from_secrets_manager(api_key_secret),
            },
            port_mappings=[
                ecs.PortMapping(container_port=8000, protocol=ecs.Protocol.TCP),
            ],
        )

        # Task role needs read access to KGC bucket for ad-hoc fetches
        kgc_bucket.grant_read(task_definition.task_role)

        cert_arn = self.node.try_get_context("api_cert_arn")
        service_kwargs: dict[str, Any] = {
            "cluster": self.cluster,
            "task_definition": task_definition,
            "desired_count": 1,
            "min_healthy_percent": 100,
            "public_load_balancer": True,
            "assign_public_ip": True,
            "task_subnets": ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            "health_check_grace_period": Duration.seconds(60),
        }
        if cert_arn:
            certificate = acm.Certificate.from_certificate_arn(
                self,
                "ApiCertificate",
                cert_arn,
            )
            service_kwargs.update(
                certificate=certificate,
                protocol=elbv2.ApplicationProtocol.HTTPS,
                redirect_http=True,
                listener_port=443,
            )
        else:
            service_kwargs["listener_port"] = 80

        self.service = ecs_patterns.ApplicationLoadBalancedFargateService(
            self,
            "ApiService",
            **service_kwargs,
        )

        self.service.target_group.configure_health_check(
            path="/health",
            healthy_http_codes="200",
            interval=Duration.seconds(30),
            timeout=Duration.seconds(5),
            healthy_threshold_count=2,
            unhealthy_threshold_count=3,
        )

        scheme = "https" if cert_arn else "http"
        cdk.CfnOutput(
            self,
            "ApiUrl",
            value=f"{scheme}://{self.service.load_balancer.load_balancer_dns_name}",
            description="Public API URL (ALB DNS)",
        )

        cdk.CfnOutput(
            self,
            "ApiKeySecretArn",
            value=api_key_secret.secret_arn,
            description=(
                "Secrets Manager ARN for the API bearer token. Fetch the "
                "value with: aws secretsmanager get-secret-value "
                "--secret-id <arn> --query SecretString --output text"
            ),
        )
