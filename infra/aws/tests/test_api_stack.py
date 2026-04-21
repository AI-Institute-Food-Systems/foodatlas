"""Snapshot tests for ApiStack."""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk.assertions import Match, Template

from stacks.api_stack import ApiStack
from stacks.database_stack import DatabaseStack
from stacks.downloads_stack import DownloadsStack
from stacks.ecr_stack import EcrStack
from stacks.network_stack import NetworkStack
from stacks.storage_stack import StorageStack

_FAKE_CERT_ARN = (
    "arn:aws:acm:us-west-1:123456789012:certificate/"
    "11111111-2222-3333-4444-555555555555"
)


def _synth(*, cert_arn: str | None = None) -> Template:
    context = {"api_cert_arn": cert_arn} if cert_arn else None
    app = cdk.App(context=context)
    network = NetworkStack(app, "TestNetworkStack")
    storage = StorageStack(app, "TestStorageStack")
    downloads = DownloadsStack(app, "TestDownloadsStack")
    ecr_stack = EcrStack(app, "TestEcrStack")
    database = DatabaseStack(
        app,
        "TestDatabaseStack",
        vpc=network.vpc,
    )
    stack = ApiStack(
        app,
        "TestApiStack",
        vpc=network.vpc,
        repository=ecr_stack.api_repository,
        db_instance=database.db_instance,
        db_secret=database.db_secret,
        kgc_bucket=storage.kgc_bucket,
        downloads_bucket=downloads.downloads_bucket,
    )
    return Template.from_stack(stack)


def test_api_stack_does_not_declare_ecr_repository() -> None:
    template = _synth()
    template.resource_count_is("AWS::ECR::Repository", 0)


def test_ecs_cluster_and_service_exist() -> None:
    template = _synth()
    template.resource_count_is("AWS::ECS::Cluster", 1)
    template.resource_count_is("AWS::ECS::Service", 1)


def test_task_definition_has_correct_cpu_and_memory() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::ECS::TaskDefinition",
        Match.object_like(
            {
                "Cpu": "256",
                "Memory": "512",
                "NetworkMode": "awsvpc",
                "RequiresCompatibilities": ["FARGATE"],
            },
        ),
    )


def test_alb_is_internet_facing() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::ElasticLoadBalancingV2::LoadBalancer",
        Match.object_like({"Scheme": "internet-facing", "Type": "application"}),
    )


def test_target_group_health_check_path_is_health() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::ElasticLoadBalancingV2::TargetGroup",
        Match.object_like(
            {
                "HealthCheckPath": "/health",
                "Protocol": "HTTP",
                "Port": 80,
            },
        ),
    )


def test_log_group_retention_one_month() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::Logs::LogGroup",
        Match.object_like({"RetentionInDays": 30}),
    )


def test_api_key_secret_resource_exists() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::SecretsManager::Secret",
        Match.object_like(
            {
                "GenerateSecretString": Match.object_like(
                    {
                        "PasswordLength": 32,
                        "ExcludePunctuation": True,
                    },
                ),
            },
        ),
    )


def test_task_definition_injects_api_key_secret() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::ECS::TaskDefinition",
        Match.object_like(
            {
                "ContainerDefinitions": Match.array_with(
                    [
                        Match.object_like(
                            {
                                "Secrets": Match.array_with(
                                    [Match.object_like({"Name": "API_KEY"})],
                                ),
                            },
                        ),
                    ],
                ),
            },
        ),
    )


def test_http_mode_has_single_port_80_listener() -> None:
    template = _synth()
    template.resource_count_is("AWS::ElasticLoadBalancingV2::Listener", 1)
    template.has_resource_properties(
        "AWS::ElasticLoadBalancingV2::Listener",
        Match.object_like({"Protocol": "HTTP", "Port": 80}),
    )


def test_https_mode_declares_443_listener_with_cert() -> None:
    template = _synth(cert_arn=_FAKE_CERT_ARN)
    template.has_resource_properties(
        "AWS::ElasticLoadBalancingV2::Listener",
        Match.object_like(
            {
                "Protocol": "HTTPS",
                "Port": 443,
                "Certificates": [{"CertificateArn": _FAKE_CERT_ARN}],
            },
        ),
    )


def test_https_mode_redirects_port_80_to_443() -> None:
    template = _synth(cert_arn=_FAKE_CERT_ARN)
    template.resource_count_is("AWS::ElasticLoadBalancingV2::Listener", 2)
    template.has_resource_properties(
        "AWS::ElasticLoadBalancingV2::Listener",
        Match.object_like(
            {
                "Protocol": "HTTP",
                "Port": 80,
                "DefaultActions": [
                    {
                        "Type": "redirect",
                        "RedirectConfig": Match.object_like(
                            {
                                "Protocol": "HTTPS",
                                "Port": "443",
                                "StatusCode": "HTTP_301",
                            },
                        ),
                    },
                ],
            },
        ),
    )
