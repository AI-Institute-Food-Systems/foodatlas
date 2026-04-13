"""Snapshot tests for ApiStack."""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk.assertions import Match, Template

from stacks.api_stack import ApiStack
from stacks.database_stack import DatabaseStack
from stacks.ecr_stack import EcrStack
from stacks.network_stack import NetworkStack
from stacks.storage_stack import StorageStack


def _synth() -> Template:
    app = cdk.App()
    network = NetworkStack(app, "TestNetworkStack")
    storage = StorageStack(app, "TestStorageStack")
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
        repository=ecr_stack.repository,
        db_instance=database.db_instance,
        db_secret=database.db_secret,
        parquet_bucket=storage.parquet_bucket,
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
