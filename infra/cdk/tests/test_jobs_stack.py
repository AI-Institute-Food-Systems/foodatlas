"""Snapshot tests for JobsStack."""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk.assertions import Match, Template

from stacks.api_stack import ApiStack
from stacks.database_stack import DatabaseStack
from stacks.ecr_stack import EcrStack
from stacks.jobs_stack import JobsStack
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
    api_stack = ApiStack(
        app,
        "TestApiStack",
        vpc=network.vpc,
        repository=ecr_stack.api_repository,
        db_instance=database.db_instance,
        db_secret=database.db_secret,
        kgc_bucket=storage.kgc_bucket,
    )
    stack = JobsStack(
        app,
        "TestJobsStack",
        vpc=network.vpc,
        cluster=api_stack.cluster,
        repository=ecr_stack.db_repository,
        db_instance=database.db_instance,
        db_secret=database.db_secret,
        kgc_bucket=storage.kgc_bucket,
    )
    return Template.from_stack(stack)


def test_jobs_stack_creates_a_task_definition_but_no_service() -> None:
    template = _synth()
    template.resource_count_is("AWS::ECS::TaskDefinition", 1)
    template.resource_count_is("AWS::ECS::Service", 0)


def test_task_definition_sized_for_etl_workload() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::ECS::TaskDefinition",
        Match.object_like(
            {
                "Cpu": "4096",
                "Memory": "16384",
                "NetworkMode": "awsvpc",
                "RequiresCompatibilities": ["FARGATE"],
            },
        ),
    )


def test_task_security_group_allows_egress() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::EC2::SecurityGroup",
        Match.object_like(
            {
                "GroupDescription": "FoodAtlas one-off jobs Fargate task egress",
                "SecurityGroupEgress": Match.any_value(),
            },
        ),
    )


def test_log_group_retention_one_month() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::Logs::LogGroup",
        Match.object_like({"RetentionInDays": 30}),
    )


def test_outputs_present_for_run_task_invocation() -> None:
    template = _synth()
    template.has_output("JobsClusterName", {})
    template.has_output("JobsTaskDefinitionArn", {})
    template.has_output("JobsTaskSubnetIds", {})
    template.has_output("JobsTaskSecurityGroupId", {})
    template.has_output("JobsLogGroupName", {})
