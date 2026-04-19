"""Snapshot tests for DatabaseStack."""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk.assertions import Match, Template

from stacks.database_stack import DatabaseStack
from stacks.network_stack import NetworkStack


def _synth() -> Template:
    app = cdk.App()
    network = NetworkStack(app, "TestNetworkStack")
    stack = DatabaseStack(
        app,
        "TestDatabaseStack",
        vpc=network.vpc,
    )
    return Template.from_stack(stack)


def test_db_instance_exists() -> None:
    template = _synth()
    template.resource_count_is("AWS::RDS::DBInstance", 1)


def test_db_is_postgres_16_single_az_t4g_small() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::RDS::DBInstance",
        Match.object_like(
            {
                "Engine": "postgres",
                "DBInstanceClass": "db.t4g.small",
                "MultiAZ": False,
                "StorageEncrypted": True,
                "StorageType": "gp3",
                "AllocatedStorage": "20",
                "BackupRetentionPeriod": 7,
                "DeletionProtection": True,
                "PubliclyAccessible": False,
            },
        ),
    )


def test_db_engine_version_starts_with_16() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::RDS::DBInstance",
        Match.object_like(
            {"EngineVersion": Match.string_like_regexp(r"^16.*")},
        ),
    )


def test_db_secret_is_created() -> None:
    template = _synth()
    template.has_resource_properties(
        "AWS::SecretsManager::Secret",
        Match.object_like({"Name": "foodatlas/db/credentials"}),
    )


def test_db_security_group_exists() -> None:
    template = _synth()
    template.resource_count_is("AWS::EC2::SecurityGroup", 1)


def test_db_has_parameter_group() -> None:
    template = _synth()
    template.resource_count_is("AWS::RDS::DBParameterGroup", 1)
