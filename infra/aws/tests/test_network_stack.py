"""Snapshot tests for NetworkStack."""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk.assertions import Template

from stacks.network_stack import NetworkStack


def _synth() -> Template:
    app = cdk.App()
    stack = NetworkStack(app, "TestNetworkStack")
    return Template.from_stack(stack)


def test_vpc_exists() -> None:
    template = _synth()
    template.resource_count_is("AWS::EC2::VPC", 1)


def test_vpc_has_two_azs() -> None:
    template = _synth()
    # 2 AZs x 2 subnet configurations = 4 subnets
    template.resource_count_is("AWS::EC2::Subnet", 4)


def test_vpc_has_no_nat_gateway() -> None:
    template = _synth()
    template.resource_count_is("AWS::EC2::NatGateway", 0)


def test_public_and_isolated_subnets_exist() -> None:
    template = _synth()
    # Public subnets have MapPublicIpOnLaunch=true; isolated do not
    template.resource_properties_count_is(
        "AWS::EC2::Subnet",
        {"MapPublicIpOnLaunch": True},
        2,
    )
