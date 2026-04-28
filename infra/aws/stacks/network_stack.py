"""Network stack: VPC only.

Layout:
- 2 availability zones (structurally required by RDS subnet groups and ALB).
- Public subnets: ALB + ECS Fargate tasks (tasks get public IPs to reach
  ECR/Secrets Manager without a NAT gateway; ALB security group locks
  ingress to HTTPS from the internet).
- Isolated subnets: RDS instance (no internet route).
- No NAT gateway — saves ~$32/mo.

Security groups are created in the stacks that own them (DatabaseStack for
the RDS SG; ApiStack's `ApplicationLoadBalancedFargateService` auto-creates
ALB and task SGs). This keeps cross-stack dependencies one-directional and
avoids CDK cyclic references.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import aws_cdk as cdk
from aws_cdk import aws_ec2 as ec2

if TYPE_CHECKING:
    from constructs import Construct


class NetworkStack(cdk.Stack):
    """Shared VPC for all other stacks."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.vpc = ec2.Vpc(
            self,
            "Vpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="isolated",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                ),
            ],
            restrict_default_security_group=True,
        )
