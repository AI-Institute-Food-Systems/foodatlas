#!/usr/bin/env bash
# Open a shell inside a running API container via ECS exec (SSM Session
# Manager under the hood) — no SSH bastion, no RDS port-forward setup.
# The container already has DB credentials in env (DB_HOST/DB_USER/
# DB_PASSWORD/DB_NAME) and VPC reach to the RDS, so once you're in:
#
#   $ python -c 'from sqlalchemy import create_engine, text; from src.config import Settings; s=Settings(); e=create_engine(s.db_url); c=e.connect(); c.execute(text("SELECT 1")); c.commit()'
#
# …becomes the canonical way to run ad-hoc SQL against the live RDS.
#
# Usage:
#   ./exec-api-shell.sh                       # staging by default
#   ./exec-api-shell.sh prod                  # explicit prod target
#   STACK=FoodAtlasApiStack ./exec-api-shell.sh
#
# Prereqs: aws sso login --profile <profile>; AWS Session Manager
# plugin installed locally (https://docs.aws.amazon.com/systems-manager/
# latest/userguide/session-manager-working-with-install-plugin.html).

set -euo pipefail

ENV_ARG="${1:-staging}"
case "$ENV_ARG" in
    staging) STACK="${STACK:-FoodAtlasApiStack-Staging}";;
    prod | production) STACK="${STACK:-FoodAtlasApiStack}";;
    *)
        echo "Usage: $0 [staging|prod]" >&2
        exit 1
        ;;
esac

REGION="${AWS_REGION:-us-west-1}"

# Resolve cluster + service from the stack's ApiService construct.
CLUSTER=$(aws cloudformation describe-stack-resources \
    --stack-name "$STACK" \
    --region "$REGION" \
    --query "StackResources[?LogicalResourceId=='ApiCluster'].PhysicalResourceId" \
    --output text)
SERVICE=$(aws cloudformation describe-stack-resources \
    --stack-name "$STACK" \
    --region "$REGION" \
    --query "StackResources[?starts_with(LogicalResourceId, 'ApiService') && ResourceType=='AWS::ECS::Service'].PhysicalResourceId" \
    --output text)

if [[ -z "$CLUSTER" || -z "$SERVICE" ]]; then
    echo "Couldn't resolve cluster/service from $STACK. Is the stack deployed?" >&2
    exit 1
fi

# Pick the first running task — for ad-hoc ops it doesn't matter which.
TASK_ARN=$(aws ecs list-tasks \
    --cluster "$CLUSTER" \
    --service-name "$SERVICE" \
    --desired-status RUNNING \
    --region "$REGION" \
    --query "taskArns[0]" \
    --output text)

if [[ -z "$TASK_ARN" || "$TASK_ARN" == "None" ]]; then
    echo "No running tasks in $SERVICE." >&2
    exit 1
fi

# Container name in the ApplicationLoadBalancedFargateService pattern
# is "web" by default. Override via CONTAINER env if your task is different.
CONTAINER_NAME="${CONTAINER:-web}"

echo "stack:     $STACK"
echo "cluster:   $CLUSTER"
echo "service:   $SERVICE"
echo "task:      $TASK_ARN"
echo "container: $CONTAINER_NAME"
echo

exec aws ecs execute-command \
    --cluster "$CLUSTER" \
    --task "$TASK_ARN" \
    --container "$CONTAINER_NAME" \
    --command "/bin/bash" \
    --interactive \
    --region "$REGION"
