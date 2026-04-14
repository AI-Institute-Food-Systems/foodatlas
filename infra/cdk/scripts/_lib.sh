#!/bin/bash
# Shared helpers for invoking one-off ECS tasks against FoodAtlasJobsStack.
# Sourced by run-migration.sh and run-data-load.sh.

set -euo pipefail

REGION="${AWS_REGION:-us-west-1}"
STACK="FoodAtlasJobsStack"
CONTAINER_NAME="JobsContainer"

_output() {
    aws cloudformation describe-stacks \
        --stack-name "$STACK" \
        --region "$REGION" \
        --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" \
        --output text
}

load_jobs_stack_outputs() {
    CLUSTER=$(_output JobsClusterName)
    TASK_DEF=$(_output JobsTaskDefinitionArn)
    SUBNET_IDS=$(_output JobsTaskSubnetIds)
    SG_ID=$(_output JobsTaskSecurityGroupId)
    LOG_GROUP=$(_output JobsLogGroupName)
}

run_jobs_task() {
    local command_json="$1"
    local description="$2"

    load_jobs_stack_outputs

    local subnets_json
    subnets_json=$(printf '"%s",' ${SUBNET_IDS//,/ })
    subnets_json="[${subnets_json%,}]"

    echo "Starting $description on cluster $CLUSTER..."
    local task_arn
    task_arn=$(aws ecs run-task \
        --cluster "$CLUSTER" \
        --task-definition "$TASK_DEF" \
        --launch-type FARGATE \
        --region "$REGION" \
        --network-configuration "awsvpcConfiguration={subnets=$subnets_json,securityGroups=[\"$SG_ID\"],assignPublicIp=ENABLED}" \
        --overrides "{\"containerOverrides\":[{\"name\":\"$CONTAINER_NAME\",\"command\":$command_json}]}" \
        --query 'tasks[0].taskArn' \
        --output text)

    local task_id="${task_arn##*/}"
    echo "Task started: $task_id"
    echo "Waiting for task to finish (this can take several minutes)..."
    aws ecs wait tasks-stopped --cluster "$CLUSTER" --tasks "$task_arn" --region "$REGION"

    local exit_code
    exit_code=$(aws ecs describe-tasks \
        --cluster "$CLUSTER" \
        --tasks "$task_arn" \
        --region "$REGION" \
        --query 'tasks[0].containers[0].exitCode' \
        --output text)

    echo
    echo "==== Task logs (CloudWatch: $LOG_GROUP) ===="
    aws logs tail "$LOG_GROUP" \
        --log-stream-name-prefix "foodatlas-db/$CONTAINER_NAME/$task_id" \
        --region "$REGION" \
        --since 1h || true
    echo "==== End logs ===="
    echo "Container exit code: $exit_code"

    if [[ "$exit_code" != "0" ]]; then
        echo "Task failed." >&2
        exit 1
    fi
}
