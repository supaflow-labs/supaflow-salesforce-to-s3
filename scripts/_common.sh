#!/bin/bash
# Common variables and functions for management scripts

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
SAMCONFIG="$ROOT_DIR/samconfig.toml"

# Colors (used by sourcing scripts)
# shellcheck disable=SC2034
RED='\033[0;31m'
# shellcheck disable=SC2034
GREEN='\033[0;32m'
# shellcheck disable=SC2034
BLUE='\033[0;34m'
# shellcheck disable=SC2034
NC='\033[0m'

# Read config from samconfig.toml
read_config() {
    local key="$1"
    local default="$2"
    if [ -f "$SAMCONFIG" ]; then
        local value
        value=$(grep "^${key} = " "$SAMCONFIG" 2>/dev/null | \
            sed 's/.*= "//' | sed 's/".*$//' | \
            sed "s/.*= '//" | sed "s/'.*$//")
        if [ -n "$value" ]; then
            echo "$value"
            return
        fi
    fi
    echo "$default"
}

STACK_NAME=$(read_config "stack_name" "supaflow-salesforce-to-s3")
REGION=$(read_config "region" "us-east-1")
PROFILE=$(read_config "profile" "")

# Build profile argument for AWS CLI (only if profile is set and not "default")
AWS_PROFILE_ARGS=()
if [ -n "$PROFILE" ] && [ "$PROFILE" != "default" ]; then
    AWS_PROFILE_ARGS=(--profile "$PROFILE")
fi

# Get State Machine ARN from CloudFormation outputs
get_state_machine_arn() {
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        "${AWS_PROFILE_ARGS[@]}" \
        --query "Stacks[0].Outputs[?OutputKey==\`StateMachineArn\`].OutputValue" \
        --output text 2>/dev/null
}

STATE_MACHINE_ARN=$(get_state_machine_arn)

if [ -z "$STATE_MACHINE_ARN" ] || [ "$STATE_MACHINE_ARN" = "None" ]; then
    echo -e "${RED}Error: Could not find State Machine ARN.${NC}"
    echo "Make sure the stack '$STACK_NAME' is deployed."
    exit 1
fi
