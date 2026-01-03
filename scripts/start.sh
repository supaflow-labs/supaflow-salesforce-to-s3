#!/bin/bash
# shellcheck disable=SC1091
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_common.sh"

echo "Starting execution..."

RESULT=$(aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --region "$REGION" \
  "${AWS_PROFILE_ARGS[@]}")

EXECUTION_ARN=$(echo "$RESULT" | grep -o '"executionArn": "[^"]*"' | cut -d'"' -f4)
EXECUTION_NAME=$(basename "$EXECUTION_ARN")

echo ""
echo "Execution started!"
echo "  Name: $EXECUTION_NAME"
echo "  ARN:  $EXECUTION_ARN"
echo ""
echo "Monitor with:"
echo "  $SCRIPT_DIR/status.sh"
echo ""
echo "Or view in AWS Console:"
echo "  https://${REGION}.console.aws.amazon.com/states/home?region=${REGION}#/v2/executions/details/${EXECUTION_ARN}"
