#!/bin/bash
# shellcheck disable=SC1091
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_common.sh"

# Find running executions
RUNNING=$(aws stepfunctions list-executions \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --region "$REGION" \
  "${AWS_PROFILE_ARGS[@]}" \
  --status-filter RUNNING \
  --query 'executions[].executionArn' \
  --output text)

if [ -z "$RUNNING" ] || [ "$RUNNING" = "None" ]; then
    echo "No running executions found."
    exit 0
fi

# Count running executions
COUNT=$(echo "$RUNNING" | wc -w | tr -d ' ')

echo "Found $COUNT running execution(s)."
echo ""

for ARN in $RUNNING; do
    NAME=$(basename "$ARN")
    echo -n "Stop $NAME? [y/N]: "
    read -r CONFIRM

    if [[ "$CONFIRM" =~ ^[Yy] ]]; then
        aws stepfunctions stop-execution \
          --execution-arn "$ARN" \
          --region "$REGION" \
          "${AWS_PROFILE_ARGS[@]}" \
          --cause "Stopped by user via stop.sh" > /dev/null

        echo -e "${GREEN}Stopped.${NC}"

        # Also clear the DynamoDB run lock
        echo -n "Clear DynamoDB run lock? [Y/n]: "
        read -r CLEAR_LOCK

        if [[ ! "$CLEAR_LOCK" =~ ^[Nn] ]]; then
            TABLE_NAME=$(aws cloudformation describe-stacks \
              --stack-name "$STACK_NAME" \
              --region "$REGION" \
              "${AWS_PROFILE_ARGS[@]}" \
              --query "Stacks[0].Outputs[?OutputKey==\`RunStateTable\`].OutputValue" \
              --output text 2>/dev/null)

            if [ -n "$TABLE_NAME" ] && [ "$TABLE_NAME" != "None" ]; then
                aws dynamodb delete-item \
                  --table-name "$TABLE_NAME" \
                  --region "$REGION" \
                  "${AWS_PROFILE_ARGS[@]}" \
                  --key '{"pk": {"S": "RUN"}}' 2>/dev/null || true
                echo -e "${GREEN}Run lock cleared.${NC}"
            fi
        fi
    else
        echo "Skipped."
    fi
    echo ""
done

echo "Done."
