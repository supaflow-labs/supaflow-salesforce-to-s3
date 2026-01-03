#!/bin/bash
# shellcheck disable=SC1091
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_common.sh"

echo "This will delete ALL state from DynamoDB:"
echo "  - Run lock and status"
echo "  - Object last_run timestamps (forces full re-sync)"
echo "  - Any pagination checkpoints"
echo ""
echo -e "${RED}WARNING: Next run will do a FULL sync of all objects!${NC}"
echo ""

# Get table name and bucket name
TABLE_NAME=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  "${AWS_PROFILE_ARGS[@]}" \
  --query "Stacks[0].Outputs[?OutputKey==\`RunStateTable\`].OutputValue" \
  --output text 2>/dev/null)

BUCKET_NAME=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  "${AWS_PROFILE_ARGS[@]}" \
  --query "Stacks[0].Outputs[?OutputKey==\`OutputBucketName\`].OutputValue" \
  --output text 2>/dev/null)

if [ -z "$TABLE_NAME" ] || [ "$TABLE_NAME" = "None" ]; then
    echo -e "${RED}Error: Could not find state table.${NC}"
    exit 1
fi

echo "Table:  $TABLE_NAME"
echo "Bucket: $BUCKET_NAME"
echo ""

# Count items
ITEM_COUNT=$(aws dynamodb scan \
  --table-name "$TABLE_NAME" \
  --region "$REGION" \
  "${AWS_PROFILE_ARGS[@]}" \
  --select COUNT \
  --query "Count" \
  --output text 2>/dev/null)

echo "Found $ITEM_COUNT item(s) in DynamoDB."
echo ""

echo -n "Delete DynamoDB state? [y/N]: "
read -r CONFIRM

if [[ "$CONFIRM" =~ ^[Yy] ]]; then
    if [ "$ITEM_COUNT" -gt 0 ]; then
        echo "Deleting items (batch mode)..."

        # Use batch-write-item for faster deletion (25 items at a time)
        DELETED=0
        BATCH_SIZE=25

        while true; do
            # Scan for items
            RESULT=$(aws dynamodb scan \
              --table-name "$TABLE_NAME" \
              --region "$REGION" \
              "${AWS_PROFILE_ARGS[@]}" \
              --projection-expression "pk" \
              --limit "$BATCH_SIZE" \
              --output json 2>/dev/null)

            # Extract PKs
            PKS=$(echo "$RESULT" | python3 -c '
import json, sys
data = json.load(sys.stdin)
for item in data.get("Items", []):
    print(item["pk"]["S"])
' 2>/dev/null)

            if [ -z "$PKS" ]; then
                break
            fi

            # Build batch delete request
            REQUESTS=""
            for pk in $PKS; do
                if [ -n "$REQUESTS" ]; then
                    REQUESTS="$REQUESTS,"
                fi
                REQUESTS="$REQUESTS{\"DeleteRequest\":{\"Key\":{\"pk\":{\"S\":\"$pk\"}}}}"
            done

            # Execute batch delete
            aws dynamodb batch-write-item \
              --region "$REGION" \
              "${AWS_PROFILE_ARGS[@]}" \
              --request-items "{\"$TABLE_NAME\":[$REQUESTS]}" > /dev/null 2>&1

            COUNT=$(echo "$PKS" | wc -l | tr -d ' ')
            DELETED=$((DELETED + COUNT))
            echo "  Deleted $DELETED items..."
        done

        echo -e "${GREEN}Deleted $DELETED item(s) from DynamoDB.${NC}"
    else
        echo "No items to delete."
    fi
else
    echo "Skipped DynamoDB reset."
fi

echo ""

# Optionally clear S3 bucket
if [ -n "$BUCKET_NAME" ] && [ "$BUCKET_NAME" != "None" ]; then
    # Note: KeyCount isn't always present in the response, count Contents instead
    OBJECT_COUNT=$(aws s3api list-objects-v2 \
      --bucket "$BUCKET_NAME" \
      --region "$REGION" \
      "${AWS_PROFILE_ARGS[@]}" \
      --query "length(Contents || \`[]\`)" \
      --output text 2>/dev/null || echo "0")

    if [ "$OBJECT_COUNT" != "0" ] && [ "$OBJECT_COUNT" != "None" ]; then
        echo "Found $OBJECT_COUNT object(s) in S3 bucket."
        echo -n "Delete all S3 objects? [y/N]: "
        read -r CONFIRM_S3

        if [[ "$CONFIRM_S3" =~ ^[Yy] ]]; then
            echo "Deleting S3 objects..."
            aws s3 rm "s3://$BUCKET_NAME" --recursive \
              --region "$REGION" \
              "${AWS_PROFILE_ARGS[@]}" 2>&1 | tail -5
            echo -e "${GREEN}S3 bucket cleared.${NC}"
        else
            echo "Skipped S3 cleanup."
        fi
    else
        echo "S3 bucket is empty."
    fi
fi

echo ""
echo -e "${GREEN}Reset complete.${NC}"
echo "Next execution will perform a full sync of all objects."
