#!/bin/bash
# shellcheck disable=SC1091
set -e
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_common.sh"

echo -e "${RED}========================================${NC}"
echo -e "${RED}  Uninstall Salesforce Lambda Stack${NC}"
echo -e "${RED}========================================${NC}"
echo ""
echo "This will PERMANENTLY DELETE:"
echo "  - CloudFormation stack: $STACK_NAME"
echo "  - DynamoDB tables (StateTable, RunControlTable)"
echo "  - S3 bucket and all exported data"
echo "  - Lambda function and Step Functions state machine"
echo "  - IAM roles and EventBridge rules"
echo "  - SAM CLI deployment artifacts"
echo ""
echo -e "${RED}WARNING: This action cannot be undone!${NC}"
echo ""

# Check if stack exists
STACK_STATUS=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  "${AWS_PROFILE_ARGS[@]}" \
  --query "Stacks[0].StackStatus" \
  --output text 2>/dev/null || echo "DOES_NOT_EXIST")

if [ "$STACK_STATUS" = "DOES_NOT_EXIST" ]; then
    echo "Stack '$STACK_NAME' does not exist in region '$REGION'."
    exit 0
fi

echo "Stack status: $STACK_STATUS"
echo ""

# Get bucket name before deletion
BUCKET_NAME=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  "${AWS_PROFILE_ARGS[@]}" \
  --query "Stacks[0].Outputs[?OutputKey==\`OutputBucketName\`].OutputValue" \
  --output text 2>/dev/null || echo "")

echo -n "Type 'DELETE' to confirm permanent deletion: "
read -r CONFIRM

if [ "$CONFIRM" != "DELETE" ]; then
    echo "Aborted."
    exit 1
fi

echo ""

# Empty S3 bucket first (CloudFormation can't delete non-empty buckets)
BUCKET_EMPTY_FAILED=false
if [ -n "$BUCKET_NAME" ] && [ "$BUCKET_NAME" != "None" ]; then
    echo "Emptying S3 bucket: $BUCKET_NAME"

    # Check if bucket exists and we have access
    if aws s3api head-bucket --bucket "$BUCKET_NAME" "${AWS_PROFILE_ARGS[@]}" 2>/dev/null; then
        VERSIONING_STATUS=$(aws s3api get-bucket-versioning \
          --bucket "$BUCKET_NAME" \
          --region "$REGION" \
          "${AWS_PROFILE_ARGS[@]}" \
          --query "Status" \
          --output text 2>/dev/null || echo "Disabled")

        if [ "$VERSIONING_STATUS" = "Enabled" ] || [ "$VERSIONING_STATUS" = "Suspended" ]; then
            echo "Bucket versioning is $VERSIONING_STATUS. Deleting all versions and delete markers..."

            # Paginate through versions and delete in batches
            KEY_MARKER=""
            VERSION_MARKER=""
            DELETED_COUNT=0

            while true; do
                # Build pagination args
                PAGINATION_ARGS=()
                if [ -n "$KEY_MARKER" ]; then
                    PAGINATION_ARGS+=(--key-marker "$KEY_MARKER")
                    PAGINATION_ARGS+=(--version-id-marker "$VERSION_MARKER")
                fi

                # List one page of versions
                if ! PAGE=$(aws s3api list-object-versions \
                  --bucket "$BUCKET_NAME" \
                  --region "$REGION" \
                  "${AWS_PROFILE_ARGS[@]}" \
                  "${PAGINATION_ARGS[@]}" \
                  --max-items 1000 \
                  --output json 2>/dev/null); then
                    echo -e "${RED}Warning: Failed to list bucket versions${NC}"
                    BUCKET_EMPTY_FAILED=true
                    break
                fi

                # Extract objects to delete and pagination tokens
                READ_RESULT=$(echo "$PAGE" | python3 -c '
import json, sys
data = json.load(sys.stdin)
objects = []
for key in ("Versions", "DeleteMarkers"):
    for obj in data.get(key) or []:
        k, v = obj.get("Key"), obj.get("VersionId")
        if k is not None and v is not None:
            objects.append({"Key": k, "VersionId": v})
# Output: count, next_key_marker, next_version_marker, then batches
next_key = data.get("NextKeyMarker", "")
next_version = data.get("NextVersionIdMarker", "")
print(len(objects))
print(next_key)
print(next_version)
for i in range(0, len(objects), 100):
    print(json.dumps({"Objects": objects[i:i + 100]}))
')

                # Parse the output
                OBJ_COUNT=$(echo "$READ_RESULT" | sed -n '1p')
                KEY_MARKER=$(echo "$READ_RESULT" | sed -n '2p')
                VERSION_MARKER=$(echo "$READ_RESULT" | sed -n '3p')

                # Delete batches (lines 4+)
                while IFS= read -r batch; do
                    if [ -n "$batch" ]; then
                        if ! aws s3api delete-objects \
                          --bucket "$BUCKET_NAME" \
                          --region "$REGION" \
                          "${AWS_PROFILE_ARGS[@]}" \
                          --delete "$batch" > /dev/null 2>&1; then
                            echo -e "${RED}Warning: Failed to delete some objects${NC}"
                            BUCKET_EMPTY_FAILED=true
                        fi
                    fi
                done < <(echo "$READ_RESULT" | tail -n +4)

                DELETED_COUNT=$((DELETED_COUNT + OBJ_COUNT))
                echo "  Deleted $DELETED_COUNT objects..."

                # Check if there are more pages
                if [ -z "$KEY_MARKER" ]; then
                    break
                fi
            done
        else
            # Delete all objects when versioning is not enabled
            DELETE_OUTPUT=$(aws s3 rm "s3://$BUCKET_NAME" --recursive \
              --region "$REGION" \
              "${AWS_PROFILE_ARGS[@]}" 2>&1) || {
                echo -e "${RED}Warning: Failed to delete some objects${NC}"
                echo "$DELETE_OUTPUT" | tail -5
                BUCKET_EMPTY_FAILED=true
            }
        fi

        if [ "$BUCKET_EMPTY_FAILED" = "true" ]; then
            echo -e "${RED}Warning: Bucket may not be fully emptied.${NC}"
            echo -n "Continue with stack deletion anyway? [y/N]: "
            read -r CONTINUE_ANYWAY
            if [[ ! "$CONTINUE_ANYWAY" =~ ^[Yy] ]]; then
                echo "Aborted. Please empty the bucket manually and try again."
                exit 1
            fi
        else
            echo -e "${GREEN}S3 bucket emptied.${NC}"
        fi
    else
        echo "Bucket not accessible or already deleted."
    fi
fi

echo ""
echo "Deleting CloudFormation stack: $STACK_NAME"
echo "This may take a few minutes..."
echo ""

# Delete the stack
aws cloudformation delete-stack \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  "${AWS_PROFILE_ARGS[@]}"

# Wait for deletion to complete
echo "Waiting for stack deletion to complete..."
aws cloudformation wait stack-delete-complete \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  "${AWS_PROFILE_ARGS[@]}" 2>/dev/null || true

# Verify deletion
FINAL_STATUS=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  "${AWS_PROFILE_ARGS[@]}" \
  --query "Stacks[0].StackStatus" \
  --output text 2>/dev/null || echo "DELETED")

if [ "$FINAL_STATUS" = "DELETED" ] || [ "$FINAL_STATUS" = "DELETE_COMPLETE" ]; then
    # Clean up SAM CLI deployment artifacts
    SAM_BUCKET=$(aws s3 ls "${AWS_PROFILE_ARGS[@]}" 2>/dev/null | \
      awk '{print $3}' | grep '^aws-sam-cli-managed-default-samclisourcebucket-' | head -1)
    if [ -n "$SAM_BUCKET" ]; then
        echo ""
        echo "Cleaning up SAM CLI deployment artifacts..."
        if aws s3 rm "s3://$SAM_BUCKET/$STACK_NAME/" --recursive \
          "${AWS_PROFILE_ARGS[@]}" 2>/dev/null; then
            echo -e "${GREEN}SAM artifacts cleaned up.${NC}"
        else
            echo "No SAM artifacts found or already cleaned."
        fi
    fi

    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  Uninstall Complete${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo "All resources have been deleted."
    echo "You can now run ./install.sh to create a fresh installation."
else
    echo ""
    echo -e "${RED}Warning: Stack deletion may have failed.${NC}"
    echo "Status: $FINAL_STATUS"
    echo ""
    echo "Check the CloudFormation console for details:"
    echo "  https://$REGION.console.aws.amazon.com/cloudformation/home?region=$REGION#/stacks"
fi
