#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
DEFAULT_STACK_NAME="supaflow-salesforce-to-s3"
DEFAULT_REGION="us-east-1"
DEFAULT_S3_PREFIX="supaflow-salesforce-to-s3"
DEFAULT_SF_USERNAME_PARAM="/salesforce/username"
DEFAULT_SF_PASSWORD_PARAM="/salesforce/password"
DEFAULT_SF_TOKEN_PARAM="/salesforce/security_token"
DEFAULT_OUTPUT_PREFIX="salesforce-export"
DEFAULT_SCHEDULE="rate(1 day)"
DEFAULT_SF_LOGIN_URL="https://login.salesforce.com"

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SAMCONFIG_FILE="$SCRIPT_DIR/samconfig.toml"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Supaflow Salesforce Lambda Deployer${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------

print_ok() {
    echo -e "${GREEN}[OK]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

prompt_with_default() {
    local prompt="$1"
    local default="$2"
    local result

    if [ -n "$default" ]; then
        read -r -p "$prompt [$default]: " result
        echo "${result:-$default}"
    else
        read -r -p "$prompt: " result
        echo "$result"
    fi
}

prompt_secret() {
    local prompt="$1"
    local result
    read -r -s -p "$prompt: " result
    echo ""
    echo "$result"
}

prompt_yes_no() {
    local prompt="$1"
    local default="$2"
    local result

    if [ "$default" = "y" ]; then
        read -r -p "$prompt [Y/n]: " result
        result="${result:-y}"
    else
        read -r -p "$prompt [y/N]: " result
        result="${result:-n}"
    fi

    [[ "$result" =~ ^[Yy] ]]
}

sanitize_bucket_name() {
    local name="$1"
    name=$(echo "$name" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9.-]/-/g')
    name=$(echo "$name" | sed 's/--*/-/g' | sed 's/^-*//;s/-*$//')
    if [ ${#name} -lt 3 ]; then
        name="salesforce-export"
    fi
    if [ ${#name} -gt 63 ]; then
        name=$(echo "$name" | cut -c1-63 | sed 's/-*$//')
    fi
    echo "$name"
}

# Read value from samconfig.toml if it exists
read_samconfig_value() {
    local key="$1"
    local default="$2"
    local value

    if [ -f "$SAMCONFIG_FILE" ]; then
        # Extract value, strip quotes, and remove inline comments
        value=$(grep "^${key} = " "$SAMCONFIG_FILE" 2>/dev/null | \
            sed 's/.*= "//' | sed 's/".*$//' | \
            sed "s/.*= '//" | sed "s/'.*$//")
        if [ -n "$value" ]; then
            echo "$value"
            return
        fi
    fi
    echo "$default"
}

# -----------------------------------------------------------------------------
# Step 1: Check prerequisites
# -----------------------------------------------------------------------------

echo -e "${YELLOW}Step 1: Checking prerequisites...${NC}"
echo ""

# Check SAM CLI
if command -v sam &> /dev/null; then
    SAM_VERSION=$(sam --version 2>&1 | head -1)
    print_ok "SAM CLI: $SAM_VERSION"
else
    print_error "SAM CLI not found. Install with: brew install aws-sam-cli"
    exit 1
fi

# Check AWS CLI
if command -v aws &> /dev/null; then
    AWS_VERSION=$(aws --version 2>&1 | awk '{print $1}')
    print_ok "AWS CLI: $AWS_VERSION"
else
    print_error "AWS CLI not found. Install with: brew install awscli"
    exit 1
fi

echo ""

# -----------------------------------------------------------------------------
# Step 2: Configure AWS profile
# -----------------------------------------------------------------------------

echo -e "${YELLOW}Step 2: Configuring AWS profile...${NC}"
echo ""

# Get current/default profile
CURRENT_PROFILE="${AWS_PROFILE:-default}"
SAVED_PROFILE=$(read_samconfig_value "profile" "$CURRENT_PROFILE")

AWS_PROFILE_TO_USE=$(prompt_with_default "AWS profile" "$SAVED_PROFILE")
export AWS_PROFILE="$AWS_PROFILE_TO_USE"

# Verify profile works
echo ""
print_info "Verifying AWS credentials..."
if CALLER_IDENTITY=$(aws sts get-caller-identity 2>&1); then
    ACCOUNT_ID=$(echo "$CALLER_IDENTITY" | grep -o '"Account": "[^"]*"' | cut -d'"' -f4)
    USER_ARN=$(echo "$CALLER_IDENTITY" | grep -o '"Arn": "[^"]*"' | cut -d'"' -f4)
    print_ok "AWS Account: $ACCOUNT_ID"
    print_ok "Identity: $USER_ARN"
else
    print_error "Failed to verify AWS credentials for profile '$AWS_PROFILE_TO_USE'"
    print_error "$CALLER_IDENTITY"
    exit 1
fi

echo ""

# -----------------------------------------------------------------------------
# Step 3: Configure deployment settings
# -----------------------------------------------------------------------------

echo -e "${YELLOW}Step 3: Configuring deployment settings...${NC}"
echo ""

SAVED_STACK_NAME=$(read_samconfig_value "stack_name" "$DEFAULT_STACK_NAME")
SAVED_REGION=$(read_samconfig_value "region" "$DEFAULT_REGION")
SAVED_S3_PREFIX=$(read_samconfig_value "s3_prefix" "$DEFAULT_S3_PREFIX")

STACK_NAME=$(prompt_with_default "Stack name" "$SAVED_STACK_NAME")
REGION=$(prompt_with_default "AWS region" "$SAVED_REGION")
S3_PREFIX=$(prompt_with_default "S3 prefix for SAM artifacts" "$SAVED_S3_PREFIX")
DEFAULT_OUTPUT_BUCKET=$(sanitize_bucket_name "salesforce-export-${ACCOUNT_ID}-${REGION}")

echo ""

# -----------------------------------------------------------------------------
# Step 4: Check/configure Salesforce credentials in SSM
# -----------------------------------------------------------------------------

echo -e "${YELLOW}Step 4: Checking Salesforce credentials in SSM...${NC}"
echo ""

check_ssm_param() {
    local param_name="$1"
    aws ssm get-parameter --name "$param_name" --with-decryption --region "$REGION" &> /dev/null
}

create_or_update_ssm_param() {
    local param_name="$1"
    local param_value="$2"
    local exists="$3"

    if [ "$exists" = "true" ]; then
        aws ssm put-parameter --name "$param_name" --type "SecureString" --value "$param_value" --overwrite --region "$REGION" > /dev/null
    else
        aws ssm put-parameter --name "$param_name" --type "SecureString" --value "$param_value" --region "$REGION" > /dev/null
    fi
}

# Check each SSM parameter
SF_USERNAME_EXISTS="false"
SF_PASSWORD_EXISTS="false"
SF_TOKEN_EXISTS="false"

if check_ssm_param "$DEFAULT_SF_USERNAME_PARAM"; then
    print_ok "$DEFAULT_SF_USERNAME_PARAM exists"
    SF_USERNAME_EXISTS="true"
else
    print_warn "$DEFAULT_SF_USERNAME_PARAM not found"
fi

if check_ssm_param "$DEFAULT_SF_PASSWORD_PARAM"; then
    print_ok "$DEFAULT_SF_PASSWORD_PARAM exists"
    SF_PASSWORD_EXISTS="true"
else
    print_warn "$DEFAULT_SF_PASSWORD_PARAM not found"
fi

if check_ssm_param "$DEFAULT_SF_TOKEN_PARAM"; then
    print_ok "$DEFAULT_SF_TOKEN_PARAM exists"
    SF_TOKEN_EXISTS="true"
else
    print_warn "$DEFAULT_SF_TOKEN_PARAM not found"
fi

echo ""

# Prompt to create/update credentials if any are missing
if [ "$SF_USERNAME_EXISTS" = "false" ] || [ "$SF_PASSWORD_EXISTS" = "false" ] || [ "$SF_TOKEN_EXISTS" = "false" ]; then
    echo "Some Salesforce credentials are missing in SSM."
    if prompt_yes_no "Create/update Salesforce credentials now?" "y"; then
        echo ""
        SF_USERNAME=$(prompt_with_default "Salesforce username" "")
        SF_PASSWORD=$(prompt_secret "Salesforce password")
        SF_TOKEN=$(prompt_secret "Salesforce security token")

        echo ""
        print_info "Saving credentials to SSM..."

        if [ -n "$SF_USERNAME" ]; then
            create_or_update_ssm_param "$DEFAULT_SF_USERNAME_PARAM" "$SF_USERNAME" "$SF_USERNAME_EXISTS"
            print_ok "Saved $DEFAULT_SF_USERNAME_PARAM"
        fi

        if [ -n "$SF_PASSWORD" ]; then
            create_or_update_ssm_param "$DEFAULT_SF_PASSWORD_PARAM" "$SF_PASSWORD" "$SF_PASSWORD_EXISTS"
            print_ok "Saved $DEFAULT_SF_PASSWORD_PARAM"
        fi

        if [ -n "$SF_TOKEN" ]; then
            create_or_update_ssm_param "$DEFAULT_SF_TOKEN_PARAM" "$SF_TOKEN" "$SF_TOKEN_EXISTS"
            print_ok "Saved $DEFAULT_SF_TOKEN_PARAM"
        fi
    else
        print_warn "Skipping SSM parameter creation. Deployment may fail if parameters don't exist."
    fi
else
    if prompt_yes_no "Update existing Salesforce credentials?" "n"; then
        echo ""
        echo "Leave any field empty to keep current value."
        echo ""
        SF_USERNAME=$(prompt_with_default "Salesforce username" "")
        SF_PASSWORD=$(prompt_secret "Salesforce password")
        SF_TOKEN=$(prompt_secret "Salesforce security token")

        echo ""

        if [ -n "$SF_USERNAME" ] || [ -n "$SF_PASSWORD" ] || [ -n "$SF_TOKEN" ]; then
            print_info "Updating credentials in SSM..."

            if [ -n "$SF_USERNAME" ]; then
                create_or_update_ssm_param "$DEFAULT_SF_USERNAME_PARAM" "$SF_USERNAME" "true"
                print_ok "Updated $DEFAULT_SF_USERNAME_PARAM"
            fi

            if [ -n "$SF_PASSWORD" ]; then
                create_or_update_ssm_param "$DEFAULT_SF_PASSWORD_PARAM" "$SF_PASSWORD" "true"
                print_ok "Updated $DEFAULT_SF_PASSWORD_PARAM"
            fi

            if [ -n "$SF_TOKEN" ]; then
                create_or_update_ssm_param "$DEFAULT_SF_TOKEN_PARAM" "$SF_TOKEN" "true"
                print_ok "Updated $DEFAULT_SF_TOKEN_PARAM"
            fi
        else
            print_info "No credentials to update."
        fi
    fi
fi

echo ""

# -----------------------------------------------------------------------------
# Step 5: Configure output bucket and optional parameters
# -----------------------------------------------------------------------------

echo -e "${YELLOW}Step 5: Output bucket and optional parameters...${NC}"
echo ""

# Use array for parameter overrides to handle spaces correctly
PARAM_OVERRIDES=()

OUTPUT_BUCKET_INPUT=$(prompt_with_default "Output S3 bucket (leave blank or use default shown)" "$DEFAULT_OUTPUT_BUCKET")
OUTPUT_BUCKET_INPUT_LOWER=$(echo "$OUTPUT_BUCKET_INPUT" | tr '[:upper:]' '[:lower:]')
if [ -z "$OUTPUT_BUCKET_INPUT" ] || [ "$OUTPUT_BUCKET_INPUT" = "-" ] || [ "$OUTPUT_BUCKET_INPUT_LOWER" = "$DEFAULT_OUTPUT_BUCKET" ]; then
    OUTPUT_BUCKET=""
    print_info "Using default output bucket name."
else
    OUTPUT_BUCKET="$OUTPUT_BUCKET_INPUT"
    # Check if bucket exists
    if ! aws s3api head-bucket --bucket "$OUTPUT_BUCKET" --region "$REGION" 2>/dev/null; then
        print_warn "Bucket '$OUTPUT_BUCKET' does not exist."
        if prompt_yes_no "Create bucket '$OUTPUT_BUCKET' now?" "y"; then
            echo "Creating bucket..."
            if [ "$REGION" = "us-east-1" ]; then
                aws s3api create-bucket --bucket "$OUTPUT_BUCKET" --region "$REGION"
            else
                aws s3api create-bucket --bucket "$OUTPUT_BUCKET" --region "$REGION" \
                    --create-bucket-configuration LocationConstraint="$REGION"
            fi
            aws s3api wait bucket-exists --bucket "$OUTPUT_BUCKET" --region "$REGION"
            print_info "Configuring bucket security settings..."
            aws s3api put-bucket-encryption \
                --bucket "$OUTPUT_BUCKET" \
                --region "$REGION" \
                --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
            aws s3api put-public-access-block \
                --bucket "$OUTPUT_BUCKET" \
                --region "$REGION" \
                --public-access-block-configuration 'BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true'
            print_ok "Bucket created."
        else
            print_error "Bucket does not exist. Lambda writes will fail."
            echo "Please create the bucket manually or re-run with '-' to auto-create."
            exit 1
        fi
    else
        print_ok "Bucket '$OUTPUT_BUCKET' exists."
    fi
fi

# Set OutputBucketName parameter (empty string = let CloudFormation create)
PARAM_OVERRIDES=()
if [ -n "$OUTPUT_BUCKET" ]; then
    PARAM_OVERRIDES=("OutputBucketName=$OUTPUT_BUCKET")
else
    # Explicitly pass empty to trigger CreateBucket condition
    PARAM_OVERRIDES=('OutputBucketName=""')
fi

if prompt_yes_no "Configure optional parameters (prefix, schedule, login URL, etc.)?" "n"; then
    echo ""
    OUTPUT_PREFIX=$(prompt_with_default "Output prefix" "$DEFAULT_OUTPUT_PREFIX")
    SCHEDULE=$(prompt_with_default "Schedule expression" "$DEFAULT_SCHEDULE")
    SF_LOGIN_URL=$(prompt_with_default "Salesforce login URL" "$DEFAULT_SF_LOGIN_URL")
    echo ""
    echo "Full refresh interval for objects without cursor fields:"
    echo "  -1 = Skip these objects"
    echo "   0 = Full refresh every run"
    echo "  >0 = Minutes between full refreshes (e.g., 2880 = 48 hours)"
    echo "  Default = 1440 (once per day)"
    FULL_REFRESH_INTERVAL=$(prompt_with_default "Full refresh interval (minutes)" "1440")
    echo ""
    echo "Object allowlist (comma-separated):"
    echo "  Default syncs 20 core CRM objects: Account, Contact, Lead, etc."
    echo "  Enter 'ALL' to sync ALL queryable objects."
    OBJECT_ALLOWLIST=$(prompt_with_default "Object allowlist (or ALL)" "Account,Contact,Lead,Opportunity,Case,User,UserRole,Profile,Group,GroupMember,PermissionSet,PermissionSetAssignment,Product2,Pricebook2,PricebookEntry,Document,EmailTemplate,Solution,Entitlement,BusinessHours")
    # Convert ALL sentinel to empty string
    if [ "${OBJECT_ALLOWLIST^^}" = "ALL" ]; then
        OBJECT_ALLOWLIST=""
    fi

    PARAM_OVERRIDES+=(
        "OutputPrefix=$OUTPUT_PREFIX"
        "ScheduleExpression=$SCHEDULE"
        "SalesforceLoginUrl=$SF_LOGIN_URL"
        "FullRefreshIntervalMinutes=$FULL_REFRESH_INTERVAL"
        "ObjectAllowlist=$OBJECT_ALLOWLIST"
    )
fi

echo ""

# -----------------------------------------------------------------------------
# Step 6: Write samconfig.toml
# -----------------------------------------------------------------------------

echo -e "${YELLOW}Step 6: Updating samconfig.toml...${NC}"
echo ""

cat > "$SAMCONFIG_FILE" << EOF
version = 0.1

[default.deploy.parameters]
stack_name = "$STACK_NAME"
resolve_s3 = true
s3_prefix = "$S3_PREFIX"
region = "$REGION"
profile = "$AWS_PROFILE_TO_USE"
confirm_changeset = false
capabilities = "CAPABILITY_IAM"
EOF

print_ok "Updated $SAMCONFIG_FILE"
echo ""

# -----------------------------------------------------------------------------
# Step 7: Build and deploy
# -----------------------------------------------------------------------------

echo -e "${YELLOW}Step 7: Building and deploying...${NC}"
echo ""

print_info "Running: sam build"
echo ""
cd "$SCRIPT_DIR"
sam build

echo ""
print_info "Running: sam deploy"
echo ""

if [ ${#PARAM_OVERRIDES[@]} -gt 0 ]; then
    sam deploy --parameter-overrides "${PARAM_OVERRIDES[@]}"
else
    sam deploy
fi

echo ""

# -----------------------------------------------------------------------------
# Step 8: Show outputs and next steps
# -----------------------------------------------------------------------------

echo -e "${YELLOW}Step 8: Deployment complete!${NC}"
echo ""

# Get stack outputs
print_info "Fetching stack outputs..."
echo ""

OUTPUTS=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" --query 'Stacks[0].Outputs' --output json 2>/dev/null || echo "[]")

STATE_MACHINE_ARN=$(echo "$OUTPUTS" | grep -A1 '"OutputKey": "StateMachineArn"' | grep '"OutputValue"' | sed 's/.*: "//' | sed 's/".*//' || echo "")
STATE_TABLE=$(echo "$OUTPUTS" | grep -A1 '"OutputKey": "RunStateTable"' | grep '"OutputValue"' | sed 's/.*: "//' | sed 's/".*//' || echo "")
OUTPUT_BUCKET_NAME=$(echo "$OUTPUTS" | grep -A1 '"OutputKey": "OutputBucketName"' | grep '"OutputValue"' | sed 's/.*: "//' | sed 's/".*//' || echo "")

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Deployment Successful!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Resources created:"
echo "  State Machine:  $STATE_MACHINE_ARN"
echo "  DynamoDB Table: $STATE_TABLE"
echo "  S3 Bucket:      $OUTPUT_BUCKET_NAME"
echo ""

# Offer to start manual execution
if [ -n "$STATE_MACHINE_ARN" ]; then
    echo -e "${BLUE}----------------------------------------${NC}"
    echo ""
    if prompt_yes_no "Start a manual execution now?" "n"; then
        echo ""
        print_info "Starting execution..."
        EXECUTION=$(aws stepfunctions start-execution --state-machine-arn "$STATE_MACHINE_ARN" --region "$REGION" 2>&1)
        EXECUTION_ARN=$(echo "$EXECUTION" | grep -o '"executionArn": "[^"]*"' | cut -d'"' -f4)

        if [ -n "$EXECUTION_ARN" ]; then
            print_ok "Execution started!"
            echo ""
            echo "Execution ARN: $EXECUTION_ARN"
            echo ""
            echo "Monitor in AWS Console:"
            echo "  https://${REGION}.console.aws.amazon.com/states/home?region=${REGION}#/executions/details/${EXECUTION_ARN}"
        else
            print_error "Failed to start execution"
            echo "$EXECUTION"
        fi
    else
        echo ""
        echo "To start a manual execution later:"
        echo ""
        echo "  aws stepfunctions start-execution \\"
        echo "    --state-machine-arn \"$STATE_MACHINE_ARN\" \\"
        echo "    --region \"$REGION\""
    fi
fi

echo ""
echo -e "${GREEN}Done!${NC}"
