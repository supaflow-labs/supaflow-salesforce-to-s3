
## Table of Contents

- [What this does](#what-this-does)
- [Why you'll like this](#why-youll-like-this)
- [Quick Start](#quick-start-guided-deployment)
- [How it works](#architecture)
- [Configuration](#configuration)
- [Object Allowlist](#object-allowlist)
- [Object and Field Filtering](#object-and-field-filtering)
- [Incremental Logic](#incremental-logic)
- [Full Refresh for No-Cursor Objects](#full-refresh-for-no-cursor-objects)
- [Security](#security)
- [Costs](#costs)
- [Querying with Athena](#querying-with-athena)
- [Operational guide](#operational-guide)
- [Troubleshooting](#troubleshooting)
- [Local Testing](#local-testing)
- [Advanced Configuration](#advanced-configuration)
- [FAQ](#faq)
- [License](#license)


# Salesforce to S3 (Lambda + Step Functions)

Exports every queryable Salesforce object to S3 as CSV on a schedule using **Salesforce Bulk API 2.0**,
**AWS Lambda**, and **AWS Step Functions**.

- A daily/hourly schedule only **starts** a run.
- The run itself is orchestrated by Step Functions until completion.
- Output is partitioned for easy downstream consumption (Athena/Glue/Spark/Databricks).

If you want a “batteries included” Salesforce → S3 export with minimal infrastructure and sane defaults,
this repo is meant to be copy/paste deployable.

## Why you'll like this

You can go from **zero** to **queryable Salesforce tables** with just a couple commands.

- **10-minute setup:** run `./install.sh` to deploy everything (Lambdas + Step Functions + DynamoDB + schedule + optional S3 bucket).
- **One command to run:** `./scripts/start.sh` kicks off an export immediately.
- **Instant Athena/Glue readiness:** after each successful export, a Glue Crawler updates the **Glue Data Catalog** so tables and partitions stay current.
- **Always append-only + partitioned:** each run writes to `run=<run_id>` partitions so you can query point-in-time snapshots or build “latest” views.
- **Rate-limit friendly:** if Salesforce throttles you, the run exits as `RATE_LIMITED` (controlled) and the next run resumes without advancing cursors.

**Outcome:** after your first run completes, you can open Athena and run:

```sql
SELECT * FROM salesforce_export.account_raw LIMIT 10;
```

…and start building temporary views for “latest row per id” using the patterns in this README.

## Key Features

- **Incremental sync**: Uses `SystemModstamp`, then `LastModifiedDate`, then `CreatedDate` if filterable
- **Glue-compatible partitioning**: Outputs to `s3://<bucket>/<prefix>/<object>_raw/run=<run_id>/part-00001.csv.gz`
- **State management**: DynamoDB tracks run state and last successful run per object
- **No overlap**: Only one run active at a time; scheduled triggers skip if a run is in progress
- **Auto-recovery**: Stale runs (default 24h) are automatically taken over
- **Rate limit handling**: Exits as `RATE_LIMITED` (controlled), so the next schedule resumes without advancing cursors
- **Retry with backoff**: Transient failures (5xx) retried with exponential backoff
- **Parallel processing**: Batch job submission (15 concurrent), parallel fetching (4 concurrent)
- **Empty result skipping**: No S3 files written for objects with 0 records in incremental window

## What this does

**Does**
- Bulk-export Salesforce objects to S3 on a schedule
- Supports incremental sync when a filterable cursor exists
- Handles Salesforce rate limits gracefully (exits as `RATE_LIMITED` so the next run can resume)
- Tracks per-object state in DynamoDB so failures don’t poison the whole pipeline

**Does not**
- Transform data (exports raw CSV)
- Guarantee exactly-once semantics across re-runs (designed for idempotent, partitioned exports)

**Automatic schema discovery**
- Glue Crawler runs automatically after each **successful** export to keep schemas and partitions updated
- Creates tables named `<object>_raw` (e.g., `account_raw`, `contact_raw`) in the `salesforce_export` database
- Adds a new `run=<run_id>` partition every run for point-in-time queries
- Works immediately with Athena / Glue / Spark without any manual DDL

## Architecture

The Step Functions workflow uses a batch polling pattern for efficiency:

**Why Step Functions?** It keeps Lambdas short-lived and cheap (submit / poll / fetch), avoids long-running
workers, and makes retries + state visible in the console.

```
AcquireLock
    ↓
StartJobs (Map, MaxConcurrency=15)
    → Submit Bulk API jobs for all objects in parallel
    ↓
PollLoop:
    CheckJobsBatch (single Lambda)
        → Check status of all pending jobs
    ↓
    FetchCompleted (Map, MaxConcurrency=4)
        → Fetch results for completed jobs in parallel
    ↓
    HasPending? → Yes: Wait 10s → PollLoop
                → No: Continue
    ↓
ReleaseLock
    ↓
StartGlueCrawler
    → Update Glue Data Catalog with new tables/partitions
```

After a successful export, Step Functions triggers the Glue Crawler to update schemas and partitions automatically.

**Benefits over sequential processing:**
- Jobs submitted in parallel (up to 15, Salesforce limit)
- Single shared poll interval across all jobs
- Results fetched in parallel as jobs complete
- Significantly faster for large object counts

## Prerequisites

- AWS SAM CLI
- Python 3.11+
- AWS credentials with permissions to create IAM roles/policies (required for SAM deploy)
- A Salesforce org with API access enabled (Bulk API 2.0)
- Salesforce credentials (username, password, security token)

## Quick Start (Guided Deployment)

The easiest way to deploy is using the interactive deployment script:

```bash
./install.sh
```

This script will:
1. Check prerequisites (SAM CLI, AWS CLI)
2. Verify your AWS credentials
3. Create/update Salesforce credentials in SSM Parameter Store
4. Configure deployment settings
5. Set a default output bucket name (`salesforce-export-<account>-<region>`) unless you override it
6. Build and deploy the Lambda + Step Functions
7. Optionally start a test execution

After the first successful run, the Glue Crawler populates the `salesforce_export` database so you can query immediately in Athena.

### Non-Interactive Deployment

For CI/CD or scripted deployments, you can run `install.sh` without user input:

```bash
# Accept all defaults (auto-create bucket, skip optional params)
printf '\n\n\n\nn\n\nn\n' | ./install.sh

# Order of prompts answered:
#   1. AWS profile (Enter = default)
#   2. Stack name (Enter = default)
#   3. Region (Enter = default)
#   4. S3 prefix (Enter = default)
#   5. Update credentials? (n = no)
#   6. Output bucket (Enter = auto-create)
#   7. Optional params? (n = no)
#   8. Start execution? (n = no)
```

Or use `yes ""` to accept all defaults including starting an execution:

```bash
yes "" | ./install.sh
```

## Managing Executions

Helper scripts for managing Step Functions executions:

```bash
# Start a new execution
./scripts/start.sh

# Check status of recent executions (shows DynamoDB run state too)
./scripts/status.sh

# Stop a running execution (with option to clear run lock)
./scripts/stop.sh

# Reset all state (forces full re-sync on next run)
./scripts/reset.sh

# Completely uninstall (deletes stack and state tables; deletes S3 bucket only if auto-created)
./scripts/uninstall.sh
```

### Non-Interactive Script Usage

All management scripts read configuration from `samconfig.toml` and run without prompts:

```bash
# Start execution (no prompts)
./scripts/start.sh

# Check status (no prompts)
./scripts/status.sh

# Reset with auto-confirm (DynamoDB only, skip S3)
echo -e "y\nn" | ./scripts/reset.sh

# Reset with auto-confirm (DynamoDB and S3)
echo -e "y\ny" | ./scripts/reset.sh

# Uninstall with auto-confirm (requires typing "DELETE")
echo "DELETE" | ./scripts/uninstall.sh
```

---

## Manual AWS Deployment

If you prefer manual deployment, follow these steps:

### Step 1: Install Prerequisites

```bash
# Install AWS SAM CLI (macOS)
brew install aws-sam-cli

# Or on Linux
pip install aws-sam-cli

# Verify installation
sam --version
```

### Step 2: Configure AWS Credentials

Ensure you have AWS credentials configured:

```bash
# Option A: Use default profile
aws configure

# Option B: Use named profile
aws configure --profile your-profile-name
```

### Step 3: Store Salesforce Credentials in SSM

Store your Salesforce credentials as SecureString parameters:

```bash
# Set your AWS profile (if using named profile)
export AWS_PROFILE=your-profile-name

# Create SSM parameters
aws ssm put-parameter \
  --name "/salesforce/username" \
  --type "SecureString" \
  --value "your-salesforce-username@example.com"

aws ssm put-parameter \
  --name "/salesforce/password" \
  --type "SecureString" \
  --value "your-salesforce-password"

aws ssm put-parameter \
  --name "/salesforce/security_token" \
  --type "SecureString" \
  --value "your-salesforce-security-token"
```

To update existing parameters, add `--overwrite`:
```bash
aws ssm put-parameter --name "/salesforce/username" --type "SecureString" --value "new-value" --overwrite
```

### Step 4: Configure SAM Deployment

Edit `samconfig.toml` to match your environment:

```toml
version = 0.1

[default.deploy.parameters]
stack_name = "supaflow-salesforce-to-s3"
resolve_s3 = true
s3_prefix = "supaflow-salesforce-to-s3"
region = "us-east-1"
profile = "default"
confirm_changeset = false
capabilities = "CAPABILITY_IAM"
```

### Step 5: Build and Deploy

```bash
# Build the SAM application
sam build

# Deploy to AWS (uses samconfig.toml settings)
sam deploy

# Or deploy with parameter overrides
sam deploy --parameter-overrides \
  OutputBucketName=your-existing-bucket \
  OutputPrefix=salesforce-export \
  ScheduleExpression="rate(1 day)"
```

To create a new S3 bucket automatically, leave `OutputBucketName` empty:
```bash
sam deploy --parameter-overrides OutputBucketName=""
```

### Step 6: Verify Deployment

After deployment, SAM outputs the created resources:

```
Outputs
---------------------------------------------------------------------------
Key                 StateMachineArn
Value               arn:aws:states:us-east-1:123456789:stateMachine:...

Key                 RunStateTable
Value               supaflow-salesforce-to-s3-StateTable-XXXXX

Key                 OutputBucketName
Value               supaflow-salesforce-to-s3-outputbucket-XXXXX

Key                 GlueDatabaseName
Value               salesforce_export

Key                 GlueCrawlerName
Value               supaflow-salesforce-to-s3-crawler
```

### Step 7: Start a Manual Execution (Optional)

To test immediately without waiting for the schedule:

```bash
# Get the State Machine ARN from outputs
STATE_MACHINE_ARN="arn:aws:states:us-east-1:123456789:stateMachine:ExportStateMachine-XXXXX"

# Start execution
aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --profile your-profile-name

# Monitor in AWS Console: Step Functions > State machines > ExportStateMachine
```

### Updating the Deployment

After making code changes:

```bash
sam build && sam deploy
```

## Configuration

Use the parameters below for the core behavior, and see **Advanced Configuration** for retries/KMS/custom domains.
Non-secret settings are SAM parameters; secrets live in SSM SecureString.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `OutputBucketName` | (created) | S3 bucket for exports. Leave empty to create one; if provided, bucket must exist in the same region |
| `OutputPrefix` | `salesforce-export` | S3 key prefix |
| `ScheduleExpression` | `rate(1 day)` | EventBridge schedule |
| `SalesforceLoginUrl` | `https://login.salesforce.com` | Login URL (or sandbox/custom domain) |
| `SalesforceApiVersion` | `63.0` | Salesforce API version |
| `RunLockTtlHours` | `24` | Stale run threshold (hours) |
| `MaxRecordsPerPage` | `10000` | Bulk API 2.0 page size |
| `PollSeconds` | `10` | Job status poll interval |
| `LookbackSeconds` | `0` | Overlap window for incremental runs |
| `FullRefreshIntervalMinutes` | `1440` | Full refresh interval for no-cursor objects (see below) |
| `ObjectAllowlist` | (20 core objects) | Comma-separated list of objects to sync (empty = all) |

## Object Allowlist

By default, only 20 core CRM objects are synced:

```
Account, Contact, Lead, Opportunity, Case, User, UserRole, Profile,
Group, GroupMember, PermissionSet, PermissionSetAssignment, Product2,
Pricebook2, PricebookEntry, Document, EmailTemplate, Solution,
Entitlement, BusinessHours
```

This keeps test runs fast and avoids hitting Salesforce API limits.

**To sync all objects:**
```bash
sam deploy --parameter-overrides ObjectAllowlist=""
# Or in interactive install.sh, enter: ALL
```

**To sync specific objects:**
```bash
sam deploy --parameter-overrides ObjectAllowlist="Account,Contact,Lead,Opportunity"
```

**To add objects to the default list:**
```bash
sam deploy --parameter-overrides ObjectAllowlist="Account,Contact,Lead,Opportunity,Case,User,UserRole,Profile,Group,GroupMember,PermissionSet,PermissionSetAssignment,Product2,Pricebook2,PricebookEntry,Document,EmailTemplate,Solution,Entitlement,BusinessHours,FieldPermissions,ObjectPermissions"
```

Object names are case-insensitive. The allowlist does not override the skip filters below.

## Object and Field Filtering

### Skipped Objects

Objects are filtered in two stages: pre-filtering (before describe) and post-filtering (after describe).

#### Pre-Filter Rules (by name pattern)

| Pattern | Reason |
|---------|--------|
| `*ChangeEvent` | CDC event streams, not bulk-queryable |
| `*__b` | BigObjects (different query syntax) |
| `*__x` | External objects (different data source) |
| `*__hd` | Historical data objects |
| `*__Tag` tied to Custom Settings | Tag objects for custom settings |
| `AccountUserTerritory2View` | Known performance issues |

#### Bulk API Unsupported Objects (18)

These objects return errors when queried via Bulk API 2.0:

```
AcceptedEventRelation, CaseStatus, ContractStatus, DeclinedEventRelation,
FieldSecurityClassification, OrderStatus, PartnerRole, RecentlyViewed,
ServiceAppointmentStatus, ShiftStatus, SolutionStatus, TaskPriority,
TaskStatus, UndecidedEventRelation, WorkOrderLineItemStatus, WorkOrderStatus,
WorkStepStatus
```

#### Query-Restricted Objects (35)

These objects require special query conditions or have access restrictions:

```
Announcement, AppTabMember, ApexTypeImplementor, CollaborationGroupRecord,
ColorDefinition, ContentDocumentLink, ContentFolderItem, ContentFolderMember,
DatacloudAddress, DatacloudDandBCompany, DataStatistics, EntityParticle,
FieldDefinition, FieldHistoryArchive, FlexQueueItem, FlowTestView,
FlowVariableView, FlowVersionView, IconDefinition, IdeaComment,
NetworkUserHistoryRecent, OwnerChangeOptionInfo, PicklistValueInfo,
PlatformAction, RecordActionHistory, RelatedListColumnDefinition,
RelatedListDefinition, RelationshipDomain, RelationshipInfo, SearchLayout,
SiteDetail, UserEntityAccess, UserFieldAccess, Vote
```

#### Query-Incompatible Objects (25)

These objects cannot be queried directly or return incompatible data:

```
ActivityHistory, AggregateResult, AttachedContentDocument, AttachedContentNote,
CombinedAttachment, ContentBody, DataType, EmailStatus, FeedLike,
FeedSignal, FeedTrackedChange, FolderedContentDocument, ListViewChartInstance,
LookedUpFromActivity, Name, NoteAndAttachment, OpenActivity, OutgoingEmail,
OutgoingEmailRelation, OwnedContentDocument, ProcessInstanceHistory,
QuoteTemplateRichTextData, UserRecordAccess
```

#### Post-Filter Rules (after describe)

Objects are also skipped if their metadata indicates:
- Not queryable (`queryable: false`)
- Not retrieveable (`retrieveable: false`)
- Deprecated and hidden (`deprecatedAndHidden: true`)

### Skipped Fields

| Condition | Reason |
|-----------|--------|
| `deprecatedAndHidden: true` | Field is deprecated |
| Type `base64` | Binary data not supported in CSV |
| Type `complexvalue` | Compound field type |
| Type `address` | Compound address (use individual fields) |
| Type `location` | Geolocation compound field |
| `EntityDefinition.RecordTypesSupported` | Known problematic field |

## Incremental Logic

Uses **cutoff-anchored incremental windows** for deterministic sync behavior.

### Cursor Field Selection

When starting a job for an object, the handler selects a cursor field in priority order:

| Priority | Field | Description |
|----------|-------|-------------|
| 1 | `SystemModstamp` | System-managed, updated on any change (preferred) |
| 2 | `LastModifiedDate` | User-facing, updated on record modifications |
| 3 | `CreatedDate` | Fallback for append-only objects |

The first **filterable** field from this list is used. If none are filterable, the object has no cursor and follows the [Full Refresh](#full-refresh-for-no-cursor-objects) logic.

**Why this order?**
- `SystemModstamp` captures all changes including system-triggered updates
- `LastModifiedDate` may miss some automated changes
- `CreatedDate` only captures new records, not updates

### Query Window Behavior

1. **Run cutoff**: The `run_started_at` timestamp anchors all objects in a run
2. **Window bounds**:
   - Initial sync: `WHERE cursor < cutoff`
   - Incremental sync: `WHERE cursor >= (last_run - lookback) AND cursor < cutoff`
3. On success, stores `run_started_at` as the object's `last_run` (the cutoff becomes the next lower bound)
4. Use `LookbackSeconds` to overlap windows (catches late-arriving updates)

**Why cutoff anchoring matters:**
- All objects in a run use the same upper bound, preventing drift
- No missed records: deterministic windows with optional lookback overlap
- Failures safe: failed objects keep old `last_run`, so they re-query on next run

## Full Refresh for No-Cursor Objects

Some Salesforce objects don't have a filterable timestamp field (`SystemModstamp`, `LastModifiedDate`, or `CreatedDate`). By default, these objects are full refreshed every 24 hours.

The `FullRefreshIntervalMinutes` parameter controls how to handle these objects (default: `1440`):

| Value | Behavior |
|-------|----------|
| `-1` | Skip no-cursor objects |
| `0` | Full refresh every run |
| `> 0` | Full refresh only if last full refresh is older than N minutes |

**Examples:**

```bash
# Default behavior (1440): full refresh once per day - no override needed

# Skip no-cursor objects entirely
sam deploy --parameter-overrides FullRefreshIntervalMinutes=-1

# Full refresh no-cursor objects every run
sam deploy --parameter-overrides FullRefreshIntervalMinutes=0

# Full refresh no-cursor objects every 48 hours
sam deploy --parameter-overrides FullRefreshIntervalMinutes=2880
```

**How it works:**

1. When a no-cursor object is encountered, the handler checks `FULL_REFRESH_INTERVAL_MINUTES`
2. If the interval is `-1`, the object is skipped with `missing_cursor_field`
3. If the interval is `0`, a full refresh (no WHERE clause) runs every time
4. If the interval is `> 0`, the handler checks `last_full_refresh_at` in DynamoDB
   against the run start time, with a 5% grace window:
   - If missing or older than the interval (minus 5%): run full refresh
   - Otherwise: skip with `full_refresh_not_due`
5. On successful full refresh, `last_full_refresh_at` is updated

**Tracking:**

- RunControlTable stores `refresh_mode` per object (`incremental` or `full_refresh`)
- StateTable stores `last_full_refresh_at` for scheduling future full refreshes

## Security

- Salesforce credentials are stored in SSM Parameter Store as `SecureString` values.
- S3 bucket is encrypted (SSE-S3) and blocked from public access by default.
- IAM policies are scoped to the deployed stack resources.
- Do not commit `.env` files or secrets; use `.env` locally (git-ignored) or SSM in AWS.

## Costs

Primary cost drivers:
- **Lambda**: total runtime across submit/poll/fetch steps
- **Step Functions**: state transitions per run
- **S3**: storage + PUTs for exported CSVs
- **DynamoDB**: run/object state writes
- **EventBridge**: schedule trigger
- **SSM**: parameter storage (low cost)

Cost scales with object count, run frequency, and data volume. Start with daily runs and adjust as needed.

## Querying with Athena

After deployment, a Glue Crawler automatically creates tables in the `salesforce_export` database.

### Table Structure

Each Salesforce object creates a table named `<object>_raw`:
- `account_raw` - Account records
- `contact_raw` - Contact records
- `lead_raw` - Lead records
- etc.

Tables are partitioned by `run` (the export timestamp).

### Query Examples

```sql
-- List all tables
SHOW TABLES IN salesforce_export;

-- Query all data from a table
SELECT * FROM salesforce_export.account_raw LIMIT 10;

-- Query specific partition (point-in-time)
SELECT * FROM salesforce_export.account_raw
WHERE run = '20260103T042422Z';

-- Get latest version of each record (deduplicated by id)
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY run DESC) as _row_num
  FROM salesforce_export.account_raw
) WHERE _row_num = 1;

-- Count records per run
SELECT run, COUNT(*) as record_count
FROM salesforce_export.account_raw
GROUP BY run
ORDER BY run DESC;
```

### Automatic Schema Updates

The Glue Crawler runs automatically at the end of each successful export:
- Discovers new tables for newly exported objects
- Adds new partitions for each run
- Updates column types if schema changes

To run the crawler manually (use GlueCrawlerName from stack outputs):
```bash
aws glue start-crawler --name supaflow-salesforce-to-s3-crawler
```

## Operational Guide

### What happens on Salesforce rate limits

- **Before submitting jobs**: the run exits early and is recorded as `RATE_LIMITED`. No jobs are submitted.
- **During job polling**: the state machine records `RATE_LIMITED` and exits early.
- **During result fetch**: the run is marked `RATE_LIMITED`, fetch errors are recorded, and the status is preserved (not overwritten by `COMPLETED`).

**Recovery behavior**
- `RATE_LIMITED` is not an error - it's a controlled exit so the next schedule resumes without advancing cursors.
- The next scheduled run attempts again once limits reset.
- Per-object cursors only advance on success, so failed objects re-run safely.
- Check `scripts/status.sh` and CloudWatch logs for details.

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Transient API error (5xx) | Retry with exponential backoff + jitter |
| HTTP 429 (Too Many Requests) | Mark run `RATE_LIMITED`, exit gracefully |
| Rate limit exceeded | Mark run `RATE_LIMITED`, exit gracefully |
| Job failure | Mark run `FAILED`, next schedule starts fresh |
| Stale run (no heartbeat) | Next schedule takes over the lock |

## Troubleshooting

### Execution Fails with "JSONPath could not be found"

**Symptom:** Step Functions execution fails immediately with an error like:
```
The JSONPath '$.Payload.run_id' specified for the field 'run_id' could not be found
```

**Cause:** A previous run is still marked as `IN_PROGRESS` in DynamoDB, so `acquire_lock` returns `skip: true` with a minimal payload.

**Solution:** Delete the stale run lock from DynamoDB:
```bash
# Find your table name (from SAM outputs)
TABLE_NAME="supaflow-salesforce-to-s3-StateTable-XXXXX"

# Delete the RUN record
aws dynamodb delete-item \
  --table-name "$TABLE_NAME" \
  --key '{"pk": {"S": "RUN"}}' \
  --profile your-profile-name
```

### Execution Skips with "run_in_progress"

**Symptom:** Execution succeeds but skips immediately, logs show "run_in_progress".

**Cause:** Same as above - a previous run is still active or stale.

**Solution:** Either wait for the previous run to complete, or delete the stale lock (see above).

### Salesforce Rate Limit Errors

**Symptom:** Execution ends with status `RATE_LIMITED`.

**Cause:** Salesforce API quota exceeded.

**Solution:**
- Wait for quota to reset (usually 24 hours)
- The next scheduled run will resume from where it left off
- Consider reducing `MaxRecordsPerPage` or spreading runs across time

### Bulk API Job Failures

**Symptom:** Execution fails with `SalesforceJobFailed`.

**Cause:** Salesforce Bulk API job failed or was aborted.

**Solutions:**
- Check Salesforce Setup > Bulk Data Load Jobs for error details
- Some objects may have complex queries that timeout
- The object will be retried on the next run

### Lambda Timeout

**Symptom:** Lambda times out after 15 minutes.

**Cause:** Large result sets or slow Salesforce API response.

**Solutions:**
- Reduce `MaxRecordsPerPage` to process smaller chunks
- The Step Functions workflow handles pagination, so partial progress is saved

### Viewing Execution Details

```bash
# List recent executions
aws stepfunctions list-executions \
  --state-machine-arn "arn:aws:states:REGION:ACCOUNT:stateMachine:NAME" \
  --max-results 10 \
  --profile your-profile-name

# Get execution details
aws stepfunctions describe-execution \
  --execution-arn "arn:aws:states:REGION:ACCOUNT:execution:NAME:EXECUTION-ID" \
  --profile your-profile-name

# Get execution history (all state transitions)
aws stepfunctions get-execution-history \
  --execution-arn "arn:aws:states:REGION:ACCOUNT:execution:NAME:EXECUTION-ID" \
  --profile your-profile-name
```

### Checking DynamoDB State

```bash
# View all state records
aws dynamodb scan \
  --table-name "supaflow-salesforce-to-s3-StateTable-XXXXX" \
  --profile your-profile-name

# View specific object state
aws dynamodb get-item \
  --table-name "supaflow-salesforce-to-s3-StateTable-XXXXX" \
  --key '{"pk": {"S": "OBJ#Account"}}' \
  --profile your-profile-name
```

---

## Local Testing

Tests can run locally without deploying to Lambda. The test suite covers:
- Salesforce authentication and Bulk API access
- Object/field filtering logic
- Incremental cursor selection
- Query building with lookback
- End-to-end Bulk API read + S3 write

### Setup

```bash
cd /path/to/supaflow-salesforce-to-s3

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements-dev.txt

# Configure credentials
cp .env.example .env
# Edit .env with your Salesforce credentials
```

### Configure Credentials

Copy `.env.example` to `.env` and fill in your values:

```bash
# .env (git-ignored)
SF_USERNAME=your-username@example.com
SF_PASSWORD=your-password
SF_TOKEN=your-security-token
AWS_PROFILE=your-aws-profile  # for E2E tests
```

The test scripts automatically load credentials from `.env`.

### Run Unit Tests

Tests Salesforce logic without AWS resources:

```bash
source .venv/bin/activate
python tests/unit/test_local.py
```

(Credentials are loaded from `.env` automatically)

**What it tests:**
- Login to Salesforce
- Bulk API access verification
- Object listing and pre-filtering (blacklists)
- Object describe and field filtering
- Cursor field selection
- Query building (full sync and incremental)
- Lookback window calculation

**Sample output:**
```
============================================================
TEST: Object Listing & Pre-filtering
============================================================
  Total queryable objects: 735
  After pre-filter:        685 objects
  Pre-filtered out:        50 objects
  [PASS]
```

### Run End-to-End Tests

Two E2E test options are available:

#### Option 1: Quick E2E (tests/e2e/test_e2e.py)

Tests Bulk API read and S3 write with direct Salesforce calls (bypasses SSM):

```bash
source .venv/bin/activate

# Single object test (Account, 100 records)
python tests/e2e/test_e2e.py

# Multi-object test (Account, Contact, Lead)
python tests/e2e/test_e2e.py --multi

# Keep test bucket for inspection (default: auto-cleanup)
python tests/e2e/test_e2e.py --keep-bucket
```

#### Option 2: Full E2E (tests/e2e/local_e2e_runner.py) - Recommended

Simulates the complete Step Functions workflow with real AWS resources (DynamoDB, S3, SSM):

```bash
source .venv/bin/activate

# First, create SSM parameters (one-time setup):
aws ssm put-parameter --name "/salesforce/username" --type "SecureString" --value "your-user"
aws ssm put-parameter --name "/salesforce/password" --type "SecureString" --value "your-pass"
aws ssm put-parameter --name "/salesforce/security_token" --type "SecureString" --value "your-token"

# Run with specific objects
python tests/e2e/local_e2e_runner.py --objects Account,Lead

# Run with max objects limit
python tests/e2e/local_e2e_runner.py --max-objects 5

# Keep resources for inspection (default: auto-cleanup)
python tests/e2e/local_e2e_runner.py --objects Account --keep-resources

# Use existing resources
python tests/e2e/local_e2e_runner.py --use-existing-table my-table --use-existing-bucket my-bucket
```

**What it validates:**
- DynamoDB state transitions (IN_PROGRESS → COMPLETED)
- Object `last_run` and `incremental_field` updates
- S3 files exist with correct Hive-style partitioning
- Run lock acquisition and release
- Same code paths as Lambda/Step Functions

**Sample output:**
```
[DynamoDB State]
  Run status: COMPLETED
  Run ID: 20260101T045017Z
  [PASS] Run state correct

[S3 Output]
  Account: 1 files, 491983 bytes
  Lead: 1 files, 29899 bytes

[PASS] E2E TEST COMPLETE
```

### Test Coverage Matrix

| Component | tests/unit/test_local.py | tests/e2e/test_e2e.py | tests/e2e/local_e2e_runner.py |
|-----------|:------------------------:|:---------------------:|:-----------------------------:|
| Salesforce login | Yes | Yes | Yes |
| Bulk API access check | Yes | Yes | Yes |
| Object listing | Yes | - | Yes |
| Object pre-filtering | Yes | - | Yes |
| Field filtering | Yes | Yes | Yes |
| Cursor selection | Yes | Yes | Yes |
| Query building | Yes | Yes | Yes |
| Lookback window | Yes | - | Yes |
| Bulk API query execution | - | Yes | Yes |
| S3 upload streaming | - | Yes | Yes |
| S3 data verification | - | Yes | Yes |
| DynamoDB state management | - | - | Yes |
| Run lock acquire/release | - | - | Yes |
| Object last_run tracking | - | - | Yes |
| SSM parameter loading | - | - | Yes |
| Full Step Functions flow | - | - | Yes |

---

## Advanced Configuration

### Retry Tuning

Set via environment variables in `template.yaml`:

```yaml
HTTP_MAX_RETRIES: "3"        # Max retry attempts
HTTP_RETRY_BASE_SECONDS: "1" # Initial backoff delay
HTTP_RETRY_MAX_SECONDS: "30" # Max backoff delay
```

### Custom KMS Key

If SSM parameters use a custom KMS key, add to the Lambda role:

```yaml
- Effect: Allow
  Action: kms:Decrypt
  Resource: arn:aws:kms:REGION:ACCOUNT:key/KEY-ID
```

### Sandbox/Custom Domain

- Use `SalesforceLoginUrl` to target a sandbox or custom domain.
- Update the parameter and redeploy.
- Examples:

```bash
sam deploy --parameter-overrides SalesforceLoginUrl="https://test.salesforce.com"
# or
sam deploy --parameter-overrides SalesforceLoginUrl="https://mycompany.my.salesforce.com"
```

## Repo Layout

```
.
├── src/
│   ├── handler.py          # Lambda handler + business logic
│   ├── salesforce.py       # Salesforce API client
│   ├── state_store.py      # DynamoDB state management
│   └── requirements.txt    # Python dependencies
├── tests/
│   ├── unit/
│   │   └── test_local.py   # Unit tests (Salesforce logic only)
│   └── e2e/
│       ├── test_e2e.py     # Quick E2E (Bulk API + S3)
│       └── local_e2e_runner.py  # Full E2E (simulates Step Functions)
├── scripts/
│   ├── start.sh            # Start a new execution
│   ├── status.sh           # Check execution status
│   ├── stop.sh             # Stop running execution
│   ├── reset.sh            # Reset all state (full re-sync)
│   └── uninstall.sh        # Delete entire stack and all resources
├── statemachine.asl.yaml   # Step Functions definition
├── template.yaml           # SAM/CloudFormation template
├── samconfig.toml          # SAM deployment configuration
├── install.sh              # Interactive deployment script
├── requirements-dev.txt    # Local dev/test dependencies
├── .env.example            # Template for credentials
├── .gitignore
└── README.md
```

## FAQ

**Does this overwrite previous exports?**  
No. Each run writes to a unique `run=<run_id>` partition, so exports are append-only.

**How do I force a full re-sync?**  
Run `./scripts/reset.sh` to clear state so the next run re-exports everything.

**Why was an object skipped?**  
Check the skip reason in logs (`missing_cursor_field`, `not_queryable`, `full_refresh_not_due`, etc.).

**How do I change the schedule?**  
Update `ScheduleExpression` and redeploy, or edit the EventBridge rule in the console.

**Where are logs?**  
CloudWatch log group for the Lambda function, plus Step Functions execution history.

## License

MIT License. See `LICENSE`.
