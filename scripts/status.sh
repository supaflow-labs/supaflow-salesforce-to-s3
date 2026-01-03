#!/bin/bash
# shellcheck disable=SC1091
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_common.sh"

# Get RunControlTable name and State Machine ARN
RUN_CONTROL_TABLE=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  "${AWS_PROFILE_ARGS[@]}" \
  --query "Stacks[0].Outputs[?OutputKey==\`RunControlTable\`].OutputValue" \
  --output text 2>/dev/null)

STATE_MACHINE_ARN=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  "${AWS_PROFILE_ARGS[@]}" \
  --query "Stacks[0].Outputs[?OutputKey==\`StateMachineArn\`].OutputValue" \
  --output text 2>/dev/null)

GLUE_CRAWLER_NAME=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  "${AWS_PROFILE_ARGS[@]}" \
  --query "Stacks[0].Outputs[?OutputKey==\`GlueCrawlerName\`].OutputValue" \
  --output text 2>/dev/null)

if [ -z "$RUN_CONTROL_TABLE" ] || [ "$RUN_CONTROL_TABLE" = "None" ]; then
    echo "RunControlTable not found. Deploy first."
    exit 1
fi

echo "Recent runs for: $STACK_NAME"
echo ""

# Get recent Step Functions executions (to cross-reference status)
SF_EXECUTIONS=$(aws stepfunctions list-executions \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --max-results 10 \
  --region "$REGION" \
  "${AWS_PROFILE_ARGS[@]}" \
  --output json 2>/dev/null || echo '{"executions":[]}')

# Query last 3 runs using GSI1 (sorted by started_at descending)
aws dynamodb query \
  --table-name "$RUN_CONTROL_TABLE" \
  --index-name "gsi1-runs-by-time-v2" \
  --key-condition-expression "gsi1pk = :runs" \
  --expression-attribute-values '{":runs":{"S":"RUNS"}}' \
  --no-scan-index-forward \
  --limit 3 \
  --region "$REGION" \
  "${AWS_PROFILE_ARGS[@]}" \
  --output json 2>/dev/null | SF_EXECUTIONS="$SF_EXECUTIONS" python3 -c '
import json
import sys
import os

data = json.load(sys.stdin)
items = data.get("Items", [])

# Parse Step Functions executions from environment
sf_data = json.loads(os.environ.get("SF_EXECUTIONS", "{}"))
sf_executions = {e.get("name", ""): e.get("status", "") for e in sf_data.get("executions", [])}

if not items:
    print("No runs found.")
    sys.exit(0)

def get_str(item, key):
    return (item.get(key) or {}).get("S", "")

def get_num(item, key):
    val = (item.get(key) or {}).get("N", "0")
    return int(val) if val else 0

for i, item in enumerate(items):
    if i > 0:
        print("")

    status = get_str(item, "status")
    run_id = get_str(item, "run_id")
    started_at = get_str(item, "started_at")
    ended_at = get_str(item, "ended_at")
    error = get_str(item, "error")
    execution_arn = get_str(item, "execution_arn")
    crawler_started_at = get_str(item, "crawler_started_at")

    total = get_num(item, "total_objects")
    completed = get_num(item, "completed_objects")
    failed = get_num(item, "failed_objects")
    skipped = get_num(item, "skipped_objects")
    running = get_num(item, "running_objects")
    queued = get_num(item, "queued_objects")
    pages = get_num(item, "pages_fetched")
    bytes_written = get_num(item, "bytes_written")
    records = get_num(item, "records_fetched")
    polls = get_num(item, "poll_loops")

    # Check Step Functions status for this run
    sf_status = None
    for exec_name, exec_status in sf_executions.items():
        if run_id in exec_name or exec_name in (execution_arn or ""):
            sf_status = exec_status
            break

    # If DynamoDB shows IN_PROGRESS but Step Functions shows FAILED, show SF status
    display_status = status
    if status == "IN_PROGRESS" and sf_status in ("FAILED", "TIMED_OUT", "ABORTED"):
        display_status = f"{status} (SF: {sf_status})"

    # Status color
    if status == "IN_PROGRESS" and sf_status in ("FAILED", "TIMED_OUT", "ABORTED"):
        color = "\033[0;31m"  # Red for SF failure
    elif status == "IN_PROGRESS":
        color = "\033[0;34m"
    elif status == "COMPLETED":
        color = "\033[0;32m"
    elif status in ("FAILED", "RATE_LIMITED"):
        color = "\033[0;31m"
    else:
        color = "\033[0m"
    reset = "\033[0m"

    print(f"Run: {run_id}")
    print(f"  Status:  {color}{display_status}{reset}")
    print(f"  Started: {started_at}")
    if ended_at:
        print(f"  Ended:   {ended_at}")

    if total > 0 or status == "IN_PROGRESS":
        print(f"  Objects: {completed}/{total} completed, {failed} failed, {skipped} skipped")
        if running > 0 or queued > 0:
            print(f"           {running} running, {queued} queued")

    if total > 0:
        mb = bytes_written / (1024 * 1024)
        print(f"  Data:    {records:,} records, {pages} pages, {mb:.2f} MB")

    if polls > 0:
        print(f"  Polls:   {polls}")

    if crawler_started_at:
        print(f"  Crawler: started at {crawler_started_at}")

    if error:
        # Truncate long errors
        if len(error) > 100:
            error = error[:100] + "..."
        print(f"  Error:   {error}")
'

# Show Glue Crawler status
if [ -n "$GLUE_CRAWLER_NAME" ] && [ "$GLUE_CRAWLER_NAME" != "None" ]; then
    echo ""
    echo "Glue Crawler: $GLUE_CRAWLER_NAME"

    CRAWLER_INFO=$(aws glue get-crawler \
      --name "$GLUE_CRAWLER_NAME" \
      --region "$REGION" \
      "${AWS_PROFILE_ARGS[@]}" \
      --output json 2>/dev/null || echo '{}')

    echo "$CRAWLER_INFO" | python3 -c '
import json
import sys

data = json.load(sys.stdin)
crawler = data.get("Crawler", {})

if not crawler:
    print("  Status: Not found")
    sys.exit(0)

state = crawler.get("State", "UNKNOWN")
last_crawl = crawler.get("LastCrawl", {})

# State color
if state == "RUNNING":
    color = "\033[0;34m"  # Blue
elif state == "READY":
    color = "\033[0;32m"  # Green
else:
    color = "\033[0;33m"  # Yellow
reset = "\033[0m"

print(f"  State:   {color}{state}{reset}")

if last_crawl:
    status = last_crawl.get("Status", "")
    start_time = last_crawl.get("StartTime", "")
    tables_created = last_crawl.get("TablesCreated", 0)
    tables_updated = last_crawl.get("TablesUpdated", 0)
    partitions_created = last_crawl.get("PartitionsCreated", 0)
    partitions_updated = last_crawl.get("PartitionsUpdated", 0)

    if status == "SUCCEEDED":
        status_color = "\033[0;32m"
    elif status == "FAILED":
        status_color = "\033[0;31m"
    else:
        status_color = "\033[0m"

    print(f"  Last:    {status_color}{status}{reset}")
    if start_time:
        # Format: 2026-01-02T20:54:59-08:00
        print(f"  Started: {start_time}")

    if tables_created or tables_updated or partitions_created or partitions_updated:
        print(f"  Changes: {tables_created} tables created, {tables_updated} updated")
        print(f"           {partitions_created} partitions created, {partitions_updated} updated")

    error_msg = last_crawl.get("ErrorMessage", "")
    if error_msg:
        if len(error_msg) > 80:
            error_msg = error_msg[:80] + "..."
        print(f"  Error:   {error_msg}")
'
fi
