"""Run control table operations for tracking run and object state.

This module manages the RunControlTable which stores:
- Run summaries (RUN#<run_id>, SUMMARY)
- Per-object state (RUN#<run_id>, OBJ#<object_name>)

GSI indexes:
- gsi1-runs-by-time-v2: gsi1pk=RUNS, gsi1sk=started_at (recent runs)
"""

import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

_TABLE_NAME = os.environ.get("RUN_CONTROL_TABLE", "")
_DYNAMODB = boto3.resource("dynamodb")
_TABLE = _DYNAMODB.Table(_TABLE_NAME)

# Key prefixes
RUN_PREFIX = "RUN#"
SUMMARY_SK = "SUMMARY"
OBJ_PREFIX = "OBJ#"

# GSI keys
GSI1_PK_RUNS = "RUNS"

# GSI index names
GSI1_NAME = "gsi1-runs-by-time-v2"

# Status constants
STATUS_PENDING = "PENDING"
STATUS_RUNNING = "RUNNING"
STATUS_IN_PROGRESS = "IN_PROGRESS"
STATUS_COMPLETED = "COMPLETED"
STATUS_FAILED = "FAILED"
STATUS_SKIPPED = "SKIPPED"
STATUS_RATE_LIMITED = "RATE_LIMITED"

# TTL for run items (days)
RUN_TTL_DAYS = 30


def _iso_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ttl_timestamp(days=RUN_TTL_DAYS):
    """Return Unix timestamp for TTL expiration."""
    expires = datetime.now(timezone.utc) + timedelta(days=days)
    return int(expires.timestamp())


def create_run_summary(run_id, execution_arn=None, total_objects=0, cutoff_time=None):
    """Create a new run summary item.

    Called after acquire_lock succeeds and object list is known.

    Args:
        cutoff_time: The upper bound timestamp for incremental queries.
                     All objects in this run use this as their cutoff.
                     Defaults to now if not provided.
    """
    now = _iso_now()
    item = {
        "pk": f"{RUN_PREFIX}{run_id}",
        "sk": SUMMARY_SK,
        "run_id": run_id,
        "status": STATUS_IN_PROGRESS,
        "started_at": now,
        "cutoff_time": cutoff_time or now,
        "updated_at": now,
        "total_objects": total_objects,
        "queued_objects": total_objects,
        "running_objects": 0,
        "completed_objects": 0,
        "failed_objects": 0,
        "skipped_objects": 0,
        "bytes_written": 0,
        "pages_fetched": 0,
        "records_fetched": 0,
        "poll_loops": 0,
        "jobs_submitted": 0,
        "step_attempts": 0,
        "expires_at": _ttl_timestamp(),
        # GSI keys
        "gsi1pk": GSI1_PK_RUNS,
        "gsi1sk": now,  # started_at for sorting
    }
    if execution_arn:
        item["execution_arn"] = execution_arn
    _TABLE.put_item(Item=item)
    return item


def get_run_summary(run_id):
    """Get the run summary item."""
    resp = _TABLE.get_item(
        Key={"pk": f"{RUN_PREFIX}{run_id}", "sk": SUMMARY_SK}
    )
    return resp.get("Item")


def list_recent_runs(limit=10):
    """List recent runs using GSI1, sorted by started_at descending."""
    resp = _TABLE.query(
        IndexName=GSI1_NAME,
        KeyConditionExpression=Key("gsi1pk").eq(GSI1_PK_RUNS),
        ScanIndexForward=False,  # Descending order
        Limit=limit,
    )
    return resp.get("Items", [])


def update_run_status(run_id, status, error=None, cause=None):
    """Update run status and optionally set error/cause.

    Sets ended_at for terminal statuses.
    """
    now = _iso_now()
    update = "SET #status = :status, updated_at = :updated_at"
    values = {
        ":status": status,
        ":updated_at": now,
    }
    names = {"#status": "status"}

    if status in (STATUS_COMPLETED, STATUS_FAILED, STATUS_RATE_LIMITED, STATUS_SKIPPED):
        update += ", ended_at = :ended_at"
        values[":ended_at"] = now

    if error:
        update += ", #error = :error"
        values[":error"] = error
        names["#error"] = "error"

    if cause:
        update += ", cause = :cause"
        values[":cause"] = cause

    _TABLE.update_item(
        Key={"pk": f"{RUN_PREFIX}{run_id}", "sk": SUMMARY_SK},
        UpdateExpression=update,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def update_run_status_if_active(run_id, status, error=None, cause=None):
    """Update run status only if currently IN_PROGRESS.

    Returns True if updated, False if already terminal.
    """
    now = _iso_now()
    update = "SET #status = :status, updated_at = :updated_at"
    values = {
        ":status": status,
        ":updated_at": now,
        ":in_progress": STATUS_IN_PROGRESS,
    }
    names = {"#status": "status"}

    if status in (STATUS_COMPLETED, STATUS_FAILED, STATUS_RATE_LIMITED, STATUS_SKIPPED):
        update += ", ended_at = :ended_at"
        values[":ended_at"] = now

    if error:
        update += ", #error = :error"
        values[":error"] = error
        names["#error"] = "error"

    if cause:
        update += ", cause = :cause"
        values[":cause"] = cause

    try:
        _TABLE.update_item(
            Key={"pk": f"{RUN_PREFIX}{run_id}", "sk": SUMMARY_SK},
            UpdateExpression=update,
            ConditionExpression="#status = :in_progress",
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise


def increment_run_counters(run_id, **counters):
    """Increment run summary counters using ADD operations.

    Only updates if the summary item exists (has run_id attribute).
    This prevents creating partial items if create_run_summary failed.

    Supported counters:
    - queued_objects (use negative to decrement)
    - running_objects
    - completed_objects
    - failed_objects
    - skipped_objects
    - bytes_written
    - pages_fetched
    - poll_loops
    - jobs_submitted
    """
    if not counters:
        return

    now = _iso_now()
    update_parts = ["SET updated_at = :updated_at"]
    add_parts = []
    values = {":updated_at": now}

    for key, value in counters.items():
        if value == 0:
            continue
        placeholder = f":{key}"
        values[placeholder] = value
        add_parts.append(f"{key} {placeholder}")

    if add_parts:
        update_expr = update_parts[0] + " ADD " + ", ".join(add_parts)
    else:
        update_expr = update_parts[0]

    try:
        _TABLE.update_item(
            Key={"pk": f"{RUN_PREFIX}{run_id}", "sk": SUMMARY_SK},
            UpdateExpression=update_expr,
            ConditionExpression="attribute_exists(run_id)",
            ExpressionAttributeValues=values,
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            # Summary doesn't exist - skip silently to avoid partial items
            return
        raise


def create_object_state(run_id, object_name, status=STATUS_PENDING, job_id=None, incremental_field=None, refresh_mode=None):
    """Create or update an object state item.

    Called when starting a job for an object.

    Args:
        refresh_mode: "incremental" or "full_refresh" to indicate the sync mode.
    """
    now = _iso_now()
    item = {
        "pk": f"{RUN_PREFIX}{run_id}",
        "sk": f"{OBJ_PREFIX}{object_name}",
        "run_id": run_id,
        "object_name": object_name,
        "status": status,
        "started_at": now,
        "updated_at": now,
        "pages_fetched": 0,
        "bytes_written": 0,
        "expires_at": _ttl_timestamp(),
    }
    if job_id:
        item["job_id"] = job_id
    if incremental_field:
        item["incremental_field"] = incremental_field
    if refresh_mode:
        item["refresh_mode"] = refresh_mode
    _TABLE.put_item(Item=item)
    return item


def get_object_state(run_id, object_name):
    """Get a specific object state item."""
    resp = _TABLE.get_item(
        Key={"pk": f"{RUN_PREFIX}{run_id}", "sk": f"{OBJ_PREFIX}{object_name}"}
    )
    return resp.get("Item")


def get_all_object_states(run_id):
    """Get all object states for a run."""
    resp = _TABLE.query(
        KeyConditionExpression=Key("pk").eq(f"{RUN_PREFIX}{run_id}") & Key("sk").begins_with(OBJ_PREFIX)
    )
    return resp.get("Items", [])


def get_running_objects(run_id):
    """Get all objects with RUNNING status for a run.

    Returns list of dicts with job metadata needed for polling.
    """
    items = get_all_object_states(run_id)
    running = []
    for item in items:
        if item.get("status") == STATUS_RUNNING:
            running.append({
                "job_id": item.get("job_id"),
                "object_name": item.get("object_name"),
                "incremental_field": item.get("incremental_field"),
                "refresh_mode": item.get("refresh_mode"),
            })
    return running


def update_object_status(run_id, object_name, status, error=None, cause=None, last_locator=None):
    """Update object status and optionally set error/cause.

    Sets completed_at for terminal statuses.
    """
    now = _iso_now()
    update = "SET #status = :status, updated_at = :updated_at"
    values = {":status": status, ":updated_at": now}
    names = {"#status": "status"}

    if status in (STATUS_COMPLETED, STATUS_FAILED, STATUS_SKIPPED):
        update += ", completed_at = :completed_at"
        values[":completed_at"] = now

    if error:
        update += ", #error = :error"
        values[":error"] = error
        names["#error"] = "error"

    if cause:
        update += ", cause = :cause"
        values[":cause"] = cause

    if last_locator:
        update += ", last_locator = :last_locator"
        values[":last_locator"] = last_locator

    _TABLE.update_item(
        Key={"pk": f"{RUN_PREFIX}{run_id}", "sk": f"{OBJ_PREFIX}{object_name}"},
        UpdateExpression=update,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def increment_object_counters(run_id, object_name, pages_fetched=0, bytes_written=0, record_count=None):
    """Increment object counters using ADD operations.

    Also sets record_count if provided (not incremented, just set).
    """
    now = _iso_now()
    update = "SET updated_at = :updated_at"
    values = {":updated_at": now}
    add_parts = []

    # record_count is SET, not ADDed
    if record_count is not None:
        update += ", record_count = :records"
        values[":records"] = record_count

    if pages_fetched != 0:
        add_parts.append("pages_fetched :pages")
        values[":pages"] = pages_fetched

    if bytes_written != 0:
        add_parts.append("bytes_written :bytes")
        values[":bytes"] = bytes_written

    if add_parts:
        update += " ADD " + ", ".join(add_parts)

    _TABLE.update_item(
        Key={"pk": f"{RUN_PREFIX}{run_id}", "sk": f"{OBJ_PREFIX}{object_name}"},
        UpdateExpression=update,
        ExpressionAttributeValues=values,
    )


def record_skipped_object(run_id, object_name, reason):
    """Record an object that was skipped (e.g., not queryable).

    Creates object item with SKIPPED status and increments summary counter.
    """
    # Create object state
    now = _iso_now()
    _TABLE.put_item(Item={
        "pk": f"{RUN_PREFIX}{run_id}",
        "sk": f"{OBJ_PREFIX}{object_name}",
        "run_id": run_id,
        "object_name": object_name,
        "status": STATUS_SKIPPED,
        "skip_reason": reason,
        "started_at": now,
        "completed_at": now,
        "updated_at": now,
        "pages_fetched": 0,
        "bytes_written": 0,
        "expires_at": _ttl_timestamp(),
    })

    # Update summary counters
    increment_run_counters(run_id, queued_objects=-1, skipped_objects=1)


def start_object_job(run_id, object_name, job_id, incremental_field=None, refresh_mode=None):
    """Record that a job has been started for an object.

    Creates object item with RUNNING status and updates summary counters.

    Args:
        refresh_mode: "incremental" or "full_refresh" to indicate the sync mode.
    """
    # Create object state
    create_object_state(
        run_id, object_name,
        status=STATUS_RUNNING,
        job_id=job_id,
        incremental_field=incremental_field,
        refresh_mode=refresh_mode,
    )

    # Update summary counters
    increment_run_counters(run_id, queued_objects=-1, running_objects=1, jobs_submitted=1)


def complete_object_fetch(run_id, object_name, pages_fetched, bytes_written, record_count=-1):
    """Mark object as completed after successful fetch.

    Updates object status and counters, then summary counters.
    These are observability updates - failures are logged but don't block execution.
    record_count is stored at object level; -1 means unknown (legacy).
    """
    # Update object status
    update_object_status(run_id, object_name, STATUS_COMPLETED)

    # Update object counters (including record_count)
    increment_object_counters(run_id, object_name, pages_fetched, bytes_written, record_count)

    # Update summary counters
    increment_run_counters(
        run_id,
        running_objects=-1,
        completed_objects=1,
        pages_fetched=pages_fetched,
        bytes_written=bytes_written,
        records_fetched=max(0, record_count),  # Don't add negative for unknown
    )


def fail_object_fetch(run_id, object_name, error, cause=None):
    """Mark object as failed after fetch error.

    Updates object status and summary counters.
    These are observability updates - failures are logged but don't block execution.
    """
    # Update object status
    update_object_status(run_id, object_name, STATUS_FAILED, error=error, cause=cause)

    # Update summary counters
    increment_run_counters(run_id, running_objects=-1, failed_objects=1)


def increment_poll_loops(run_id):
    """Increment the poll loop counter."""
    increment_run_counters(run_id, poll_loops=1)


def record_step_attempt(run_id):
    """Record a Lambda invocation attempt.

    Called at the start of each Lambda invocation.
    """
    increment_run_counters(run_id, step_attempts=1)


def record_crawler_started(run_id):
    """Record that the Glue Crawler was started for this run.

    Stores the timestamp so we can correlate crawler runs with exports.
    """
    now = _iso_now()
    try:
        _TABLE.update_item(
            Key={"pk": f"{RUN_PREFIX}{run_id}", "sk": SUMMARY_SK},
            UpdateExpression="SET crawler_started_at = :started, updated_at = :updated",
            ConditionExpression="attribute_exists(run_id)",
            ExpressionAttributeValues={
                ":started": now,
                ":updated": now,
            },
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return  # Summary doesn't exist - skip
        raise


def record_skipped_run(run_id, status, error=None, cause=None):
    """Record a run that was skipped (e.g., rate limited during acquire_lock).

    Creates a summary item with terminal status and zero objects.
    """
    now = _iso_now()
    item = {
        "pk": f"{RUN_PREFIX}{run_id}",
        "sk": SUMMARY_SK,
        "run_id": run_id,
        "status": status,
        "started_at": now,
        "ended_at": now,
        "updated_at": now,
        "total_objects": 0,
        "queued_objects": 0,
        "running_objects": 0,
        "completed_objects": 0,
        "failed_objects": 0,
        "skipped_objects": 0,
        "bytes_written": 0,
        "pages_fetched": 0,
        "records_fetched": 0,
        "poll_loops": 0,
        "jobs_submitted": 0,
        "step_attempts": 0,
        "expires_at": _ttl_timestamp(),
        # GSI keys
        "gsi1pk": GSI1_PK_RUNS,
        "gsi1sk": now,
    }
    if error:
        item["error"] = error
    if cause:
        item["cause"] = cause
    _TABLE.put_item(Item=item)
