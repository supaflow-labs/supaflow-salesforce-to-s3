import os
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

_TABLE = boto3.resource("dynamodb").Table(os.environ["STATE_TABLE"])

RUN_PK = "RUN"
OBJ_PREFIX = "OBJ#"
CHECKPOINT_PREFIX = "CKPT#"


def _iso_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stale_cutoff(hours):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    return cutoff.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def get_run_state():
    resp = _TABLE.get_item(Key={"pk": RUN_PK})
    return resp.get("Item")


def try_acquire_run_lock(run_id, run_started_at, ttl_hours):
    now = _iso_now()
    stale_cutoff = _stale_cutoff(ttl_hours)
    try:
        _TABLE.update_item(
            Key={"pk": RUN_PK},
            UpdateExpression=(
                "SET #status = :status, run_id = :run_id, "
                "run_started_at = :run_started_at, updated_at = :updated_at"
            ),
            ConditionExpression=(
                "attribute_not_exists(pk) OR #status <> :in_progress OR "
                "attribute_not_exists(updated_at) OR updated_at < :stale_cutoff"
            ),
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "IN_PROGRESS",
                ":in_progress": "IN_PROGRESS",
                ":run_id": run_id,
                ":run_started_at": run_started_at,
                ":updated_at": now,
                ":stale_cutoff": stale_cutoff,
            },
        )
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise


def heartbeat(run_id):
    now = _iso_now()
    try:
        _TABLE.update_item(
            Key={"pk": RUN_PK},
            UpdateExpression="SET updated_at = :updated_at",
            ConditionExpression="run_id = :run_id",
            ExpressionAttributeValues={":updated_at": now, ":run_id": run_id},
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return
        raise


def complete_run(run_id, status, error=None):
    now = _iso_now()
    update = "SET #status = :status, updated_at = :updated_at"
    values = {":status": status, ":updated_at": now, ":run_id": run_id}
    names = {"#status": "status"}
    if error:
        update += ", #error = :error"
        values[":error"] = error
        names["#error"] = "error"
    _TABLE.update_item(
        Key={"pk": RUN_PK},
        UpdateExpression=update,
        ConditionExpression="run_id = :run_id",
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def complete_run_if_active(run_id, status):
    """Set run status only if currently IN_PROGRESS.

    Uses conditional update to avoid race conditions with eventual consistency.
    Returns True if status was updated, False if skipped (already terminal).
    """
    now = _iso_now()
    try:
        _TABLE.update_item(
            Key={"pk": RUN_PK},
            UpdateExpression="SET #status = :status, updated_at = :updated_at",
            ConditionExpression="run_id = :run_id AND #status = :in_progress",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": status,
                ":updated_at": now,
                ":run_id": run_id,
                ":in_progress": "IN_PROGRESS",
            },
        )
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            # Status already changed (RATE_LIMITED, FAILED, etc.) - preserve it
            return False
        raise


def record_skipped_run(run_id, run_started_at, status, error=None, ttl_hours=24):
    """Record a run that was skipped (e.g., rate limited during acquire_lock).

    Upserts the RUN item only if there's no active run in progress.
    This handles the case where DynamoDB is empty or has a terminal status,
    and also allows overwriting stale IN_PROGRESS runs.
    """
    now = _iso_now()
    stale_cutoff = _stale_cutoff(ttl_hours)
    update = (
        "SET #status = :status, run_id = :run_id, run_started_at = :run_started_at, "
        "updated_at = :updated_at"
    )
    values = {
        ":status": status,
        ":run_id": run_id,
        ":run_started_at": run_started_at or now,
        ":updated_at": now,
        ":in_progress": "IN_PROGRESS",
        ":stale_cutoff": stale_cutoff,
    }
    names = {"#status": "status"}
    if error:
        update += ", #error = :error"
        values[":error"] = error
        names["#error"] = "error"

    try:
        _TABLE.update_item(
            Key={"pk": RUN_PK},
            UpdateExpression=update,
            ConditionExpression=(
                "attribute_not_exists(pk) OR #status <> :in_progress OR "
                "attribute_not_exists(updated_at) OR updated_at < :stale_cutoff"
            ),
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            # There's an active run in progress - don't overwrite
            return False
        raise


def get_object_state(object_name):
    resp = _TABLE.get_item(Key={"pk": f"{OBJ_PREFIX}{object_name}"})
    return resp.get("Item")


def set_object_incremental_field(object_name, incremental_field):
    now = _iso_now()
    _TABLE.update_item(
        Key={"pk": f"{OBJ_PREFIX}{object_name}"},
        UpdateExpression="SET incremental_field = :field, updated_at = :updated_at",
        ExpressionAttributeValues={":field": incremental_field, ":updated_at": now},
    )


def complete_object(object_name, run_id, last_run, incremental_field, is_full_refresh=False):
    """Mark object complete and clear any checkpoint.

    For incremental runs: updates last_run with run timestamp.
    For full refresh runs: updates last_full_refresh_at instead.
    """
    now = _iso_now()
    if is_full_refresh:
        # Full refresh: update last_full_refresh_at, leave incremental_field empty
        _TABLE.update_item(
            Key={"pk": f"{OBJ_PREFIX}{object_name}"},
            UpdateExpression=(
                "SET last_full_refresh_at = :last_full_refresh, updated_at = :updated_at"
            ),
            ExpressionAttributeValues={
                ":last_full_refresh": last_run,
                ":updated_at": now,
            },
        )
    else:
        # Incremental run: update last_run and incremental_field
        _TABLE.update_item(
            Key={"pk": f"{OBJ_PREFIX}{object_name}"},
            UpdateExpression=(
                "SET last_run = :last_run, incremental_field = :field, updated_at = :updated_at"
            ),
            ExpressionAttributeValues={
                ":last_run": last_run,
                ":field": incremental_field,
                ":updated_at": now,
            },
        )
    # Clear checkpoint for this object/run
    clear_checkpoint(run_id, object_name)


def save_checkpoint(run_id, object_name, locator, page_index):
    """Save pagination checkpoint for resumability."""
    now = _iso_now()
    _TABLE.put_item(
        Item={
            "pk": f"{CHECKPOINT_PREFIX}{run_id}#{object_name}",
            "run_id": run_id,
            "object_name": object_name,
            "locator": locator,
            "page_index": page_index,
            "updated_at": now,
        }
    )


def get_checkpoint(run_id, object_name):
    """Get checkpoint for resuming pagination."""
    resp = _TABLE.get_item(Key={"pk": f"{CHECKPOINT_PREFIX}{run_id}#{object_name}"})
    return resp.get("Item")


def clear_checkpoint(run_id, object_name):
    """Clear checkpoint after object is complete."""
    try:
        _TABLE.delete_item(Key={"pk": f"{CHECKPOINT_PREFIX}{run_id}#{object_name}"})
    except ClientError:
        pass  # Ignore if doesn't exist
