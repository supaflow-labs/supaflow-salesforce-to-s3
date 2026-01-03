import json
import logging
import os
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree

import boto3

from salesforce import (
    RateLimitError,
    check_bulk_api_enabled,
    describe_object,
    is_rate_limit_message,
    list_queryable_objects,
    login,
)
from run_control import (
    complete_object_fetch,
    create_run_summary,
    fail_object_fetch,
    get_running_objects,
    increment_poll_loops,
    increment_run_counters,
    record_crawler_started,
    record_skipped_object,
    record_skipped_run as rc_record_skipped_run,
    record_step_attempt,
    start_object_job,
    update_run_status,
    update_run_status_if_active,
)
from state_store import (
    clear_checkpoint,
    complete_object,
    complete_run,
    complete_run_if_active,
    get_checkpoint,
    get_object_state,
    get_run_state,
    heartbeat,
    record_skipped_run,
    save_checkpoint,
    set_object_incremental_field,
    try_acquire_run_lock,
)

LOGGER = logging.getLogger()
LOGGER.setLevel(os.getenv("LOG_LEVEL", "INFO"))

S3 = boto3.client("s3")

UNSUPPORTED_BULK_API_OBJECTS = {
    "FieldSecurityClassification",
    "WorkStepStatus",
    "ShiftStatus",
    "WorkOrderStatus",
    "WorkOrderLineItemStatus",
    "ServiceAppointmentStatus",
    "SolutionStatus",
    "ContractStatus",
    "RecentlyViewed",
    "DeclinedEventRelation",
    "AcceptedEventRelation",
    "TaskStatus",
    "PartnerRole",
    "TaskPriority",
    "CaseStatus",
    "UndecidedEventRelation",
    "OrderStatus",
}

QUERY_RESTRICTED_OBJECTS = {
    "Announcement",
    "ContentDocumentLink",
    "CollaborationGroupRecord",
    "Vote",
    "IdeaComment",
    "FieldDefinition",
    "PlatformAction",
    "UserEntityAccess",
    "RelationshipInfo",
    "ContentFolderMember",
    "ContentFolderItem",
    "SearchLayout",
    "SiteDetail",
    "EntityParticle",
    "OwnerChangeOptionInfo",
    "DataStatistics",
    "UserFieldAccess",
    "PicklistValueInfo",
    "RelationshipDomain",
    "FlexQueueItem",
    "NetworkUserHistoryRecent",
    "FieldHistoryArchive",
    "RecordActionHistory",
    "FlowVersionView",
    "FlowVariableView",
    "AppTabMember",
    "ColorDefinition",
    "DatacloudDandBCompany",
    "DatacloudAddress",
    "FlowTestView",
    "RelatedListColumnDefinition",
    "RelatedListDefinition",
    "ApexTypeImplementor",
    "IconDefinition",
}

QUERY_INCOMPATIBLE_OBJECTS = {
    "DataType",
    "ListViewChartInstance",
    "FeedLike",
    "OutgoingEmail",
    "OutgoingEmailRelation",
    "FeedSignal",
    "ActivityHistory",
    "EmailStatus",
    "UserRecordAccess",
    "Name",
    "AggregateResult",
    "OpenActivity",
    "ProcessInstanceHistory",
    "OwnedContentDocument",
    "FolderedContentDocument",
    "FeedTrackedChange",
    "CombinedAttachment",
    "AttachedContentDocument",
    "ContentBody",
    "NoteAndAttachment",
    "LookedUpFromActivity",
    "AttachedContentNote",
    "QuoteTemplateRichTextData",
}

BLACKLISTED_FIELD_PAIRS = {("EntityDefinition", "RecordTypesSupported")}


def _iso_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso(ts):
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _is_stale(updated_at, ttl_hours):
    if not updated_at:
        return True
    updated = _parse_iso(updated_at)
    if not updated:
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(hours=ttl_hours)
    return updated < cutoff


def _summarize_salesforce_fault(message):
    if not message or "<soapenv:Envelope" not in message:
        return message
    try:
        root = ElementTree.fromstring(message)
    except ElementTree.ParseError:
        return message

    def _get_text(tag):
        elem = root.find(f".//{{*}}{tag}")
        return elem.text.strip() if elem is not None and elem.text else ""

    code = _get_text("exceptionCode") or _get_text("faultcode")
    if ":" in code:
        code = code.split(":", 1)[1]
    detail = _get_text("exceptionMessage") or _get_text("faultstring")

    if code and detail:
        return f"{code}: {detail}"
    return detail or code or message


def _select_incremental_field(fields):
    priority = ["SystemModstamp", "LastModifiedDate", "CreatedDate"]
    for field_name in priority:
        for field in fields:
            if field.get("name", "").lower() == field_name.lower():
                if not field.get("filterable"):
                    continue
                return field.get("name")
    return ""


def _format_cutoff(cutoff):
    """Normalize cutoff timestamp to ISO format for SOQL."""
    if not cutoff:
        return ""
    return cutoff.replace("+00:00", "Z")


def _build_query(object_name, field_names, incremental_field, last_run, cutoff):
    """Build SOQL query with cutoff-anchored incremental window.

    Window bounds:
      - Initial sync (no last_run): WHERE field < cutoff
      - Incremental sync (with last_run): WHERE field >= (last_run - lookback) AND field < cutoff

    The cutoff is the run_started_at timestamp, ensuring all objects in a run
    use the same upper bound for deterministic windows.
    """
    select_clause = ", ".join(field_names)
    query = f"SELECT {select_clause} FROM {object_name}"

    if not incremental_field:
        return query

    cutoff_value = _format_cutoff(cutoff)
    if not cutoff_value:
        # Fallback: no cutoff provided (shouldn't happen in normal flow)
        if last_run:
            last_run_value = _apply_lookback(last_run)
            query += f" WHERE {incremental_field} >= {last_run_value}"
        return query

    if last_run:
        # Incremental sync: [last_run - lookback, cutoff)
        last_run_value = _apply_lookback(last_run)
        query += f" WHERE {incremental_field} >= {last_run_value} AND {incremental_field} < {cutoff_value}"
    else:
        # Initial sync: only upper bound
        query += f" WHERE {incremental_field} < {cutoff_value}"

    return query


def _should_skip_object(object_info):
    name = object_info.get("name", "")
    if object_info.get("deprecatedAndHidden"):
        return "deprecated_or_hidden"
    if name.endswith("ChangeEvent"):
        return "change_event_not_queryable"
    if object_info.get("queryable") is False:
        return "not_queryable"
    if object_info.get("retrieveable") is False:
        return "not_retrieveable"
    if name.endswith("__b") or name.endswith("__x") or name.endswith("__hd"):
        return "unsupported_suffix"
    if name == "AccountUserTerritory2View":
        return "performance_exclusion"
    if name in UNSUPPORTED_BULK_API_OBJECTS:
        return "bulk_api_unsupported"
    if name in QUERY_RESTRICTED_OBJECTS:
        return "query_restricted"
    if name in QUERY_INCOMPATIBLE_OBJECTS:
        return "query_incompatible"
    return ""


def _should_skip_field(field, object_name):
    if field.get("deprecatedAndHidden"):
        return True
    field_name = field.get("name") or ""
    if (object_name, field_name) in BLACKLISTED_FIELD_PAIRS:
        return True
    field_type = (field.get("type") or "").lower()
    if field_type == "base64":
        return True
    if field_type == "complexvalue":
        return True
    if field_type == "address":
        return True
    if field_type == "location":
        return True
    if not field_name:
        return True
    return False


def _filter_fields(fields, object_name):
    filtered = []
    for field in fields:
        if _should_skip_field(field, object_name):
            continue
        filtered.append(field)
    return filtered


def _is_custom_setting_tag(session, object_info):
    object_name = object_info.get("name", "")
    if not object_name.endswith("__Tag"):
        return False
    item_field = next(
        (field for field in object_info.get("fields", []) if field.get("relationshipName") == "Item"),
        None,
    )
    if not item_field:
        return False
    reference_to = item_field.get("referenceTo") or []
    if isinstance(reference_to, str):
        reference_to = [reference_to]
    for ref in reference_to:
        try:
            ref_info = describe_object(session, ref)
        except Exception:
            continue
        if ref_info.get("customSetting"):
            return True
    return False


def _pre_filter_object_names(object_names):
    filtered = []
    for name in object_names:
        if name.endswith("ChangeEvent"):
            continue
        if name.endswith("__b") or name.endswith("__x") or name.endswith("__hd"):
            continue
        if name == "AccountUserTerritory2View":
            continue
        if name in UNSUPPORTED_BULK_API_OBJECTS:
            continue
        if name in QUERY_RESTRICTED_OBJECTS:
            continue
        if name in QUERY_INCOMPATIBLE_OBJECTS:
            continue
        filtered.append(name)
    return filtered


def _apply_allowlist(object_names):
    """Filter objects to only those in the allowlist (if configured).

    If OBJECT_ALLOWLIST is empty or "ALL", returns all objects.
    Otherwise, returns only objects whose names match the allowlist (case-insensitive).
    """
    allowlist_str = os.getenv("OBJECT_ALLOWLIST", "").strip()
    if not allowlist_str:
        return object_names

    # "ALL" sentinel means no filtering
    if allowlist_str.upper() == "ALL":
        LOGGER.info("allowlist set to ALL, returning all %d objects", len(object_names))
        return object_names

    # Parse allowlist (comma-separated, case-insensitive)
    allowlist = {name.strip().lower() for name in allowlist_str.split(",") if name.strip()}
    if not allowlist:
        return object_names

    filtered = [name for name in object_names if name.lower() in allowlist]
    LOGGER.info(
        "allowlist filter: %d objects allowed, %d matched from %d available",
        len(allowlist), len(filtered), len(object_names)
    )
    return filtered


def _output_key(prefix, object_name, run_id, page_index, extension="csv"):
    """Build S3 key for output file.

    Path format: <prefix>/<table_name>_raw/run=<run_id>/part-XXXXX.csv

    This structure is Glue-compliant:
    - Each object gets its own table (top-level folder = table name)
    - 'run' is the partition key
    - '_raw' suffix allows creating views with the object name
    """
    safe_prefix = prefix.strip("/")
    # Convert to lowercase, normalize double underscores, add _raw suffix
    table_name = object_name.lower().replace("__", "_") + "_raw"
    if safe_prefix:
        return f"{safe_prefix}/{table_name}/run={run_id}/part-{page_index:05d}.{extension}"
    return f"{table_name}/run={run_id}/part-{page_index:05d}.{extension}"


def _get_env_int(name, default):
    value = os.getenv(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _apply_lookback(last_run):
    if not last_run:
        return ""
    lookback_seconds = _get_env_int("LOOKBACK_SECONDS", 0)
    if lookback_seconds <= 0:
        return last_run.replace("+00:00", "Z")
    dt = _parse_iso(last_run)
    if not dt:
        return last_run.replace("+00:00", "Z")
    adjusted = dt - timedelta(seconds=lookback_seconds)
    return adjusted.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _is_full_refresh_due(object_state, interval_minutes, run_started_at):
    """Check if a full refresh is due for a no-cursor object.

    Args:
        object_state: Object state from StateTable (may be None).
        interval_minutes: Full refresh interval from config.
            -1: Never (skip no-cursor objects)
            0: Always (every run)
            >0: Only if last_full_refresh_at is older than this many minutes
        run_started_at: Run start time (ISO) to anchor the interval window.

    Returns:
        True if full refresh should run, False if should skip.
    """
    if interval_minutes < 0:
        return False  # Skip no-cursor objects
    if interval_minutes == 0:
        return True  # Always run full refresh

    anchor = _parse_iso(run_started_at) if run_started_at else None
    if not anchor:
        anchor = datetime.now(timezone.utc)

    # Apply 5% grace to reduce drift when runs finish late.
    effective_interval = interval_minutes * 0.95

    # Check last_full_refresh_at
    if not object_state:
        return True  # Never refreshed, run now

    last_refresh = object_state.get("last_full_refresh_at")
    if not last_refresh:
        return True  # Never refreshed, run now

    last_dt = _parse_iso(last_refresh)
    if not last_dt:
        return True  # Invalid timestamp, run now

    cutoff = anchor - timedelta(minutes=effective_interval)
    return last_dt < cutoff


def acquire_lock(event):
    """Acquire run lock and return list of objects to export.

    Args:
        event: Lambda event containing optional execution_arn from Step Functions.
    """
    execution_arn = event.get("execution_arn")
    # Return structure must always include all fields for state machine compatibility
    skip_result = {
        "skip": True,
        "reason": None,
        "run_id": None,
        "run_started_at": None,
        "objects": [],
    }

    ttl_hours = _get_env_int("RUN_LOCK_TTL_HOURS", 24)
    run_state = get_run_state()
    if run_state and run_state.get("status") == "IN_PROGRESS":
        if not _is_stale(run_state.get("updated_at"), ttl_hours):
            skip_result["reason"] = "run_in_progress"
            return skip_result

    try:
        session = login()
        check_bulk_api_enabled(session)
        objects = list_queryable_objects(session)
        objects = _pre_filter_object_names(objects)
        objects = _apply_allowlist(objects)
    except RateLimitError as exc:
        LOGGER.warning("rate limit encountered during acquire_lock: %s", exc)
        # Record rate limit to DynamoDB so status.sh can display it
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        error_message = _summarize_salesforce_fault(str(exc))
        # Write to legacy StateTable
        try:
            record_skipped_run(
                run_id=run_id,
                run_started_at=_iso_now(),
                status="RATE_LIMITED",
                error=error_message,
                ttl_hours=ttl_hours,
            )
        except Exception as e:
            LOGGER.error("Failed to record rate limit in StateTable: %s", e)
        # Write to RunControlTable
        try:
            rc_record_skipped_run(run_id, "RATE_LIMITED", error=error_message)
        except Exception as e:
            LOGGER.error("Failed to record rate limit in RunControlTable: %s", e)
        skip_result["reason"] = "rate_limited"
        return skip_result

    run_started_at = _iso_now()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if not try_acquire_run_lock(run_id, run_started_at, ttl_hours):
        skip_result["reason"] = "run_in_progress"
        return skip_result

    # Create run summary in RunControlTable
    try:
        create_run_summary(run_id, execution_arn=execution_arn, total_objects=len(objects), cutoff_time=run_started_at)
    except Exception as e:
        LOGGER.error("Failed to create run summary: %s", e)

    return {
        "skip": False,
        "reason": None,
        "run_id": run_id,
        "run_started_at": run_started_at,
        "objects": objects,
    }


def start_job(event):
    object_name = event["object_name"]
    run_id = event["run_id"]
    run_started_at = event["run_started_at"]

    # Return structure must always include all fields for state machine compatibility
    def skip_with_reason(reason):
        # Record skip in RunControlTable
        try:
            record_skipped_object(run_id, object_name, reason)
        except Exception as e:
            LOGGER.error("Failed to record skipped object: %s", e)
        return {
            "skip": True,
            "reason": reason,
            "object_name": object_name,
            "job_id": None,
            "incremental_field": None,
            "refresh_mode": None,
            "run_started_at": run_started_at,
        }

    session = login()
    object_info = describe_object(session, object_name)
    skip_reason = _should_skip_object(object_info)
    if skip_reason:
        LOGGER.info("skipping object %s (%s)", object_name, skip_reason)
        heartbeat(run_id)
        return skip_with_reason(skip_reason)
    if _is_custom_setting_tag(session, object_info):
        LOGGER.info("skipping object %s (custom_setting_tag)", object_name)
        heartbeat(run_id)
        return skip_with_reason("custom_setting_tag")

    fields = _filter_fields(object_info.get("fields", []), object_name)
    if not fields:
        LOGGER.info("skipping object %s (no usable fields)", object_name)
        heartbeat(run_id)
        return skip_with_reason("no_usable_fields")

    field_names = [field["name"] for field in fields]
    object_state = get_object_state(object_name) or {}
    incremental_field = _select_incremental_field(fields)

    # Determine refresh mode
    if incremental_field:
        # Has cursor field: use incremental mode
        refresh_mode = "incremental"
        last_run = object_state.get("last_run")
        set_object_incremental_field(object_name, incremental_field)
        query = _build_query(object_name, field_names, incremental_field, last_run, run_started_at)
    else:
        # No cursor field: check if full refresh is enabled/due
        full_refresh_interval = _get_env_int("FULL_REFRESH_INTERVAL_MINUTES", -1)
        if not _is_full_refresh_due(object_state, full_refresh_interval, run_started_at):
            if full_refresh_interval < 0:
                LOGGER.info("skipping object %s (no filterable cursor field)", object_name)
                heartbeat(run_id)
                return skip_with_reason("missing_cursor_field")
            else:
                LOGGER.info("skipping object %s (full refresh not due)", object_name)
                heartbeat(run_id)
                return skip_with_reason("full_refresh_not_due")

        # Full refresh is due
        refresh_mode = "full_refresh"
        last_run = None  # No incremental cursor
        LOGGER.info("running full refresh for object %s (no cursor field)", object_name)
        # Build query without WHERE clause
        select_clause = ", ".join(field_names)
        query = f"SELECT {select_clause} FROM {object_name}"

    payload = {
        "operation": "query",
        "query": query,
        "contentType": "CSV",
        "lineEnding": "LF",
        "columnDelimiter": "COMMA",
    }
    response = session.post_json("jobs/query", payload)
    job_id = response.get("id")
    heartbeat(run_id)

    # Record job start in RunControlTable
    try:
        start_object_job(run_id, object_name, job_id, incremental_field, refresh_mode)
    except Exception as e:
        LOGGER.error("Failed to record job start: %s", e)

    LOGGER.info(
        "started job %s for %s (refresh_mode=%s incremental_field=%s last_run=%s)",
        job_id,
        object_name,
        refresh_mode,
        incremental_field,
        last_run,
    )

    return {
        "skip": False,
        "reason": None,
        "object_name": object_name,
        "job_id": job_id,
        "incremental_field": incremental_field,
        "refresh_mode": refresh_mode,
        "run_started_at": run_started_at,
    }


def check_job(event):
    job_id = event["job_id"]
    run_id = event["run_id"]

    session = login()
    payload = session.get_json(f"jobs/query/{job_id}")
    status = payload.get("state")
    if status in ("Failed", "Aborted"):
        error_message = payload.get("errorMessage") or ""
        if is_rate_limit_message(error_message):
            raise RateLimitError(error_message or "API rate limit exceeded")
    heartbeat(run_id)

    return {"status": status}


def check_jobs_batch(event):
    """Check status of multiple jobs in a single Lambda call.

    Input: {run_id, run_started_at}
    Output: {completed: [...], has_pending: bool, rate_limited: bool}

    Reads RUNNING jobs from DynamoDB instead of receiving via state machine.
    This avoids the 256KB Step Functions state limit for large job counts.
    Failed jobs are recorded to DynamoDB.
    """
    run_id = event.get("run_id")
    run_started_at = event.get("run_started_at")

    # Read running jobs from DynamoDB instead of state machine
    jobs = get_running_objects(run_id)

    if not jobs:
        return {"completed": [], "has_pending": False, "rate_limited": False}

    session = login()
    completed = []
    pending_count = 0
    failed = []
    rate_limited = False

    for job in jobs:
        job_id = job.get("job_id")
        if not job_id:
            continue

        try:
            payload = session.get_json(f"jobs/query/{job_id}")
            status = payload.get("state")

            if status == "JobComplete":
                # Include record count and run_started_at for fetch
                completed.append({
                    "job_id": job_id,
                    "object_name": job.get("object_name"),
                    "incremental_field": job.get("incremental_field"),
                    "refresh_mode": job.get("refresh_mode"),
                    "run_started_at": run_started_at,
                    "record_count": payload.get("numberRecordsProcessed", 0),
                })
            elif status in ("Failed", "Aborted"):
                error_message = payload.get("errorMessage") or ""
                if is_rate_limit_message(error_message):
                    rate_limited = True
                else:
                    failed.append({
                        "object_name": job.get("object_name"),
                        "error": error_message or f"Job {status}",
                    })
            else:
                # InProgress, UploadComplete, etc.
                pending_count += 1

        except RateLimitError:
            rate_limited = True
        except Exception as e:
            LOGGER.error("Error checking job %s: %s", job_id, e)
            failed.append({
                "object_name": job.get("object_name"),
                "error": str(e),
            })

    # Record failed jobs to DynamoDB
    if failed:
        _record_job_failures(run_id, failed)

    # Increment poll loop counter in RunControlTable
    try:
        increment_poll_loops(run_id)
    except Exception as e:
        LOGGER.error("Failed to increment poll loops: %s", e)

    heartbeat(run_id)

    LOGGER.info(
        "check_jobs_batch: completed=%d pending=%d failed=%d rate_limited=%s",
        len(completed), pending_count, len(failed), rate_limited
    )

    return {
        "completed": completed,
        "has_pending": pending_count > 0,
        "rate_limited": rate_limited,
    }


def _record_job_failures(run_id, failed_jobs):
    """Record job failures to DynamoDB for analysis."""
    from state_store import _TABLE, _iso_now

    for job in failed_jobs:
        object_name = job.get("object_name", "unknown")
        error = job.get("error", "Unknown error")
        try:
            _TABLE.put_item(Item={
                "pk": f"FAIL#{run_id}#{object_name}",
                "run_id": run_id,
                "object_name": object_name,
                "error": "JobFailed",
                "cause": error[:1000] if error else "",
                "recorded_at": _iso_now(),
            })
            LOGGER.error(
                "job_failure: object=%s run_id=%s error=%s",
                object_name, run_id, error[:200],
            )
        except Exception as e:
            LOGGER.error("Failed to record job failure for %s: %s", object_name, e)


def fetch_page(event):
    job_id = event["job_id"]
    object_name = event["object_name"]
    run_id = event["run_id"]
    locator = event.get("locator") or ""
    page_index = int(event.get("page_index", 0))

    session = login()
    max_records = _get_env_int("MAX_RECORDS", 10000)
    response = session.get_results(job_id, locator, max_records)

    next_locator = response.headers.get("Sforce-Locator")
    if next_locator:
        next_locator = next_locator.strip()
    has_more = bool(next_locator) and next_locator.lower() != "null"

    bucket = os.environ["OUTPUT_BUCKET"]
    prefix = os.getenv("OUTPUT_PREFIX", "salesforce-export")
    content_encoding = response.headers.get("Content-Encoding", "")
    is_gzipped = content_encoding.lower().startswith("gzip")
    extension = "csv.gz" if is_gzipped else "csv"
    key = _output_key(prefix, object_name, run_id, page_index, extension=extension)

    response.raw.decode_content = False
    extra_args = {"ContentType": "text/csv"}
    if is_gzipped:
        extra_args["ContentEncoding"] = "gzip"
    S3.upload_fileobj(
        response.raw,
        bucket,
        key,
        ExtraArgs=extra_args,
    )
    response.close()

    heartbeat(run_id)

    LOGGER.info(
        "uploaded %s page %s to s3://%s/%s (has_more=%s)",
        object_name,
        page_index,
        bucket,
        key,
        has_more,
    )

    return {
        "next_locator": next_locator if has_more else "",
        "has_more": has_more,
        "next_page_index": page_index + 1,
    }


def fetch_object(event):
    """Fetch all pages for a completed job and mark object complete.

    This is used by the batch polling flow to fetch results for jobs
    that have completed. Supports resuming from checkpoint if Lambda
    timed out mid-pagination.

    Input: {job_id, object_name, run_id, run_started_at, incremental_field, refresh_mode, record_count}
    Output: {status: "ok", pages_fetched: N, record_count: N}

    If record_count is 0, skips fetching and marks complete immediately
    (no empty files written to S3).

    If a rate limit is encountered, the run is marked as RATE_LIMITED
    before re-raising the error.
    """
    job_id = event["job_id"]
    object_name = event["object_name"]
    run_id = event["run_id"]
    run_started_at = event["run_started_at"]
    incremental_field = event.get("incremental_field", "")
    refresh_mode = event.get("refresh_mode", "incremental")
    record_count = event.get("record_count", -1)  # -1 means unknown (legacy)

    # Skip fetch if no records - don't write empty files
    if record_count == 0:
        LOGGER.info("skipping fetch for %s (0 records)", object_name)
        is_full_refresh = refresh_mode == "full_refresh"
        complete_object(object_name, run_id, run_started_at, incremental_field, is_full_refresh=is_full_refresh)
        heartbeat(run_id)
        try:
            complete_object_fetch(run_id, object_name, pages_fetched=0, bytes_written=0, record_count=0)
        except Exception as e:
            LOGGER.error("Failed to record object completion: %s", e)
        return {"status": "ok", "pages_fetched": 0, "record_count": 0}

    try:
        return _fetch_object_impl(
            job_id, object_name, run_id, run_started_at, incremental_field, refresh_mode, record_count
        )
    except RateLimitError as e:
        # Mark run as rate limited before re-raising
        # Note: Don't call fail_object_fetch here - record_fetch_failure handles it
        # to avoid double-counting when the Map's Catch invokes RecordFetchFailure
        LOGGER.error("Rate limit hit during fetch of %s, marking run", object_name)
        complete_run(run_id, "RATE_LIMITED", error="Rate limit during fetch")
        try:
            update_run_status(run_id, "RATE_LIMITED", error="Rate limit during fetch")
        except Exception as ex:
            LOGGER.error("Failed to update RunControlTable: %s", ex)
        raise


def _fetch_object_impl(job_id, object_name, run_id, run_started_at, incremental_field, refresh_mode="incremental", record_count=-1):
    """Internal implementation of fetch_object."""
    session = login()
    max_records = _get_env_int("MAX_RECORDS", 10000)
    bucket = os.environ["OUTPUT_BUCKET"]
    prefix = os.getenv("OUTPUT_PREFIX", "salesforce-export")

    # Check for existing checkpoint to resume from
    checkpoint = get_checkpoint(run_id, object_name)
    if checkpoint:
        locator = checkpoint.get("locator", "")
        page_index = int(checkpoint.get("page_index", 0))  # DynamoDB returns Decimal
        LOGGER.info(
            "resuming %s from checkpoint: page %d, locator=%s",
            object_name,
            page_index,
            locator[:20] + "..." if locator else "None",
        )
    else:
        locator = ""
        page_index = 0

    total_bytes = 0

    while True:
        response = session.get_results(job_id, locator, max_records)

        next_locator = response.headers.get("Sforce-Locator")
        if next_locator:
            next_locator = next_locator.strip()
        has_more = bool(next_locator) and next_locator.lower() != "null"

        # Track bytes from Content-Length header
        content_length = response.headers.get("Content-Length")
        if content_length:
            try:
                total_bytes += int(content_length)
            except ValueError:
                pass

        content_encoding = response.headers.get("Content-Encoding", "")
        is_gzipped = content_encoding.lower().startswith("gzip")
        extension = "csv.gz" if is_gzipped else "csv"
        key = _output_key(prefix, object_name, run_id, page_index, extension=extension)

        response.raw.decode_content = False
        extra_args = {"ContentType": "text/csv"}
        if is_gzipped:
            extra_args["ContentEncoding"] = "gzip"
        S3.upload_fileobj(
            response.raw,
            bucket,
            key,
            ExtraArgs=extra_args,
        )
        response.close()

        LOGGER.info(
            "uploaded %s page %s to s3://%s/%s (has_more=%s)",
            object_name,
            page_index,
            bucket,
            key,
            has_more,
        )

        page_index += 1

        if not has_more:
            break

        # Guard against infinite loop (locator not advancing)
        if next_locator == locator:
            raise Exception(f"Locator not advancing for {object_name}, possible API bug")

        # Save checkpoint before fetching next page
        save_checkpoint(run_id, object_name, next_locator, page_index)
        locator = next_locator

    # Mark object complete (also clears checkpoint)
    is_full_refresh = refresh_mode == "full_refresh"
    complete_object(object_name, run_id, run_started_at, incremental_field, is_full_refresh=is_full_refresh)
    heartbeat(run_id)

    # Record completion in RunControlTable
    try:
        complete_object_fetch(run_id, object_name, page_index, total_bytes, record_count)
    except Exception as e:
        LOGGER.error("Failed to record object completion: %s", e)

    LOGGER.info(
        "completed object %s with %d pages, %d bytes, %d records (refresh_mode=%s)",
        object_name, page_index, total_bytes, record_count, refresh_mode
    )

    return {
        "status": "ok",
        "object_name": object_name,
        "pages_fetched": page_index,
        "bytes_written": total_bytes,
        "record_count": record_count,
    }


def complete_object_action(event):
    object_name = event["object_name"]
    run_id = event["run_id"]
    run_started_at = event["run_started_at"]
    incremental_field = event.get("incremental_field", "")
    refresh_mode = event.get("refresh_mode", "incremental")

    is_full_refresh = refresh_mode == "full_refresh"
    complete_object(object_name, run_id, run_started_at, incremental_field, is_full_refresh=is_full_refresh)
    heartbeat(run_id)
    return {"status": "ok"}


def release_lock(event):
    """Release the run lock and mark as COMPLETED.

    Uses conditional update - only sets COMPLETED if status is still
    IN_PROGRESS. If already RATE_LIMITED or FAILED, preserves that status.
    This avoids race conditions with eventual consistency.
    """
    run_id = event["run_id"]

    updated = complete_run_if_active(run_id, "COMPLETED")

    # Update RunControlTable (also uses conditional update)
    try:
        update_run_status_if_active(run_id, "COMPLETED")
    except Exception as e:
        LOGGER.error("Failed to update RunControlTable: %s", e)

    if updated:
        return {"status": "released"}
    else:
        LOGGER.info("release_lock: status already terminal for run %s", run_id)
        return {"status": "preserved"}


def fail_run(event):
    run_id = event.get("run_id")
    error = event.get("error") or event.get("Error")
    cause = event.get("cause") or event.get("Cause")
    details = json.dumps({"error": error, "cause": cause})
    if run_id:
        complete_run(run_id, "FAILED", error=details)
        # Update RunControlTable
        try:
            update_run_status(run_id, "FAILED", error=error, cause=cause)
        except Exception as e:
            LOGGER.error("Failed to update RunControlTable: %s", e)
    return {"status": "failed"}


def limit_run(event):
    run_id = event.get("run_id")
    error = event.get("error") or event.get("Error")
    cause = event.get("cause") or event.get("Cause")
    details = json.dumps({"error": error, "cause": cause})
    if run_id:
        complete_run(run_id, "RATE_LIMITED", error=details)
        # Update RunControlTable
        try:
            update_run_status(run_id, "RATE_LIMITED", error=error, cause=cause)
        except Exception as e:
            LOGGER.error("Failed to update RunControlTable: %s", e)
    return {"status": "rate_limited"}


def record_fetch_failure(event):
    """Record a fetch failure to DynamoDB for later analysis.

    This is called when FetchObject fails, to track failures without
    passing them through Step Functions state (which has a 256KB limit).

    Input: {object_name, run_id, error, cause}
    Output: {status: "recorded"}
    """
    object_name = event.get("object_name")
    run_id = event.get("run_id")
    error = event.get("error", "Unknown")
    cause = event.get("cause", "")

    LOGGER.error(
        "fetch_failure: object=%s run_id=%s error=%s cause=%s",
        object_name,
        run_id,
        error,
        cause[:200] if cause else "",
    )

    # Record failure in legacy StateTable
    from state_store import _TABLE, _iso_now
    try:
        _TABLE.put_item(Item={
            "pk": f"FAIL#{run_id}#{object_name}",
            "run_id": run_id,
            "object_name": object_name,
            "error": error,
            "cause": cause[:1000] if cause else "",
            "recorded_at": _iso_now(),
        })
    except Exception as e:
        LOGGER.error("Failed to record failure in StateTable: %s", e)

    # Record failure in RunControlTable
    try:
        fail_object_fetch(run_id, object_name, error, cause[:1000] if cause else None)
    except Exception as e:
        LOGGER.error("Failed to record failure in RunControlTable: %s", e)

    return {"status": "recorded"}


def chunk_completed_jobs(event):
    """Split completed jobs into chunks to avoid Step Functions data limit.

    Step Functions has a 256KB limit on state data. With 500+ objects,
    the Map state results can exceed this. We process in chunks of 25.

    Input: {completed, pending, failed, run_id, run_started_at, prior_failed}
    Output: {to_process, remaining, pending, failed, run_id, run_started_at, prior_failed}
    """
    completed = event.get("completed") or []
    pending = event.get("pending") or []
    failed = event.get("failed") or []
    run_id = event.get("run_id")
    run_started_at = event.get("run_started_at")
    prior_failed = event.get("prior_failed") or []

    chunk_size = 25
    to_process = completed[:chunk_size]
    remaining = completed[chunk_size:]

    LOGGER.info(
        "chunk_completed_jobs: total=%d chunk=%d remaining=%d",
        len(completed),
        len(to_process),
        len(remaining),
    )

    return {
        "to_process": to_process,
        "remaining": remaining,
        "pending": pending,
        "failed": failed,
        "run_id": run_id,
        "run_started_at": run_started_at,
        "prior_failed": prior_failed,
    }


def filter_active_jobs(event):
    """Filter out skipped jobs, keeping only jobs with job_id.

    Step Functions JSONPath doesn't support filter predicates like
    $.jobs[?(@.skip == false)], so we do this filtering in Lambda.

    Input: {jobs: [{skip, job_id, ...}, ...], run_id, run_started_at}
    Output: {jobs: [...], run_id, run_started_at}
    """
    jobs = event.get("jobs") or []
    run_id = event.get("run_id")
    run_started_at = event.get("run_started_at")

    active_jobs = [job for job in jobs if not job.get("skip") and job.get("job_id")]

    LOGGER.info(
        "filter_active_jobs: total=%d active=%d skipped=%d",
        len(jobs),
        len(active_jobs),
        len(jobs) - len(active_jobs),
    )

    return {
        "jobs": active_jobs,
        "run_id": run_id,
        "run_started_at": run_started_at,
    }


def accumulate_failures(event):
    """Combine failures from batch check, fetch results, and prior iterations.

    Input: {
        batch_failed: [{object_name, error, ...}, ...],  # Jobs that failed during status check
        fetch_results: [{status, object_name, error?, ...}, ...],  # Results from fetch (some failed)
        prior_failed: [{object_name, error, ...}, ...]  # Accumulated from previous iterations
    }
    Output: {all_failed: [{object_name, error, ...}, ...]}
    """
    batch_failed = event.get("batch_failed") or []
    fetch_results = event.get("fetch_results") or []
    prior_failed = event.get("prior_failed") or []

    all_failed = list(prior_failed)

    # Add failures from batch check
    for item in batch_failed:
        all_failed.append({
            "object_name": item.get("object_name"),
            "error": item.get("error", "Job failed"),
            "source": "batch_check",
        })

    # Add failures from fetch results
    for item in fetch_results:
        if item.get("status") == "failed":
            all_failed.append({
                "object_name": item.get("object_name"),
                "error": item.get("error", "Unknown"),
                "cause": item.get("cause", ""),
                "source": "fetch",
            })

    LOGGER.info(
        "accumulate_failures: batch_failed=%d fetch_failed=%d prior=%d total=%d",
        len(batch_failed),
        sum(1 for r in fetch_results if r.get("status") == "failed"),
        len(prior_failed),
        len(all_failed),
    )

    return {"all_failed": all_failed}


def lambda_handler(event, _context):
    action = event.get("action")

    # Track Lambda invocations for observability (skip acquire_lock which creates the run)
    # Wrapped in try/except since this is just a counter - don't fail the Lambda if it conflicts
    run_id = event.get("run_id")
    if run_id and action != "acquire_lock":
        try:
            record_step_attempt(run_id)
        except Exception:
            pass  # Counter update failed, but don't block the actual work

    if action == "acquire_lock":
        return acquire_lock(event)
    if action == "start_job":
        return start_job(event)
    if action == "check_job":
        return check_job(event)
    if action == "check_jobs_batch":
        return check_jobs_batch(event)
    if action == "fetch_page":
        return fetch_page(event)
    if action == "fetch_object":
        return fetch_object(event)
    if action == "complete_object":
        return complete_object_action(event)
    if action == "release_lock":
        return release_lock(event)
    if action == "fail_run":
        return fail_run(event)
    if action == "limit_run":
        return limit_run(event)
    if action == "accumulate_failures":
        return accumulate_failures(event)
    if action == "filter_active_jobs":
        return filter_active_jobs(event)
    if action == "record_fetch_failure":
        return record_fetch_failure(event)
    if action == "chunk_completed_jobs":
        return chunk_completed_jobs(event)
    if action == "record_crawler_started":
        run_id = event.get("run_id")
        record_crawler_started(run_id)
        return {"status": "ok"}

    raise ValueError(f"Unsupported action: {action}")
