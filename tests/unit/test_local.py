#!/usr/bin/env python3
"""Local test script for Salesforce functionality."""

import logging
import os
import sys
from datetime import datetime

# Project root is two levels up from tests/unit/
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging - output to both console and file
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"test_local_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),  # Console output
        logging.FileHandler(LOG_FILE),       # File output
    ],
)
log = logging.getLogger(__name__)

# Load .env file if present (for credentials)
from pathlib import Path
env_file = Path(PROJECT_ROOT) / ".env"
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())

# Set STATE_TABLE before any imports (state_store.py needs it at import time)
os.environ.setdefault("STATE_TABLE", "dummy-table-not-used")

# Add src to path
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

# Monkey-patch _get_param to use direct credentials instead of SSM
import salesforce

# Direct credentials (bypassing SSM)
# Loaded from .env file or environment variables
CREDENTIALS = {
    "/salesforce/username": os.environ.get("SF_USERNAME", ""),
    "/salesforce/password": os.environ.get("SF_PASSWORD", ""),
    "/salesforce/security_token": os.environ.get("SF_TOKEN", ""),
}

def _get_param_local(name, decrypt=False):
    """Return credentials directly instead of from SSM."""
    value = CREDENTIALS.get(name)
    if not value:
        raise ValueError(f"Unknown parameter: {name}")
    return value

# Patch before any imports that use it
salesforce._get_param = _get_param_local

# Set required environment variables
os.environ.setdefault("SF_USERNAME_PARAM", "/salesforce/username")
os.environ.setdefault("SF_PASSWORD_PARAM", "/salesforce/password")
os.environ.setdefault("SF_TOKEN_PARAM", "/salesforce/security_token")
os.environ.setdefault("SF_LOGIN_URL", "https://login.salesforce.com")
os.environ.setdefault("SF_API_VERSION", "63.0")
os.environ.setdefault("LOOKBACK_SECONDS", "0")

# Now import the modules
from salesforce import login, list_queryable_objects, describe_object, check_bulk_api_enabled, RateLimitError
from handler import (
    _should_skip_object,
    _should_skip_field,
    _filter_fields,
    _select_incremental_field,
    _build_query,
    _is_custom_setting_tag,
    _pre_filter_object_names,
    _apply_lookback,
    UNSUPPORTED_BULK_API_OBJECTS,
    QUERY_RESTRICTED_OBJECTS,
    QUERY_INCOMPATIBLE_OBJECTS,
)


def test_login():
    """Test Salesforce authentication."""
    log.info("=" * 60)
    log.info("TEST: Login")
    log.info("=" * 60)
    session = login()
    log.info(f"  Instance URL: {session.instance_url}")
    log.info(f"  API Version:  v{session.api_version}")
    log.info("  [PASS]")
    return session


def test_bulk_api_enabled(session):
    """Test Bulk API access."""
    log.info("=" * 60)
    log.info("TEST: Bulk API Access")
    log.info("=" * 60)
    try:
        check_bulk_api_enabled(session)
        log.info("  Bulk API 2.0 is enabled and accessible")
        log.info("  [PASS]")
    except Exception as e:
        log.error(f"  [FAIL] {e}")
        raise


def test_list_objects(session):
    """Test object listing and pre-filtering."""
    log.info("=" * 60)
    log.info("TEST: Object Listing & Pre-filtering")
    log.info("=" * 60)

    objects = list_queryable_objects(session)
    log.info(f"  Total queryable objects: {len(objects)}")

    filtered = _pre_filter_object_names(objects)
    excluded = len(objects) - len(filtered)
    log.info(f"  After pre-filter:        {len(filtered)} objects")
    log.info(f"  Pre-filtered out:        {excluded} objects")

    # Show what was filtered
    filtered_set = set(filtered)
    removed = [o for o in objects if o not in filtered_set]
    if removed:
        log.info("  Sample excluded objects:")
        for obj in removed[:10]:
            reason = "ChangeEvent" if obj.endswith("ChangeEvent") else \
                     "__b/__x/__hd" if any(obj.endswith(s) for s in ("__b", "__x", "__hd")) else \
                     "blacklist"
            log.info(f"    - {obj} ({reason})")
        if len(removed) > 10:
            log.info(f"    ... and {len(removed) - 10} more")

    log.info("  [PASS]")
    return filtered


def test_describe_and_filter(session, object_name):
    """Test object describe and field filtering for a single object."""
    log.info(f"  --- {object_name} ---")

    try:
        obj_info = describe_object(session, object_name)
    except Exception as e:
        log.warning(f"    [SKIP] Describe failed: {e}")
        return None

    # Check object-level skip
    skip_reason = _should_skip_object(obj_info)
    if skip_reason:
        log.warning(f"    [SKIP] Object skipped: {skip_reason}")
        return None

    # Check custom setting tag
    if object_name.endswith("__Tag"):
        if _is_custom_setting_tag(session, obj_info):
            log.warning("    [SKIP] Custom setting tag")
            return None

    all_fields = obj_info.get("fields", [])
    usable_fields = _filter_fields(all_fields, object_name)
    skipped_count = len(all_fields) - len(usable_fields)

    # Find cursor field
    cursor = _select_incremental_field(usable_fields)

    log.info(f"    Total fields:   {len(all_fields)}")
    log.info(f"    Usable fields:  {len(usable_fields)}")
    log.info(f"    Skipped fields: {skipped_count}")
    log.info(f"    Cursor field:   {cursor or 'NONE'}")

    if not cursor:
        log.warning("    [SKIP] No cursor field")
        return None

    # Show sample skipped fields
    skipped_fields = [f for f in all_fields if _should_skip_field(f, object_name)]
    if skipped_fields:
        types = {}
        for f in skipped_fields:
            t = f.get("type", "unknown")
            types[t] = types.get(t, 0) + 1
        type_summary = ", ".join(f"{t}:{c}" for t, c in sorted(types.items()))
        log.info(f"    Skipped types:  {type_summary}")

    return {
        "name": object_name,
        "fields": usable_fields,
        "cursor": cursor,
        "total_fields": len(all_fields),
    }


def test_query_building(obj_result):
    """Test query generation."""
    if not obj_result:
        return

    name = obj_result["name"]
    fields = obj_result["fields"]
    cursor = obj_result["cursor"]

    field_names = [f["name"] for f in fields]

    # Full sync
    query_full = _build_query(name, field_names, cursor, None)

    # Incremental
    query_inc = _build_query(name, field_names, cursor, "2024-01-01T00:00:00Z")

    # Truncate for display
    max_len = 100
    q_full_display = query_full[:max_len] + "..." if len(query_full) > max_len else query_full
    q_inc_display = query_inc[:max_len] + "..." if len(query_inc) > max_len else query_inc

    log.info(f"    Full sync:      {q_full_display}")
    log.info(f"    Incremental:    {q_inc_display}")


def test_lookback():
    """Test lookback window calculation."""
    log.info("=" * 60)
    log.info("TEST: Lookback Window")
    log.info("=" * 60)

    test_timestamp = "2024-06-15T12:00:00Z"

    # No lookback
    os.environ["LOOKBACK_SECONDS"] = "0"
    result = _apply_lookback(test_timestamp)
    log.info(f"  Input:              {test_timestamp}")
    log.info(f"  Lookback=0:         {result}")
    assert result == test_timestamp, "No lookback should return same timestamp"

    # 1 hour lookback
    os.environ["LOOKBACK_SECONDS"] = "3600"
    result = _apply_lookback(test_timestamp)
    log.info(f"  Lookback=3600:      {result}")
    assert "11:00:00" in result, "1 hour lookback should subtract 1 hour"

    # Reset
    os.environ["LOOKBACK_SECONDS"] = "0"
    log.info("  [PASS]")


def test_blacklist_coverage():
    """Show blacklist coverage statistics."""
    log.info("=" * 60)
    log.info("TEST: Blacklist Coverage")
    log.info("=" * 60)

    log.info(f"  UNSUPPORTED_BULK_API_OBJECTS: {len(UNSUPPORTED_BULK_API_OBJECTS)} objects")
    log.info(f"  QUERY_RESTRICTED_OBJECTS:     {len(QUERY_RESTRICTED_OBJECTS)} objects")
    log.info(f"  QUERY_INCOMPATIBLE_OBJECTS:   {len(QUERY_INCOMPATIBLE_OBJECTS)} objects")
    log.info(f"  Total blacklisted:            {len(UNSUPPORTED_BULK_API_OBJECTS) + len(QUERY_RESTRICTED_OBJECTS) + len(QUERY_INCOMPATIBLE_OBJECTS)} objects")
    log.info("  [PASS]")


def main():
    log.info("=" * 60)
    log.info("SALESFORCE LOCAL TEST SUITE")
    log.info("=" * 60)
    log.info(f"Log file: {LOG_FILE}")

    try:
        # Test authentication
        session = test_login()

        # Test Bulk API
        test_bulk_api_enabled(session)

        # Test object listing
        objects = test_list_objects(session)

        # Test blacklist stats
        test_blacklist_coverage()

        # Test lookback
        test_lookback()

        # Test describe/filter for common objects
        log.info("=" * 60)
        log.info("TEST: Object Describe & Field Filtering")
        log.info("=" * 60)

        test_objects = [
            "Account", "Contact", "Lead", "Opportunity",
            "Task", "Event", "Case", "User",
        ]

        results = []
        for obj_name in test_objects:
            if obj_name in objects:
                result = test_describe_and_filter(session, obj_name)
                if result:
                    test_query_building(result)
                    results.append(result)

        # Summary
        log.info("=" * 60)
        log.info("SUMMARY")
        log.info("=" * 60)
        log.info(f"  Objects tested:     {len(test_objects)}")
        log.info(f"  Objects queryable:  {len(results)}")
        log.info(f"  Total fields:       {sum(r['total_fields'] for r in results)}")
        log.info(f"  Usable fields:      {sum(len(r['fields']) for r in results)}")

        log.info("=" * 60)
        log.info("ALL TESTS PASSED!")
        log.info("=" * 60)
        log.info(f"Log saved to: {LOG_FILE}")

    except RateLimitError as e:
        log.error(f"[RATE LIMITED] {e}")
        log.error("Try again later or check your Salesforce API limits.")
        sys.exit(1)
    except Exception as e:
        log.error(f"[FAILED] {e}")
        import traceback
        log.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
