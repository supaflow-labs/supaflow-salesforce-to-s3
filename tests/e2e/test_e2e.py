#!/usr/bin/env python3
"""
End-to-end test: Salesforce Bulk API read + S3 write.

Creates a temporary test bucket. Uses AWS_PROFILE from .env or environment.
"""

import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Project root is two levels up from tests/e2e/
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging - output to both console and file
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"test_e2e_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE),
    ],
)
log = logging.getLogger(__name__)

# Load .env file if present (for credentials)
env_file = Path(PROJECT_ROOT) / ".env"
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())

# Set environment variables before any imports
os.environ.setdefault("STATE_TABLE", "dummy-table-not-used")

# Add src to path
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

import boto3
from botocore.exceptions import ClientError

# Monkey-patch SSM to use direct credentials
import salesforce

# Direct credentials (bypassing SSM)
# Loaded from .env file or environment variables
CREDENTIALS = {
    "/salesforce/username": os.environ.get("SF_USERNAME", ""),
    "/salesforce/password": os.environ.get("SF_PASSWORD", ""),
    "/salesforce/security_token": os.environ.get("SF_TOKEN", ""),
}

def _get_param_local(name, decrypt=False):
    value = CREDENTIALS.get(name)
    if not value:
        raise ValueError(f"Unknown parameter: {name}")
    return value

salesforce._get_param = _get_param_local

# Set required environment variables
os.environ.setdefault("SF_USERNAME_PARAM", "/salesforce/username")
os.environ.setdefault("SF_PASSWORD_PARAM", "/salesforce/password")
os.environ.setdefault("SF_TOKEN_PARAM", "/salesforce/security_token")
os.environ.setdefault("SF_LOGIN_URL", "https://login.salesforce.com")
os.environ.setdefault("SF_API_VERSION", "63.0")

# Now import modules
from salesforce import login, describe_object
from handler import _filter_fields, _select_incremental_field, _build_query

# AWS clients with profile from .env or environment
aws_profile = os.environ.get("AWS_PROFILE")
session = boto3.Session(profile_name=aws_profile) if aws_profile else boto3.Session()
S3 = session.client("s3")

# Test configuration
TEST_BUCKET_PREFIX = "sf-to-s3-test"
TEST_REGION = "us-east-1"


def create_test_bucket():
    """Create a unique test bucket."""
    bucket_name = f"{TEST_BUCKET_PREFIX}-{uuid.uuid4().hex[:8]}"
    log.info(f"  Creating bucket: {bucket_name}")

    try:
        # us-east-1 doesn't need LocationConstraint
        if TEST_REGION == "us-east-1":
            S3.create_bucket(Bucket=bucket_name)
        else:
            S3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": TEST_REGION}
            )

        # Wait for bucket to be available
        waiter = S3.get_waiter("bucket_exists")
        waiter.wait(Bucket=bucket_name)

        log.info(f"  Bucket created: s3://{bucket_name}")
        return bucket_name
    except ClientError as e:
        log.error(f"  [FAIL] Failed to create bucket: {e}")
        raise


def delete_test_bucket(bucket_name):
    """Delete test bucket and all contents."""
    log.info(f"  Cleaning up bucket: {bucket_name}")

    try:
        # Delete all objects first
        paginator = S3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name):
            objects = page.get("Contents", [])
            if objects:
                delete_keys = [{"Key": obj["Key"]} for obj in objects]
                S3.delete_objects(Bucket=bucket_name, Delete={"Objects": delete_keys})
                log.info(f"    Deleted {len(delete_keys)} objects")

        # Delete bucket
        S3.delete_bucket(Bucket=bucket_name)
        log.info(f"  Bucket deleted: {bucket_name}")
    except ClientError as e:
        log.warning(f"  [WARN] Failed to delete bucket: {e}")


def submit_bulk_query(sf_session, object_name, fields, cursor_field, limit=100):
    """Submit a Bulk API 2.0 query job."""
    field_names = [f["name"] for f in fields]

    # Build query with LIMIT for testing
    select_clause = ", ".join(field_names)
    query = f"SELECT {select_clause} FROM {object_name} ORDER BY {cursor_field} DESC LIMIT {limit}"

    log.info(f"  Query: {query[:80]}...")

    payload = {
        "operation": "query",
        "query": query,
        "contentType": "CSV",
        "lineEnding": "LF",
        "columnDelimiter": "COMMA",
    }

    response = sf_session.post_json("jobs/query", payload)
    job_id = response.get("id")
    log.info(f"  Job ID: {job_id}")
    return job_id


def wait_for_job(sf_session, job_id, timeout=120):
    """Wait for Bulk API job to complete."""
    start = time.time()
    while time.time() - start < timeout:
        response = sf_session.get_json(f"jobs/query/{job_id}")
        state = response.get("state")

        if state == "JobComplete":
            records = response.get("numberRecordsProcessed", 0)
            log.info(f"  Job complete: {records} records")
            return True
        elif state in ("Failed", "Aborted"):
            error = response.get("errorMessage", "Unknown error")
            log.error(f"  Job failed: {error}")
            return False

        log.info(f"  Job state: {state}, waiting...")
        time.sleep(5)

    log.error(f"  Job timed out after {timeout}s")
    return False


def fetch_and_upload(sf_session, job_id, bucket_name, object_name, run_id):
    """Fetch Bulk API results and upload to S3."""
    from urllib.parse import urljoin
    import requests

    url = urljoin(
        sf_session.instance_url + "/",
        f"services/data/v{sf_session.api_version}/jobs/query/{job_id}/results"
    )
    headers = {"Authorization": f"Bearer {sf_session.access_token}", "Accept": "text/csv"}

    page_index = 0
    total_bytes = 0
    locator = None

    while True:
        req_headers = headers.copy()
        if locator:
            req_headers["Sforce-Locator"] = locator

        response = requests.get(url, headers=req_headers, params={"maxRecords": "2000"}, stream=True, timeout=60)
        response.raise_for_status()

        # Get next locator
        next_locator = response.headers.get("Sforce-Locator")
        if next_locator:
            next_locator = next_locator.strip()
        has_more = bool(next_locator) and next_locator.lower() != "null"

        # Upload to S3 (Glue-compliant path: {table}_raw/run={run_id}/)
        table_name = object_name.lower().replace("__", "_") + "_raw"
        key = f"test-export/{table_name}/run={run_id}/part-{page_index:05d}.csv"

        # Read content and upload
        content = response.content
        total_bytes += len(content)

        S3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=content,
            ContentType="text/csv"
        )

        # Count rows (excluding header on first page)
        lines = content.decode("utf-8").strip().split("\n")
        row_count = len(lines) - 1 if page_index == 0 else len(lines)

        log.info(f"    Page {page_index}: {row_count} rows, {len(content)} bytes -> s3://{bucket_name}/{key}")

        response.close()
        page_index += 1

        if not has_more:
            break
        locator = next_locator

    return page_index, total_bytes


def verify_s3_data(bucket_name, object_name, run_id):
    """Verify data was written to S3."""
    table_name = object_name.lower().replace("__", "_") + "_raw"
    prefix = f"test-export/{table_name}/run={run_id}/"

    response = S3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    objects = response.get("Contents", [])

    total_size = sum(obj["Size"] for obj in objects)

    log.info(f"  Found {len(objects)} files, total size: {total_size} bytes")

    # Read first file and show sample
    if objects:
        first_key = objects[0]["Key"]
        response = S3.get_object(Bucket=bucket_name, Key=first_key)
        content = response["Body"].read().decode("utf-8")
        lines = content.strip().split("\n")

        log.info(f"  Sample from {first_key}:")
        log.info(f"    Header: {lines[0][:100]}...")
        if len(lines) > 1:
            log.info(f"    Row 1:  {lines[1][:100]}...")

    return len(objects), total_size


def test_bulk_read_s3_write():
    """Main test: Bulk API read + S3 write."""
    log.info("=" * 60)
    log.info("END-TO-END TEST: Bulk API Read + S3 Write")
    log.info("=" * 60)
    log.info(f"Log file: {LOG_FILE}")

    bucket_name = None

    try:
        # Step 1: Create test bucket
        log.info("[1/6] Creating test S3 bucket...")
        bucket_name = create_test_bucket()

        # Step 2: Login to Salesforce
        log.info("[2/6] Logging into Salesforce...")
        sf_session = login()
        log.info(f"  Connected: {sf_session.instance_url}")

        # Step 3: Describe object and get fields
        log.info("[3/6] Describing Account object...")
        obj_info = describe_object(sf_session, "Account")
        fields = _filter_fields(obj_info.get("fields", []), "Account")
        cursor = _select_incremental_field(fields)
        log.info(f"  Fields: {len(fields)}, Cursor: {cursor}")

        # Step 4: Submit Bulk API query
        log.info("[4/6] Submitting Bulk API query (LIMIT 100)...")
        job_id = submit_bulk_query(sf_session, "Account", fields, cursor, limit=100)

        # Step 5: Wait for job and fetch results
        log.info("[5/6] Waiting for job completion...")
        if not wait_for_job(sf_session, job_id):
            raise Exception("Bulk API job failed")

        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        log.info(f"  Fetching results and uploading to S3 (run_id={run_id})...")
        pages, total_bytes = fetch_and_upload(sf_session, job_id, bucket_name, "Account", run_id)

        # Step 6: Verify S3 data
        log.info("[6/6] Verifying S3 data...")
        file_count, total_size = verify_s3_data(bucket_name, "Account", run_id)

        # Summary
        log.info("=" * 60)
        log.info("TEST RESULTS")
        log.info("=" * 60)
        log.info(f"  Bucket:       s3://{bucket_name}")
        log.info(f"  Object:       Account")
        log.info(f"  Pages:        {pages}")
        log.info(f"  Files in S3:  {file_count}")
        log.info(f"  Total bytes:  {total_size}")
        log.info("  [PASS] End-to-end test successful!")
        log.info("=" * 60)
        log.info(f"Log saved to: {LOG_FILE}")

        return bucket_name

    except Exception as e:
        log.error(f"[FAIL] {e}")
        import traceback
        log.error(traceback.format_exc())
        raise
    finally:
        # Cleanup by default, skip if --keep-bucket
        if bucket_name:
            if KEEP_BUCKET:
                log.info(f"  Bucket retained (--keep-bucket): s3://{bucket_name}")
                log.info(f"  Delete manually: aws s3 rb s3://{bucket_name} --force")
            else:
                delete_test_bucket(bucket_name)


def test_multiple_objects():
    """Test multiple objects in sequence."""
    log.info("=" * 60)
    log.info("MULTI-OBJECT TEST: Bulk Read + S3 Write")
    log.info("=" * 60)
    log.info(f"Log file: {LOG_FILE}")

    test_objects = ["Account", "Contact", "Lead"]
    bucket_name = None

    try:
        # Create bucket
        log.info("[1] Creating test S3 bucket...")
        bucket_name = create_test_bucket()

        # Login
        log.info("[2] Logging into Salesforce...")
        sf_session = login()
        log.info(f"  Connected: {sf_session.instance_url}")

        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        results = []

        for obj_name in test_objects:
            log.info(f"[3] Processing {obj_name}...")

            # Describe
            obj_info = describe_object(sf_session, obj_name)
            fields = _filter_fields(obj_info.get("fields", []), obj_name)
            cursor = _select_incremental_field(fields)

            if not cursor:
                log.warning(f"  [SKIP] No cursor field")
                continue

            # Submit query
            job_id = submit_bulk_query(sf_session, obj_name, fields, cursor, limit=50)

            # Wait
            if not wait_for_job(sf_session, job_id):
                log.warning(f"  [SKIP] Job failed")
                continue

            # Fetch and upload
            pages, total_bytes = fetch_and_upload(sf_session, job_id, bucket_name, obj_name, run_id)
            results.append({"object": obj_name, "pages": pages, "bytes": total_bytes})

        # Summary
        log.info("=" * 60)
        log.info("MULTI-OBJECT TEST RESULTS")
        log.info("=" * 60)
        log.info(f"  Bucket: s3://{bucket_name}")
        log.info(f"  Run ID: {run_id}")
        for r in results:
            log.info(f"  {r['object']}: {r['pages']} pages, {r['bytes']} bytes")
        log.info("  [PASS] Multi-object test successful!")
        log.info("=" * 60)
        log.info(f"Log saved to: {LOG_FILE}")

        return bucket_name

    except Exception as e:
        log.error(f"[FAIL] {e}")
        import traceback
        log.error(traceback.format_exc())
        raise
    finally:
        # Cleanup by default, skip if --keep-bucket
        if bucket_name:
            if KEEP_BUCKET:
                log.info(f"  Bucket retained (--keep-bucket): s3://{bucket_name}")
                log.info(f"  Delete manually: aws s3 rb s3://{bucket_name} --force")
            else:
                delete_test_bucket(bucket_name)


# Global flag for bucket retention
KEEP_BUCKET = False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="E2E test: Salesforce Bulk API + S3")
    parser.add_argument("--multi", action="store_true", help="Test multiple objects")
    parser.add_argument("--keep-bucket", action="store_true", help="Keep test bucket for inspection (default: auto-cleanup)")
    args = parser.parse_args()

    KEEP_BUCKET = args.keep_bucket

    if args.multi:
        test_multiple_objects()
    else:
        test_bulk_read_s3_write()
