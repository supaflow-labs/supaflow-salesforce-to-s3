#!/usr/bin/env python3
"""
Full end-to-end runner that simulates Step Functions locally.

Uses real AWS resources (DynamoDB, S3, SSM) - same code paths as Lambda.
Creates temporary resources and optionally cleans up.

Usage:
    python local_e2e_runner.py
    python local_e2e_runner.py --objects Account,Contact,Lead
    python local_e2e_runner.py --max-objects 5
    python local_e2e_runner.py --keep-resources
    python local_e2e_runner.py --use-existing-table my-table --use-existing-bucket my-bucket
"""

import argparse
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

# Project root is two levels up from tests/e2e/
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging - output to both console and file
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"local_e2e_runner_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

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

# Load .env file if present
env_file = Path(PROJECT_ROOT) / ".env"
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())

import boto3
from botocore.exceptions import ClientError

# Test configuration
TEST_PREFIX = "sf-e2e-test"
TEST_REGION = os.environ.get("AWS_REGION", "us-east-1")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "5"))


class AWSResources:
    """Manages temporary AWS resources for testing."""

    def __init__(self, profile=None):
        self.session = boto3.Session(profile_name=profile) if profile else boto3.Session()
        self.dynamodb = self.session.resource("dynamodb", region_name=TEST_REGION)
        self.s3 = self.session.client("s3", region_name=TEST_REGION)
        self.ssm = self.session.client("ssm", region_name=TEST_REGION)

        self.table_name = None
        self.bucket_name = None
        self.table_created = False
        self.bucket_created = False

    def create_table(self, name=None):
        """Create a DynamoDB table for state management."""
        self.table_name = name or f"{TEST_PREFIX}-state-{uuid.uuid4().hex[:8]}"

        if name:
            # Check if table exists
            try:
                self.dynamodb.Table(name).load()
                log.info(f"  Using existing table: {self.table_name}")
                return self.table_name
            except ClientError:
                pass

        log.info(f"  Creating table: {self.table_name}")
        table = self.dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        self.table_created = True
        log.info(f"  Table created: {self.table_name}")
        return self.table_name

    def create_bucket(self, name=None):
        """Create an S3 bucket for output."""
        self.bucket_name = name or f"{TEST_PREFIX}-output-{uuid.uuid4().hex[:8]}"

        if name:
            # Check if bucket exists
            try:
                self.s3.head_bucket(Bucket=name)
                log.info(f"  Using existing bucket: {self.bucket_name}")
                return self.bucket_name
            except ClientError:
                pass

        log.info(f"  Creating bucket: {self.bucket_name}")
        if TEST_REGION == "us-east-1":
            self.s3.create_bucket(Bucket=self.bucket_name)
        else:
            self.s3.create_bucket(
                Bucket=self.bucket_name,
                CreateBucketConfiguration={"LocationConstraint": TEST_REGION},
            )
        self.bucket_created = True
        log.info(f"  Bucket created: s3://{self.bucket_name}")
        return self.bucket_name

    def setup_ssm_params(self):
        """Verify SSM parameters exist (don't create - security risk)."""
        params = [
            os.environ.get("SF_USERNAME_PARAM", "/salesforce/username"),
            os.environ.get("SF_PASSWORD_PARAM", "/salesforce/password"),
            os.environ.get("SF_TOKEN_PARAM", "/salesforce/security_token"),
        ]
        log.info("  Verifying SSM parameters...")
        for param in params:
            try:
                self.ssm.get_parameter(Name=param, WithDecryption=True)
                log.info(f"    {param}")
            except ClientError as e:
                if e.response["Error"]["Code"] == "ParameterNotFound":
                    raise ValueError(
                        f"SSM parameter not found: {param}\n"
                        f"Create it with: aws ssm put-parameter --name {param} --type SecureString --value <value>"
                    )
                raise

    def get_table_item(self, pk):
        """Get item from DynamoDB table."""
        table = self.dynamodb.Table(self.table_name)
        response = table.get_item(Key={"pk": pk})
        return response.get("Item")

    def list_s3_objects(self, prefix):
        """List objects in S3 bucket with prefix."""
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        return response.get("Contents", [])

    def cleanup(self):
        """Delete temporary resources."""
        if self.bucket_created and self.bucket_name:
            log.info(f"  Deleting bucket: {self.bucket_name}")
            try:
                # Delete all objects first
                paginator = self.s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=self.bucket_name):
                    objects = page.get("Contents", [])
                    if objects:
                        delete_keys = [{"Key": obj["Key"]} for obj in objects]
                        self.s3.delete_objects(Bucket=self.bucket_name, Delete={"Objects": delete_keys})
                self.s3.delete_bucket(Bucket=self.bucket_name)
                log.info("    Deleted bucket")
            except ClientError as e:
                log.error(f"    Failed to delete bucket: {e}")

        if self.table_created and self.table_name:
            log.info(f"  Deleting table: {self.table_name}")
            try:
                table = self.dynamodb.Table(self.table_name)
                table.delete()
                table.wait_until_not_exists()
                log.info("    Deleted table")
            except ClientError as e:
                log.error(f"    Failed to delete table: {e}")


class StepFunctionSimulator:
    """Simulates the Step Functions workflow locally."""

    def __init__(self, resources: AWSResources, poll_seconds=5):
        self.resources = resources
        self.poll_seconds = poll_seconds

        # Import handler after setting environment
        sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))
        from handler import lambda_handler
        from salesforce import RateLimitError
        self.lambda_handler = lambda_handler
        self.rate_limit_error = RateLimitError

    def invoke(self, action, **kwargs):
        """Invoke the Lambda handler with an action."""
        event = {"action": action, **kwargs}
        return self.lambda_handler(event, None)

    def run(self, object_filter=None, max_objects=None):
        """Run the full export workflow."""
        log.info("=" * 60)
        log.info("STEP FUNCTIONS SIMULATION")
        log.info("=" * 60)

        # Step 1: Acquire lock
        log.info("[AcquireLock]")
        result = self.invoke("acquire_lock")

        if result.get("skip"):
            reason = result.get("reason", "unknown")
            log.info(f"  Skipped: {reason}")
            return {"status": "skipped", "reason": reason}

        run_id = result["run_id"]
        run_started_at = result["run_started_at"]
        objects = result["objects"]

        log.info(f"  Run ID: {run_id}")
        log.info(f"  Started at: {run_started_at}")
        log.info(f"  Objects found: {len(objects)}")

        # Apply filters
        if object_filter:
            objects = [o for o in objects if o in object_filter]
            log.info(f"  After filter: {len(objects)} objects")

        if max_objects:
            objects = objects[:max_objects]
            log.info(f"  Limited to: {len(objects)} objects")

        # Step 2: Process each object
        processed = []
        skipped = []

        for i, object_name in enumerate(objects):
            log.info(f"[ProcessObject {i+1}/{len(objects)}] {object_name}")

            try:
                obj_result = self._process_object(object_name, run_id, run_started_at)
                if obj_result.get("skipped"):
                    skipped.append({"object": object_name, "reason": obj_result.get("reason")})
                else:
                    processed.append(obj_result)
            except self.rate_limit_error as e:
                log.warning(f"  RATE LIMITED: {e}")
                self.invoke(
                    "limit_run",
                    run_id=run_id,
                    error="RateLimitError",
                    cause=str(e),
                )
                return {
                    "status": "rate_limited",
                    "run_id": run_id,
                    "processed": processed,
                    "skipped": skipped,
                }
            except Exception as e:
                log.error(f"  ERROR: {e}")
                self.invoke(
                    "fail_run",
                    run_id=run_id,
                    error=type(e).__name__,
                    cause=str(e),
                )
                return {
                    "status": "failed",
                    "run_id": run_id,
                    "processed": processed,
                    "skipped": skipped,
                }

        # Step 3: Release lock
        log.info("[ReleaseLock]")
        result = self.invoke("release_lock", run_id=run_id)
        log.info(f"  Status: {result.get('status')}")

        return {
            "status": "completed",
            "run_id": run_id,
            "processed": processed,
            "skipped": skipped,
        }

    def _process_object(self, object_name, run_id, run_started_at):
        """Process a single object through the workflow."""

        # StartJob
        log.info("  [StartJob]")
        result = self.invoke(
            "start_job",
            object_name=object_name,
            run_id=run_id,
            run_started_at=run_started_at,
        )

        if result.get("skip"):
            reason = result.get("reason", "unknown")
            log.info(f"    Skipped: {reason}")
            return {"skipped": True, "reason": reason}

        job_id = result["job_id"]
        incremental_field = result["incremental_field"]
        log.info(f"    Job ID: {job_id}")
        log.info(f"    Cursor: {incremental_field}")

        # WaitForJob + CheckJob loop
        log.info("  [WaitForJob]")
        while True:
            time.sleep(self.poll_seconds)
            result = self.invoke("check_job", job_id=job_id, run_id=run_id)
            status = result.get("status")
            log.info(f"    Status: {status}")

            if status == "JobComplete":
                break
            elif status in ("Failed", "Aborted"):
                raise Exception(f"Salesforce job {status}")

        # FetchPage loop
        log.info("  [FetchPages]")
        locator = ""
        page_index = 0
        total_bytes = 0

        while True:
            result = self.invoke(
                "fetch_page",
                job_id=job_id,
                object_name=object_name,
                run_id=run_id,
                locator=locator,
                page_index=page_index,
            )

            has_more = result.get("has_more", False)
            next_locator = result.get("next_locator", "")
            log.info(f"    Page {page_index}: has_more={has_more}")

            page_index = result.get("next_page_index", page_index + 1)

            if not has_more:
                break
            locator = next_locator

        # CompleteObject
        log.info("  [CompleteObject]")
        result = self.invoke(
            "complete_object",
            object_name=object_name,
            run_id=run_id,
            run_started_at=run_started_at,
            incremental_field=incremental_field,
        )
        log.info(f"    Status: {result.get('status')}")

        return {
            "object": object_name,
            "job_id": job_id,
            "pages": page_index,
            "incremental_field": incremental_field,
        }


def validate_results(resources: AWSResources, run_result: dict):
    """Validate the run results against AWS resources."""
    log.info("=" * 60)
    log.info("VALIDATION")
    log.info("=" * 60)

    if run_result["status"] == "skipped":
        log.info(f"  Run was skipped: {run_result.get('reason')}")
        return

    run_id = run_result["run_id"]
    processed = run_result["processed"]

    # Check DynamoDB run state
    log.info("[DynamoDB State]")
    run_state = resources.get_table_item("RUN")
    if run_state:
        log.info(f"  Run status: {run_state.get('status')}")
        log.info(f"  Run ID: {run_state.get('run_id')}")
        expected_status = "COMPLETED"
        if run_result["status"] == "rate_limited":
            expected_status = "RATE_LIMITED"
        elif run_result["status"] == "failed":
            expected_status = "FAILED"
        assert run_state.get("status") == expected_status, "Run status mismatch"
        assert run_state.get("run_id") == run_id, "Run ID mismatch"
        log.info("  [PASS] Run state correct")
    else:
        log.warning("  [WARN] No run state found")

    if run_result["status"] in ("failed", "rate_limited"):
        log.info("  [PASS] Validation complete (run did not finish)")
        return

    # Check object states
    for obj in processed:
        obj_name = obj["object"]
        obj_state = resources.get_table_item(f"OBJ#{obj_name}")
        if obj_state:
            log.info(f"  Object: {obj_name}")
            log.info(f"    last_run: {obj_state.get('last_run')}")
            log.info(f"    incremental_field: {obj_state.get('incremental_field')}")
            assert obj_state.get("last_run"), "Expected last_run to be set"
            assert obj_state.get("incremental_field") == obj["incremental_field"], "Cursor mismatch"

    # Check S3 output
    log.info("[S3 Output]")
    prefix = os.environ.get("OUTPUT_PREFIX", "salesforce-export")

    for obj in processed:
        obj_name = obj["object"]
        # Glue-compliant path: {table}_raw/run={run_id}/
        table_name = obj_name.lower().replace("__", "_") + "_raw"
        obj_prefix = f"{prefix}/{table_name}/run={run_id}/"
        s3_objects = resources.list_s3_objects(obj_prefix)

        if s3_objects:
            total_size = sum(o["Size"] for o in s3_objects)
            log.info(f"  {obj_name}: {len(s3_objects)} files, {total_size} bytes")
            log.info(f"    Path: s3://{resources.bucket_name}/{obj_prefix}")
        else:
            log.warning(f"  {obj_name}: [WARN] No files found at {obj_prefix}")

    log.info("  [PASS] Validation complete")


def main():
    parser = argparse.ArgumentParser(description="Full E2E runner with real AWS resources")
    parser.add_argument("--profile", default=os.environ.get("AWS_PROFILE"), help="AWS profile")
    parser.add_argument("--objects", help="Comma-separated list of objects to process")
    parser.add_argument("--max-objects", type=int, help="Max objects to process")
    parser.add_argument("--poll-seconds", type=int, default=POLL_SECONDS, help="Job poll interval")
    parser.add_argument("--use-existing-table", help="Use existing DynamoDB table")
    parser.add_argument("--use-existing-bucket", help="Use existing S3 bucket")
    parser.add_argument("--keep-resources", action="store_true", help="Keep test resources (table, bucket) for inspection (default: auto-cleanup)")
    parser.add_argument("--output-prefix", default="e2e-test-export", help="S3 output prefix")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("FULL E2E TEST - Real AWS Resources")
    log.info("=" * 60)
    log.info(f"Log file: {LOG_FILE}")

    resources = AWSResources(profile=args.profile)

    try:
        # Setup resources
        log.info("[1/5] Setting up AWS resources...")
        table_name = resources.create_table(args.use_existing_table)
        bucket_name = resources.create_bucket(args.use_existing_bucket)
        resources.setup_ssm_params()

        # Set environment for handler
        os.environ["STATE_TABLE"] = table_name
        os.environ["OUTPUT_BUCKET"] = bucket_name
        os.environ["OUTPUT_PREFIX"] = args.output_prefix
        os.environ.setdefault("SF_USERNAME_PARAM", "/salesforce/username")
        os.environ.setdefault("SF_PASSWORD_PARAM", "/salesforce/password")
        os.environ.setdefault("SF_TOKEN_PARAM", "/salesforce/security_token")
        os.environ.setdefault("SF_LOGIN_URL", "https://login.salesforce.com")
        os.environ.setdefault("SF_API_VERSION", "63.0")
        os.environ.setdefault("LOOKBACK_SECONDS", "0")
        os.environ.setdefault("MAX_RECORDS", "10000")
        os.environ.setdefault("RUN_LOCK_TTL_HOURS", "24")

        # Parse object filter
        object_filter = None
        if args.objects:
            object_filter = set(args.objects.split(","))

        # Run simulation
        log.info("[2/5] Running Step Functions simulation...")
        simulator = StepFunctionSimulator(resources, poll_seconds=args.poll_seconds)
        run_result = simulator.run(object_filter=object_filter, max_objects=args.max_objects)

        # Validate results
        log.info("[3/5] Validating results...")
        validate_results(resources, run_result)

        # Summary
        log.info("=" * 60)
        log.info("SUMMARY")
        log.info("=" * 60)
        log.info(f"  Status: {run_result['status']}")
        if run_result["status"] in ("completed", "failed", "rate_limited"):
            log.info(f"  Run ID: {run_result['run_id']}")
            log.info(f"  Processed: {len(run_result['processed'])} objects")
            log.info(f"  Skipped: {len(run_result['skipped'])} objects")

            if run_result["processed"]:
                log.info("  Processed objects:")
                for obj in run_result["processed"]:
                    log.info(f"    - {obj['object']}: {obj['pages']} pages")

            if run_result["skipped"]:
                log.info("  Skipped objects:")
                for obj in run_result["skipped"][:10]:
                    log.info(f"    - {obj['object']}: {obj['reason']}")
                if len(run_result["skipped"]) > 10:
                    log.info(f"    ... and {len(run_result['skipped']) - 10} more")

        log.info(f"  DynamoDB Table: {table_name}")
        log.info(f"  S3 Bucket: s3://{bucket_name}/{args.output_prefix}/")

        log.info("=" * 60)
        if run_result["status"] == "completed":
            log.info("[PASS] E2E TEST COMPLETE")
            log.info("=" * 60)
            log.info(f"Log saved to: {LOG_FILE}")
        else:
            log.warning(f"[WARN] E2E TEST ENDED WITH STATUS: {run_result['status']}")
            log.info("=" * 60)
            raise SystemExit(1)

    except Exception as e:
        log.error(f"[FAIL] {e}")
        import traceback
        log.error(traceback.format_exc())
        sys.exit(1)

    finally:
        # Cleanup
        if args.keep_resources:
            log.info("[5/5] Resources retained (--keep-resources)")
            log.info(f"  Table: {resources.table_name}")
            log.info(f"  Bucket: {resources.bucket_name}")
            log.info("  To clean up manually:")
            if resources.table_created:
                log.info(f"    aws dynamodb delete-table --table-name {resources.table_name}")
            if resources.bucket_created:
                log.info(f"    aws s3 rb s3://{resources.bucket_name} --force")
        else:
            log.info("[5/5] Cleaning up resources...")
            resources.cleanup()


if __name__ == "__main__":
    main()
