import hashlib
import json
import logging
import os
import tempfile
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from app import SearchKeywordRevenueProcessor
from metadata_profiler import MetadataProfiler

# Root logger for CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
s3_client = boto3.client("s3")
sns_client = boto3.client("sns")


def load_metadata_config(config_path: str = "metadata_config.json") -> dict:
    """
    Load metadata configuration from JSON file packaged with Lambda code.
    """
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def publish_failure(subject: str, message: str) -> None:
    """
    Publish failure notification to SNS if SNS topic ARN is configured.
    """
    topic_arn = os.environ.get("SNS_TOPIC_ARN")
    if topic_arn:
        sns_client.publish(
            TopicArn=topic_arn,
            Subject=subject[:100],
            Message=message
        )


def setup_file_logger(log_file_path: str):
    """
    Create a dedicated file logger that writes pipeline execution logs
    into a local file under Lambda /tmp, which is later uploaded to S3.
    """
    file_logger = logging.getLogger("pipeline_file_logger")
    file_logger.setLevel(logging.INFO)

    if file_logger.handlers:
        file_logger.handlers.clear()

    handler = logging.FileHandler(log_file_path, mode="w", encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    file_logger.addHandler(handler)

    return file_logger, handler


def compute_file_sha256(file_path: str) -> str:
    """
    Compute SHA-256 checksum for the source file.

    Reads the file in chunks so it works for larger files too.
    """
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def checksum_exists(bucket: str, checksum_key: str) -> bool:
    """
    Check whether a checksum registry file already exists in S3.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=checksum_key)
        return True
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def write_json_to_s3(bucket: str, key: str, payload: dict) -> None:
    """
    Helper function to write JSON content to S3.
    """
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json"
    )


def lambda_handler(event, context):
    """
    Main Lambda handler.

    Trigger:
    - S3 upload event under raw/

    Example input path:
    s3://analytics-hitdata-s3/raw/2026/03/12/18-00-01/sample data.sql

    Outputs created:
    - checksums/<sha256>.json
    - metadata/YYYY/MM/DD/HH-MM-SS/metadata-log-*.json
    - processed/YYYY/MM/DD/HH-MM-SS/YYYY-mm-dd_SearchKeywordPerformance.tab
    - logs/YYYY/MM/DD/HH-MM-SS/run-summary.json
    - logs/YYYY/MM/DD/HH-MM-SS/pipeline.log

    Skip logic:
    - If the same file content has already been processed before,
      pipeline is skipped based on SHA-256 checksum match.
    """
    logger.info("Received event: %s", json.dumps(event))

    output_prefix = os.environ.get("OUTPUT_PREFIX", "processed/")
    log_prefix = os.environ.get("LOG_PREFIX", "logs/")
    metadata_prefix = os.environ.get("METADATA_PREFIX", "metadata/")
    checksum_prefix = os.environ.get("CHECKSUM_PREFIX", "checksums/")

    file_logger = None
    file_handler = None

    try:
        config = load_metadata_config()
        delimiter = config.get("delimiter", "\t")
        expected_columns = config.get("expected_columns", [])

        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        # Example key:
        # raw/2026/03/12/18-00-01/sample data.sql
        key_parts = key.split("/")
        timestamp_path = ""
        if len(key_parts) >= 6:
            timestamp_path = "/".join(key_parts[1:-1])

        with tempfile.TemporaryDirectory() as tmpdir:
            local_input = os.path.join(tmpdir, "input_file")
            local_pipeline_log = os.path.join(tmpdir, "pipeline.log")

            file_logger, file_handler = setup_file_logger(local_pipeline_log)

            file_logger.info("Pipeline started")
            file_logger.info("Source bucket: %s", bucket)
            file_logger.info("Source key: %s", key)

            # Download source file
            s3_client.download_file(bucket, key, local_input)
            file_logger.info("Input file downloaded to local temp path: %s", local_input)

            # -------------------------------------------------------------
            # Step 0: Compute checksum and skip if already processed
            # -------------------------------------------------------------
            file_checksum = compute_file_sha256(local_input)
            checksum_key = f"{checksum_prefix}{file_checksum}.json"

            file_logger.info("Computed SHA-256 checksum: %s", file_checksum)
            file_logger.info("Checksum registry key: s3://%s/%s", bucket, checksum_key)

            if checksum_exists(bucket, checksum_key):
                file_logger.info("Duplicate file content detected. Pipeline will be skipped.")

                skipped_summary = {
                    "input_file": key,
                    "source_bucket": bucket,
                    "source_key": key,
                    "checksum": file_checksum,
                    "status": "SKIPPED_NO_CHANGE",
                    "reason": "Source file content already processed earlier",
                    "processed_at_utc": datetime.utcnow().isoformat()
                }

                if timestamp_path:
                    summary_key = f"{log_prefix}{timestamp_path}/run-summary.json"
                    pipeline_log_key = f"{log_prefix}{timestamp_path}/pipeline.log"
                else:
                    summary_key = (
                        f"{log_prefix}run-summary-{datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')}.json"
                    )
                    pipeline_log_key = (
                        f"{log_prefix}pipeline-{datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')}.log"
                    )

                write_json_to_s3(bucket, summary_key, skipped_summary)
                file_logger.info("Skipped run summary uploaded to s3://%s/%s", bucket, summary_key)

                file_logger.info("Pipeline skipped successfully")

                file_handler.flush()
                file_handler.close()
                file_logger.removeHandler(file_handler)

                s3_client.upload_file(local_pipeline_log, bucket, pipeline_log_key)
                logger.info("Pipeline log uploaded to s3://%s/%s", bucket, pipeline_log_key)

                return {
                    "statusCode": 200,
                    "body": json.dumps({
                        "message": "Pipeline skipped because source data has not changed",
                        "input_key": key,
                        "summary_key": summary_key,
                        "pipeline_log_key": pipeline_log_key,
                        "checksum": file_checksum
                    })
                }

            # -------------------------------------------------------------
            # Step 1: Metadata profiling
            # -------------------------------------------------------------
            profiler = MetadataProfiler(
                input_file=local_input,
                delimiter=delimiter,
                expected_columns=expected_columns
            )
            metadata_report = profiler.profile(bucket, key)

            file_logger.info("Metadata profiling completed")
            file_logger.info("Total rows: %s", metadata_report.get("total_rows"))
            file_logger.info("Number of columns: %s", metadata_report.get("number_of_columns"))
            file_logger.info("Metadata mismatch: %s", metadata_report.get("metadata_mismatch"))

            metadata_filename = (
                f"metadata-log-{datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')}.json"
            )

            if timestamp_path:
                metadata_key = f"{metadata_prefix}{timestamp_path}/{metadata_filename}"
            else:
                metadata_key = f"{metadata_prefix}{metadata_filename}"

            write_json_to_s3(bucket, metadata_key, metadata_report)
            file_logger.info("Metadata file uploaded to s3://%s/%s", bucket, metadata_key)

            # -------------------------------------------------------------
            # Step 2: Business processing
            # -------------------------------------------------------------
            processor = SearchKeywordRevenueProcessor(local_input)
            local_output = processor.write_output(tmpdir)
            summary = processor.summary()

            # Add checksum and source identifiers into summary
            summary["source_bucket"] = bucket
            summary["source_key"] = key
            summary["checksum"] = file_checksum

            file_logger.info("Business processing completed")
            file_logger.info("Records processed: %s", summary.get("records_processed"))
            file_logger.info("Purchase rows: %s", summary.get("purchase_rows"))
            file_logger.info("Attributed purchases: %s", summary.get("attributed_purchases"))
            file_logger.info("Unattributed purchases: %s", summary.get("unattributed_purchases"))

            output_filename = os.path.basename(local_output)

            if timestamp_path:
                output_key = f"{output_prefix}{timestamp_path}/{output_filename}"
                summary_key = f"{log_prefix}{timestamp_path}/run-summary.json"
                pipeline_log_key = f"{log_prefix}{timestamp_path}/pipeline.log"
            else:
                output_key = f"{output_prefix}{output_filename}"
                summary_key = (
                    f"{log_prefix}run-summary-{datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')}.json"
                )
                pipeline_log_key = (
                    f"{log_prefix}pipeline-{datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')}.log"
                )

            # Upload final output
            s3_client.upload_file(local_output, bucket, output_key)
            file_logger.info("Output file uploaded to s3://%s/%s", bucket, output_key)

            # Upload run summary
            write_json_to_s3(bucket, summary_key, summary)
            file_logger.info("Run summary uploaded to s3://%s/%s", bucket, summary_key)

            # Save checksum registry so future duplicate content can be skipped
            checksum_registry_payload = {
                "checksum": file_checksum,
                "source_bucket": bucket,
                "source_key": key,
                "first_processed_at_utc": datetime.utcnow().isoformat(),
                "output_key": output_key,
                "summary_key": summary_key,
                "metadata_key": metadata_key
            }
            write_json_to_s3(bucket, checksum_key, checksum_registry_payload)
            file_logger.info("Checksum registry written to s3://%s/%s", bucket, checksum_key)

            file_logger.info("Pipeline completed successfully")

            # Flush and close before uploading pipeline.log
            file_handler.flush()
            file_handler.close()
            file_logger.removeHandler(file_handler)

            s3_client.upload_file(local_pipeline_log, bucket, pipeline_log_key)
            logger.info("Pipeline log uploaded to s3://%s/%s", bucket, pipeline_log_key)

            logger.info("Successfully processed file %s", key)
            logger.info("Metadata written to s3://%s/%s", bucket, metadata_key)
            logger.info("Output written to s3://%s/%s", bucket, output_key)
            logger.info("Summary written to s3://%s/%s", bucket, summary_key)

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Processing complete",
                    "input_key": key,
                    "metadata_key": metadata_key,
                    "output_key": output_key,
                    "summary_key": summary_key,
                    "pipeline_log_key": pipeline_log_key,
                    "checksum_key": checksum_key,
                    "checksum": file_checksum
                })
            }

    except Exception as e:
        error_message = (
            f"Pipeline failed for input file {key if 'key' in locals() else ''}. "
            f"Error: {str(e)}"
        )
        logger.exception(error_message)

        if file_logger:
            try:
                file_logger.exception(error_message)
                if file_handler:
                    file_handler.flush()
                    file_handler.close()
                    file_logger.removeHandler(file_handler)
            except Exception:
                pass

        publish_failure(
            subject="AWS Pipeline Failure: Search Keyword Revenue",
            message=error_message
        )
        raise