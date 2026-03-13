import sys
import json
import hashlib
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# ============================================================
# FIXED CONFIGURATION
# Only one runtime argument is accepted: --input_key
# ============================================================
BUCKET = "analytics-hitdata-s3"
PROCESSED_PREFIX = "processed/"
METADATA_PREFIX = "metadata/"
LOGS_PREFIX = "logs/"
CHECKSUMS_PREFIX = "checksums/"
CONFIG_FILE = "metadata_config.json"


# ============================================================
# JOB ARGUMENTS
# Required runtime argument:
# --input_key raw/YYYY/MM/DD/HH-MM-SS/input_file.tab
# ============================================================
args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_key"])
JOB_NAME = args["JOB_NAME"]
INPUT_KEY = args["input_key"]


# ============================================================
# GLUE / SPARK SETUP
# ============================================================
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(JOB_NAME, args)

s3 = boto3.client("s3")

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", "200")


# ============================================================
# SIMPLE PIPELINE LOGGER
# Logs go to CloudWatch via print() and also to S3 pipeline.log
# ============================================================
PIPELINE_LOG_LINES = []


def log(message: str) -> None:
    line = f"{datetime.utcnow().isoformat()} INFO {message}"
    PIPELINE_LOG_LINES.append(line)
    print(line)


def upload_json(key: str, payload: dict) -> None:
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json"
    )


def upload_text(key: str, text: str) -> None:
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType="text/plain"
    )


# ============================================================
# HELPERS
# ============================================================
def load_metadata_config(file_name: str) -> dict:
    # Reads the config file from the local Glue runtime.
    # If you later store it in S3, replace this with s3.get_object().
    with open(file_name, "r", encoding="utf-8") as f:
        return json.load(f)


def compute_checksum(bucket: str, key: str) -> str:
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        raise ValueError(f"S3 source file not found: s3://{bucket}/{key}") from e

    hasher = hashlib.sha256()
    while True:
        chunk = obj["Body"].read(8 * 1024 * 1024)
        if not chunk:
            break
        hasher.update(chunk)
    return hasher.hexdigest()


def checksum_exists(checksum_key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=checksum_key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def move_single_part_file(temp_prefix: str, final_key: str) -> None:
    """
    Spark writes CSV output as a folder with part files.
    This copies the single part file to the final named object.
    """
    paginator = s3.get_paginator("list_objects_v2")
    objects = []

    for page in paginator.paginate(Bucket=BUCKET, Prefix=temp_prefix):
        objects.extend(page.get("Contents", []))

    part_key = None
    for obj in objects:
        key = obj["Key"]
        if "part-" in key:
            part_key = key
            break

    if not part_key:
        raise ValueError(f"No Spark part file found under s3://{BUCKET}/{temp_prefix}")

    s3.copy_object(
        Bucket=BUCKET,
        CopySource={"Bucket": BUCKET, "Key": part_key},
        Key=final_key
    )

    delete_items = [{"Key": obj["Key"]} for obj in objects]
    if delete_items:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": delete_items})


def build_s3_keys(input_key: str) -> dict:
    """
    Preserve the timestamp folder structure from raw/YYYY/MM/DD/HH-MM-SS/file
    """
    key_parts = input_key.split("/")
    timestamp_path = "/".join(key_parts[1:-1]) if len(key_parts) >= 6 else ""

    current_date = datetime.utcnow().strftime("%Y-%m-%d")
    current_ts = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")

    return {
        "timestamp_path": timestamp_path,
        "metadata_key": f"{METADATA_PREFIX}{timestamp_path}/metadata-log-{current_ts}.json",
        "summary_key": f"{LOGS_PREFIX}{timestamp_path}/run-summary.json",
        "pipeline_log_key": f"{LOGS_PREFIX}{timestamp_path}/pipeline.log",
        "output_key": f"{PROCESSED_PREFIX}{timestamp_path}/{current_date}_SearchKeywordPerformance.tab",
        "temp_output_prefix": f"{PROCESSED_PREFIX}{timestamp_path}/_tmp_output/"
    }


# ============================================================
# METADATA PROFILER CLASS
# ============================================================
class MetadataProfiler:
    """
    Profiles delimited text source metadata.

    Supported config formats:
    1) ["col1", "col2", "col3"]
    2) [{"name": "col1", "type": "int"}, {"name": "col2", "type": "datetime"}]

    Actual source values are still profiled as strings because the input
    file is read as text. Expected data types are taken from config only.
    """

    def __init__(self, expected_columns):
        self.expected_columns = expected_columns or []
        self.expected_names = []
        self.expected_defs = {}

        for col in self.expected_columns:
            if isinstance(col, str):
                self.expected_names.append(col)
                self.expected_defs[col] = {"type": "string"}
            elif isinstance(col, dict) and col.get("name"):
                name = col["name"]
                self.expected_names.append(name)
                self.expected_defs[name] = {"type": col.get("type", "string")}

    def profile(self, df, source_key: str) -> dict:
        columns = df.columns
        total_rows = df.count()

        missing = [c for c in self.expected_names if c not in columns]
        extra = [c for c in columns if c not in self.expected_names]

        mismatch_details = []
        if missing:
            mismatch_details.append({"missing_columns": missing})
        if extra:
            mismatch_details.append({"extra_columns": extra})
        if self.expected_names and len(columns) != len(self.expected_names):
            mismatch_details.append({
                "column_count_mismatch": {
                    "expected": len(self.expected_names),
                    "actual": len(columns)
                }
            })

        # Single aggregation pass for min/max/null counts across all columns.
        agg_exprs = []
        for col_name in columns:
            agg_exprs.extend([
                F.min(F.col(col_name)).alias(f"{col_name}__min"),
                F.max(F.col(col_name)).alias(f"{col_name}__max"),
                F.sum(
                    F.when(
                        F.col(col_name).isNull() | (F.trim(F.col(col_name)) == ""),
                        1
                    ).otherwise(0)
                ).alias(f"{col_name}__nulls")
            ])

        stats_row = df.agg(*agg_exprs).collect()[0].asDict()

        # Distinct counts are computed in one separate pass.
        distinct_exprs = [
            F.countDistinct(F.col(col_name)).alias(col_name)
            for col_name in columns
        ]
        distinct_row = df.agg(*distinct_exprs).collect()[0].asDict()

        profiles = []
        for col_name in columns:
            expected_type = self.expected_defs.get(col_name, {}).get("type", "string")

            profiles.append({
                "column_name": col_name,
                "actual_profiled_data_type": "string",
                "expected_data_type": expected_type,
                "min_value": stats_row.get(f"{col_name}__min"),
                "max_value": stats_row.get(f"{col_name}__max"),
                "total_nulls": int(stats_row.get(f"{col_name}__nulls", 0)),
                "total_unique_values": int(distinct_row.get(col_name, 0))
            })

        return {
            "source_bucket": BUCKET,
            "source_key": source_key,
            "profiled_at_utc": datetime.utcnow().isoformat(),
            "number_of_columns": len(columns),
            "column_names": columns,
            "total_rows": total_rows,
            "metadata_mismatch": len(mismatch_details) > 0,
            "metadata_mismatch_details": mismatch_details,
            "columns": profiles
        }


# ============================================================
# BUSINESS PROCESSOR CLASS
# ============================================================
class SearchKeywordRevenueProcessor:
    """
    Answers the client business question:

    How much revenue is the client getting from external Search Engines,
    such as Google, Yahoo and MSN, and which keywords are performing
    the best based on revenue?

    Logic:
    - detect external search engine from referrer
    - extract search keyword from referrer query string
    - detect purchase rows from event_list
    - extract revenue from product_list
    - attribute purchase to the most recent external search touchpoint
      for the same IP
    - aggregate revenue by search engine + keyword
    """

    @staticmethod
    def transform(df):
        required_cols = ["ip", "referrer", "event_list", "product_list", "date_time"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns for business processing: {missing}")

        # Convert event time once so ordering/comparison is chronological, not string-based.
        base_df = (
            df.withColumn("event_ts", F.to_timestamp(F.col("date_time")))
              .withColumn("referrer_lc", F.lower(F.trim(F.col("referrer"))))
              .withColumn("event_list_clean", F.regexp_replace(F.coalesce(F.col("event_list"), F.lit("")), r"\s+", ""))
        )

        # Extract host/domain from referrer using Spark native functions.
        # This avoids Python UDF overhead and is much faster in Glue.
        base_df = base_df.withColumn(
            "referrer_domain",
            F.lower(F.regexp_extract(F.col("referrer_lc"), r"^(?:https?://)?([^/?#]+)", 1))
        )

        # Identify supported external search engines.
        base_df = base_df.withColumn(
            "search_engine_domain",
            F.when(F.col("referrer_domain").contains("google."), F.col("referrer_domain"))
             .when(F.col("referrer_domain").contains("search.yahoo."), F.col("referrer_domain"))
             .when(F.col("referrer_domain").contains("yahoo."), F.col("referrer_domain"))
             .when(F.col("referrer_domain").contains("search.msn."), F.col("referrer_domain"))
             .when(F.col("referrer_domain").contains("msn."), F.col("referrer_domain"))
             .when(F.col("referrer_domain").contains("bing."), F.col("referrer_domain"))
        )

        # Extract search keyword from query string.
        # Google/Bing/MSN typically use q=, Yahoo typically uses p=.
        base_df = base_df.withColumn(
            "search_keyword_raw",
            F.when(
                F.col("referrer_domain").contains("yahoo."),
                F.regexp_extract(F.col("referrer_lc"), r"[?&]p=([^&]+)", 1)
            ).otherwise(
                F.regexp_extract(F.col("referrer_lc"), r"[?&]q=([^&]+)", 1)
            )
        )

        # Replace + with spaces. This handles the most common encoded search format.
        base_df = base_df.withColumn(
            "search_keyword",
            F.when(
                F.trim(F.col("search_keyword_raw")) != "",
                F.lower(F.regexp_replace(F.col("search_keyword_raw"), r"\+", " "))
            )
        )

        # Purchase is event code 1. Split by comma first so "1" does not match "10" or "100".
        base_df = base_df.withColumn(
            "is_purchase",
            F.array_contains(F.split(F.col("event_list_clean"), ","), "1")
        )

        # Product list format is semicolon-delimited per item, with revenue in the 4th token.
        # Example item structure typically looks like: category;product;qty;revenue;...
        base_df = base_df.withColumn(
            "revenue",
            F.expr("""
                aggregate(
                    filter(
                        split(coalesce(product_list, ''), ','),
                        x -> x is not null and trim(x) <> ''
                    ),
                    cast(0.0 as double),
                    (acc, x) -> acc + coalesce(try_cast(element_at(split(x, ';'), 4) as double), 0.0)
                )
            """)
        )

        enriched = base_df.cache()

        search_touchpoints = (
            enriched.filter(
                F.col("search_engine_domain").isNotNull() &
                F.col("search_keyword").isNotNull() &
                F.col("event_ts").isNotNull()
            )
            .select(
                F.col("ip").alias("s_ip"),
                F.col("event_ts").alias("s_time"),
                F.col("search_engine_domain"),
                F.col("search_keyword")
            )
        )

        purchases = (
            enriched.filter(
                F.col("is_purchase") &
                (F.col("revenue") > 0) &
                F.col("event_ts").isNotNull()
            )
            .select(
                F.col("ip").alias("p_ip"),
                F.col("event_ts").alias("p_time"),
                F.col("revenue")
            )
        )

        joined = purchases.join(
            search_touchpoints,
            (purchases["p_ip"] == search_touchpoints["s_ip"]) &
            (search_touchpoints["s_time"] <= purchases["p_time"]),
            "left"
        )

        window_spec = Window.partitionBy("p_ip", "p_time", "revenue").orderBy(F.col("s_time").desc())

        result = (
            joined.withColumn("rn", F.row_number().over(window_spec))
                  .filter(F.col("rn") == 1)
                  .filter(F.col("search_engine_domain").isNotNull())
                  .groupBy(
                      F.col("search_engine_domain").alias("Search Engine Domain"),
                      F.col("search_keyword").alias("Search Keyword")
                  )
                  .agg(F.round(F.sum("revenue"), 2).alias("Revenue"))
                  .orderBy(F.desc("Revenue"))
        )

        return result


def main():
    print(f"DEBUG INPUT_KEY = {INPUT_KEY}")
    print(f"DEBUG SOURCE PATH = s3://{BUCKET}/{INPUT_KEY}")
    print(f"DEBUG CONFIG FILE = {CONFIG_FILE}")

    keys = build_s3_keys(INPUT_KEY)
    source_path = f"s3://{BUCKET}/{INPUT_KEY}"

    log(f"Starting Glue job for source file: {source_path}")

    # --------------------------------------------------------
    # Step 0: checksum / skip unchanged data
    # --------------------------------------------------------
    checksum = compute_checksum(BUCKET, INPUT_KEY)
    checksum_key = f"{CHECKSUMS_PREFIX}{checksum}.json"
    log(f"Computed source checksum: {checksum}")

    if checksum_exists(checksum_key):
        skipped_summary = {
            "status": "SKIPPED_NO_CHANGE",
            "source_bucket": BUCKET,
            "source_key": INPUT_KEY,
            "checksum": checksum,
            "processed_at_utc": datetime.utcnow().isoformat()
        }
        upload_json(keys["summary_key"], skipped_summary)
        log("Pipeline skipped because source data has not changed.")
        upload_text(
            keys["pipeline_log_key"],
            "\n".join(PIPELINE_LOG_LINES + ["Pipeline skipped: no data change detected."])
        )
        job.commit()
        return

    # --------------------------------------------------------
    # Step 1: read source file as tab-delimited text
    # --------------------------------------------------------
    df = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .csv(source_path)
    )

    # Keep all columns as strings for metadata profiling requirements.
    for col_name in df.columns:
        df = df.withColumn(col_name, F.col(col_name).cast("string"))

    df = df.cache()
    source_row_count = df.count()

    log(f"Source file read successfully. Columns found: {len(df.columns)}")
    log(f"Source row count: {source_row_count}")

    # --------------------------------------------------------
    # Step 2: metadata profiling
    # --------------------------------------------------------
    config = load_metadata_config(CONFIG_FILE)
    profiler = MetadataProfiler(config.get("expected_columns", []))
    metadata_report = profiler.profile(df, INPUT_KEY)
    upload_json(keys["metadata_key"], metadata_report)
    log(f"Metadata log written to s3://{BUCKET}/{keys['metadata_key']}")

    # --------------------------------------------------------
    # Step 3: business processing
    # --------------------------------------------------------
    processor = SearchKeywordRevenueProcessor()
    result_df = processor.transform(df).cache()
    output_row_count = result_df.count()

    log("Business transformation completed successfully.")
    log(f"Output row count: {output_row_count}")

    # --------------------------------------------------------
    # Step 4: write final output
    # Spark writes to folder; then rename to required final file
    # --------------------------------------------------------
    temp_output_s3 = f"s3://{BUCKET}/{keys['temp_output_prefix']}"

    (
        result_df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("sep", "\t")
        .csv(temp_output_s3)
    )

    move_single_part_file(keys["temp_output_prefix"], keys["output_key"])
    log(f"Final output written to s3://{BUCKET}/{keys['output_key']}")

    # --------------------------------------------------------
    # Step 5: write checksum registry
    # --------------------------------------------------------
    upload_json(checksum_key, {
        "checksum": checksum,
        "source_bucket": BUCKET,
        "source_key": INPUT_KEY,
        "output_key": keys["output_key"],
        "metadata_key": keys["metadata_key"],
        "first_processed_at_utc": datetime.utcnow().isoformat()
    })

    # --------------------------------------------------------
    # Step 6: write run summary
    # --------------------------------------------------------
    summary = {
        "status": "SUCCESS",
        "source_bucket": BUCKET,
        "source_key": INPUT_KEY,
        "checksum": checksum,
        "records_processed": source_row_count,
        "output_rows": output_row_count,
        "processed_at_utc": datetime.utcnow().isoformat()
    }
    upload_json(keys["summary_key"], summary)

    # --------------------------------------------------------
    # Step 7: write pipeline log
    # --------------------------------------------------------
    log("Glue pipeline completed successfully.")
    upload_text(
        keys["pipeline_log_key"],
        "\n".join(PIPELINE_LOG_LINES + ["Pipeline completed successfully."])
    )

    job.commit()


try:
    main()
except Exception as e:
    error_text = str(e)
    log(f"Pipeline failed: {error_text}")

    try:
        keys = build_s3_keys(INPUT_KEY)
        upload_json(keys["summary_key"], {
            "status": "FAILED",
            "source_bucket": BUCKET,
            "source_key": INPUT_KEY,
            "error": error_text,
            "processed_at_utc": datetime.utcnow().isoformat()
        })
        upload_text(
            keys["pipeline_log_key"],
            "\n".join(PIPELINE_LOG_LINES + [f"Pipeline failed: {error_text}"])
        )
    except Exception:
        pass

    raise