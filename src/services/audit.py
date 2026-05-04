def _sql_timestamp_or_null(value):
    if value is None:
        return "NULL"
    
    return f"TIMESTAMP '{value.isoformat(sep=' ', timespec='seconds')}'"


def insert_run_log_start(
    spark,
    run_logs_table: str,
    pipeline_name: str,
    dataset: str,
    target_table: str,
    run_id: str,
) -> None:
    spark.sql(f"""
        INSERT INTO {run_logs_table} (
            pipeline_name,
            dataset,
            target_table,
            run_id,
            started_at,
            status
        )
        VALUES (
            '{pipeline_name}',
            '{dataset}',
            '{target_table}',
            '{run_id}',
            current_timestamp(),
            'RUNNING'
        )
    """)


def update_run_log_success(
    spark,
    run_logs_table: str,
    pipeline_name: str,
    dataset: str,
    run_id: str,
    dq_result: str,
    rows_in: int,
    rows_out: int,
    rows_quarantined: int,
    last_watermark_ts,
) -> None:
    spark.sql(f"""
        UPDATE {run_logs_table}
        SET finished_at = current_timestamp(),
            status = 'SUCCESS',
            dq_result = '{dq_result}',
            rows_in = {rows_in},
            rows_out = {rows_out},
            rows_quarantined = {rows_quarantined},
            last_watermark_ts = {_sql_timestamp_or_null(last_watermark_ts)}
        WHERE pipeline_name = '{pipeline_name}'
          AND dataset = '{dataset}'
          AND run_id = '{run_id}'
    """)


def update_run_log_no_new_data(
    spark,
    run_logs_table: str,
    pipeline_name: str,
    dataset: str,
    run_id: str,
    last_watermark_ts,
) -> None:
    spark.sql(f"""
        UPDATE {run_logs_table}
        SET finished_at = current_timestamp(),
            status = 'SUCCESS',
            dq_result = 'SKIPPED_NO_NEW_DATA',
            rows_in = 0,
            rows_out = 0,
            rows_quarantined = 0,
            last_watermark_ts = {_sql_timestamp_or_null(last_watermark_ts)}
        WHERE pipeline_name = '{pipeline_name}'
          AND dataset = '{dataset}'
          AND run_id = '{run_id}'
    """)


def update_run_log_failure(
    spark,
    run_logs_table: str,
    pipeline_name: str,
    dataset: str,
    run_id: str,
    error_msg: str,
    rows_in: int,
    rows_out: int,
    rows_quarantined: int,
    dq_result: str,
    last_watermark_ts,
) -> None:
    safe_msg = str(error_msg).replace("'", "''")
    spark.sql(f"""
        UPDATE {run_logs_table}
        SET finished_at = current_timestamp(),
            status = 'FAILED',
            error_msg = '{safe_msg}',
            rows_in = {rows_in},
            rows_out = {rows_out},
            rows_quarantined = {rows_quarantined},
            dq_result = '{dq_result}',
            last_watermark_ts = {_sql_timestamp_or_null(last_watermark_ts)}
        WHERE pipeline_name = '{pipeline_name}'
          AND dataset = '{dataset}'
          AND run_id = '{run_id}'
    """)