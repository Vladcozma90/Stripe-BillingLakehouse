from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StripeCursorSpec:
    page_size: int = 100
    limit_param: str = "limit"
    cursor_param: str = "starting_after"
    items_field: str = "data"
    has_more_field: str = "has_more"
    id_field: str = "id"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _backoff_seconds(attempt: int, max_backoff_s: int) -> int:
    return min(max_backoff_s, 2**attempt)


def _fetch_page_json(
    session: requests.Session,
    url: str,
    headers: Dict[str, str],
    params: Dict[str, Any],
    timeout_s: int,
    max_retries: int,
    max_backoff_s: int,
) -> Dict[str, Any]:
    for attempt in range(max_retries + 1):
        try:
            response = session.get(
                url=url,
                params=params,
                headers=headers,
                timeout=timeout_s,
            )

            if response.status_code == 429:
                if attempt >= max_retries:
                    response.raise_for_status()

                retry_after = response.headers.get("Retry-After")
                sleep_s = (int(retry_after) if retry_after and retry_after.isdigit() else _backoff_seconds(attempt, max_backoff_s))

                logger.warning(
                    "Stripe rate limit | status=%s | attempt=%s/%s | sleep_s=%s",
                    response.status_code,
                    attempt + 1,
                    max_retries,
                    sleep_s,
                )
                time.sleep(sleep_s)
                continue

            if response.status_code in (500, 502, 503, 504):
                if attempt >= max_retries:
                    response.raise_for_status()

                sleep_s = _backoff_seconds(attempt, max_backoff_s)
                logger.warning(
                    "Stripe server error | status=%s | attempt=%s/%s | sleep_s=%s",
                    response.status_code,
                    attempt + 1,
                    max_retries,
                    sleep_s,
                )
                time.sleep(sleep_s)
                continue

            response.raise_for_status()
            return response.json()

        except requests.RequestException:
            if attempt >= max_retries:
                logger.exception(
                    "Stripe request failed after retries | url=%s | params=%s",
                    url,
                    params,
                )
                raise

            sleep_s = _backoff_seconds(attempt, max_backoff_s)
            logger.warning(
                "Stripe request failed, retrying | attempt=%s/%s | sleep_s=%s",
                attempt + 1,
                max_retries,
                sleep_s,
                exc_info=True,
            )
            time.sleep(sleep_s)

    raise RuntimeError("Failed to fetch Stripe page after retries.")


def _write_json_lines_with_spark(
    *,
    spark: SparkSession,
    json_lines: list[str],
    output_path: str,
) -> None:
    if not json_lines:
        return

    df = spark.createDataFrame([(line,) for line in json_lines], ["value"])

    (
        df.write
        .mode("overwrite")
        .text(output_path)
    )


def extract_stripe_list_to_landing(
    *,
    spark: SparkSession,
    base_url: str,
    endpoint: str,
    landing_dir: str,
    token: str,
    spec: StripeCursorSpec = StripeCursorSpec(),
    timeout_s: int = 60,
    max_retries: int = 5,
    max_backoff_s: int = 60,
    extra_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Extracts a cursor-paginated Stripe list endpoint and writes JSON lines to landing.

    Output layout:

        <landing_dir>/run_id=<run_id>/
            part-*.txt

    Each line is a valid JSON object:

        {
          "_extracted_at": "...",
          "_stripe_endpoint": "...",
          "_stripe_page_index": 0,
          "data": {...}
        }
    """

    if not token or not token.strip():
        raise ValueError("Missing API token.")

    if spec.page_size <= 0:
        raise ValueError(f"Invalid page_size: {spec.page_size}")

    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"

    run_id = uuid.uuid4().hex
    run_path = f"{landing_dir.rstrip('/')}/run_id={run_id}"

    started_at = _utc_now_iso()

    headers = {
        "Authorization": f"Bearer {token.strip()}",
        "Accept": "application/json",
    }

    base_params: Dict[str, Any] = {
        spec.limit_param: spec.page_size,
    }

    if extra_params:
        base_params.update(extra_params)

    next_cursor: Optional[str] = None
    page_index = 0
    total_records = 0
    last_cursor: Optional[str] = None
    json_lines: list[str] = []

    logger.info(
        "Stripe extract start | url=%s | run_id=%s | landing_path=%s",
        url,
        run_id,
        run_path,
    )

    session = requests.Session()

    try:
        while True:
            params = dict(base_params)

            if next_cursor:
                params[spec.cursor_param] = next_cursor

            payload = _fetch_page_json(
                session=session,
                url=url,
                headers=headers,
                params=params,
                timeout_s=timeout_s,
                max_retries=max_retries,
                max_backoff_s=max_backoff_s,
            )

            items = payload.get(spec.items_field, [])
            has_more = bool(payload.get(spec.has_more_field, False))

            if not isinstance(items, list):
                raise RuntimeError(
                    f"Expected Stripe response field '{spec.items_field}' to be a list. "
                    f"Endpoint: {endpoint}"
                )

            extracted_at = _utc_now_iso()

            for item in items:
                json_lines.append(
                    json.dumps(
                        {
                            "_extracted_at": extracted_at,
                            "_stripe_endpoint": endpoint,
                            "_stripe_page_index": page_index,
                            "data": item,
                        },
                        ensure_ascii=False,
                    )
                )

            total_records += len(items)

            logger.info(
                "Stripe page extracted | endpoint=%s | page_index=%s | records=%s | has_more=%s",
                endpoint,
                page_index,
                len(items),
                has_more,
            )

            page_index += 1

            if not has_more:
                break

            if not items:
                raise RuntimeError(
                    f"Stripe returned has_more=true but no items for endpoint '{endpoint}'. "
                    "Cannot continue cursor pagination safely."
                )

            last_item = items[-1]

            if not isinstance(last_item, dict) or spec.id_field not in last_item:
                raise RuntimeError(
                    f"Cannot paginate Stripe endpoint '{endpoint}': "
                    f"missing '{spec.id_field}' in last item."
                )

            last_cursor = str(last_item[spec.id_field])
            next_cursor = last_cursor

        _write_json_lines_with_spark(
            spark=spark,
            json_lines=json_lines,
            output_path=run_path,
        )

        finished_at = _utc_now_iso()

        manifest = {
            "run_id": run_id,
            "url": url,
            "landing_path": run_path,
            "started_at_utc": started_at,
            "finished_at_utc": finished_at,
            "page_size": spec.page_size,
            "pages": page_index,
            "records": total_records,
            "last_cursor": last_cursor,
        }

        logger.info(
            "Stripe extract done | endpoint=%s | records=%s | pages=%s | run_id=%s | landing_path=%s",
            endpoint,
            total_records,
            page_index,
            run_id,
            run_path,
        )

        return manifest

    finally:
        session.close()