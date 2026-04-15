from __future__ import annotations

import os
import json
import time
import uuid
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StripeCursorSpec:
    page_size: int = 100
    limit_param: str = "limit"
    cursor_param: str = "starting_after"
    items_field: str = "data"
    has_more_field: str = "has_more"
    id_field: str = "id"


def _backoff_seconds(attempt: int, max_backoff_s: int) -> int:
    return min(max_backoff_s, 2** attempt)


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
        resp = session.get(url, params=params, headers=headers, timeout=timeout_s)

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            sleep_s = int(retry_after) if (retry_after and retry_after.isdigit()) else _backoff_seconds(attempt, max_backoff_s)
            logger.warning("429 rate limit. Sleeping %ss", sleep_s)
            time.sleep(sleep_s)
            continue

        if resp.status_code in (500, 502, 503, 504):
            sleep_s = _backoff_seconds(attempt, max_backoff_s)
            logger.warning("Server error %s. Retrying in %ss", resp.status_code, sleep_s)
            time.sleep(sleep_s)
            continue

        resp.raise_for_status()
        return resp.json()

    raise RuntimeError("Failed to fetch page after retries")


def extract_stripe_list_to_landing(
    *,
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
    Output:
      <landing_dir>/run_id=<run_id>/
        part-00000-<uuid>.jsonl
        part-00001-<uuid>.jsonl
        ...
        manifest.json
    """
    if not token or not token.strip():
        raise ValueError("Missing API token")

    url = base_url.rstrip("/") + "/" + endpoint.lstrip("/")

    run_id = uuid.uuid4().hex
    run_dir = os.path.join(landing_dir, f"run_id={run_id}")
    os.makedirs(run_dir, exist_ok=True)

    started_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"


    headers = {"Authorization": f"Bearer {token.strip()}",
                "Accept": "application/json",
            }

    params: Dict[str, Any] = {spec.limit_param: spec.page_size}
    if extra_params:
        params.update(extra_params)

    next_cursor: Optional[str] = None
    page_index = 0
    total_records = 0
    part_files: list[str] = []

    logger.info("Extract start | url=%s | run_id=%s", url, run_id)

    session = requests.Session()
    try:
        while True:

            if next_cursor is None:
                params.pop(spec.cursor_param, None)
            else:
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
                raise RuntimeError(f"Expected '{spec.items_field}' to be a list")

            extracted_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            part_name = f"part-{page_index:05d}-{uuid.uuid4().hex}.jsonl"
            part_path = os.path.join(run_dir, part_name)

            with open(part_path, "w", encoding="utf-8") as out:
                for i in items:
                    out.write(json.dumps({"_extracted_at": extracted_at, "data": i}, ensure_ascii=False) + "\n")

            part_files.append(part_name)
            total_records += len(items)
            page_index += 1

            if not items or not has_more:
                break

            last_item = items[-1]
            if not isinstance(last_item, dict) or spec.id_field not in last_item:
                raise RuntimeError(f"Cannot paginate: missing '{spec.id_field}' in last item")
            last_item_id = str(last_item[spec.id_field])
            next_cursor = last_item_id

        finished_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

        manifest = {
            "run_id": run_id,
            "url": url,
            "started_at_utc": started_at,
            "finished_at_utc": finished_at,
            "page_size": spec.page_size,
            "pages": page_index,
            "records": total_records,
            "parts": part_files,
            "last_cursor": next_cursor,
        }

        with open(os.path.join(run_dir, "manifest.json"), "w", encoding="utf-8") as mf:
            json.dump(manifest, mf, indent=2)

        logger.info("Extract done | records=%d | pages=%d | run_id=%s", total_records, page_index, run_id)
        return manifest

    finally:
        session.close()