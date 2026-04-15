import uuid
import logging
import os
import requests
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


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
        url: Path,
        params: dict[str, Any],
        headers: dict[str, str],
        timeout_s: int,
        max_retries: int,
        max_backoff_s: int,        
) -> dict[str, Any]:
    
    for attempt in range(max_retries + 1):
        resp = session.get(url, params=params, headers=headers, timeout=timeout_s)

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            sleep_s = int(retry_after) if (retry_after & retry_after.isdigit()) else _backoff_seconds(attempt, max_backoff_s)
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
        extra_params: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    
    if not token:
        raise ValueError("Missing API token")
    
    url = base_url.rstrip("/") + "/" + endpoint.rstrip("/")

    run_id = uuid.uuid4().hex
    run_dir = os.path.join(landing_dir, run_id)
    os.makedirs(run_dir, exist_ok=True)

    started_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    headers = {"Authorization" : f"bearer: {token.strip()}",
               "Accept" : "application/json"
               }
    
    params: dict[str, Any] = {spec.limit_param : spec.page_size}

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
                params=params,
                headers=headers,
                timeout_s=timeout_s,
                max_retries=max_retries,
                max_backoff_s=max_backoff_s
            )

            items = payload.get()








    




