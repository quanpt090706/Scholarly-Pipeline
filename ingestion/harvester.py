"""Works Harvester — fetch /works với cursor pagination + retry + checkpoint.

Luồng (theo 01_ingestion.md):
    OpenAlex API -> fetch_page(cursor) -> save_raw(work_id.json) -> save_state(cursor)

Kỷ luật đơn giản-nhưng-chừa-seam:
- harvest_works() nhận client/store truyền vào (DI thủ công). Test truyền Fake,
  chạy thật truyền httpx.Client + LocalRawStore. Mai thành UseCase không phải
  sửa vòng lặp.
- Mọi parse dồn vào mappers.parse_work(), mọi file dồn vào store/state_manager.
  Hàm này chỉ điều phối (SRP: 1 actor = điều phối fetch).
"""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path
from typing import Any

import httpx

from .config import HarvestConfig
from .mappers import extract_work_id, parse_work
from .state_manager import load_cursor, load_state, save_state

logger = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def fetch_page(
    client: httpx.Client,
    base_url: str,
    filter_str: str,
    cursor: str,
    per_page: int,
    mailto: str,
    api_key: str = None,
    max_retries: int = 5,
) -> dict[str, Any]:
    """GET /works 1 page, retry exponential backoff với 429/5xx. Raise sau max_retries."""
    params = {
        "filter": filter_str,
        "per_page": per_page,
        "cursor": cursor,
        "mailto": mailto,
        "api_key": api_key
    }

    if api_key:
        params["api_key"] = api_key
        
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            resp = client.get(f"{base_url}/works", params=params, timeout=30)
            if resp.status_code in RETRYABLE_STATUS:
                wait = min(2**attempt, 30)
                logger.warning(
                    "HTTP %s, retry %d/%d sau %ss", resp.status_code, attempt + 1, max_retries, wait
                )
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except (httpx.TimeoutException, httpx.TransportError) as e:
            last_exc = e
            wait = min(2**attempt, 30)
            logger.warning("Network lỗi %s, retry %d/%d sau %ss", e, attempt + 1, max_retries, wait)
            time.sleep(wait)
    raise RuntimeError(f"fetch_page thất bại sau {max_retries} retries") from last_exc


def harvest_works(
    config: HarvestConfig,
    client: httpx.Client | None = None,
    store=None,  # LocalRawStore, để Any để tránh import vòng; có .save_raw/.exists
) -> int:
    """Chạy harvest tới hết cursor hoặc max_pages. Trả về tổng số đã fetch phiên này."""
    from storage.storage import LocalRawStore  # import muộn để chạy standalone

    config.ensure_dirs()
    store = store or LocalRawStore(config.output_dir)
    store.ensure_dirs()

    own_client = client is None
    client = client or httpx.Client(headers={"User-Agent": f"scholarly-pipeline ({config.mailto})"})

    # Lưu lại config để reproduce (audit + resume đúng filter)
    store.save_harvest_config(
        {
            "filter": config.filter_str,
            "per_page": config.per_page,
            "mailto": config.mailto,
            "base_url": config.base_url,
        }
    )

    try:
        prev = load_state(config.state_path)
        total_fetched = int(prev.get("total_fetched", 0)) if prev else 0
        # Nếu đổi filter mà vẫn dùng state cũ -> cảnh báo, reset cursor để tránh trộn data
        if prev and prev.get("filter") != config.filter_str:
            logger.warning(
                "Filter đổi (%s -> %s), reset cursor từ đầu. Xóa %s nếu muốn resume filter cũ.",
                prev.get("filter"),
                config.filter_str,
                config.state_path,
            )
            cursor = "*"
            total_fetched = 0
        else:
            cursor = load_cursor(config.state_path)

        fetched_this_run = 0
        pages = 0
        while cursor:
            if config.max_pages is not None and pages >= config.max_pages:
                logger.info("Dừng theo max_pages=%s (mode test)", config.max_pages)
                break
            data = fetch_page(
                client, config.base_url, config.filter_str, cursor, config.per_page, config.mailto,
                config.api_key, config.max_retries
            )
            results = data.get("results", [])
            for raw in results:
                parsed = parse_work(raw)
                work_id = parsed.get("work_id") or extract_work_id(raw.get("id"))
                if not work_id:
                    logger.warning("Bỏ qua record không có id: %s", str(raw)[:200])
                    continue
                if store.exists(work_id):
                    continue  # idempotent: đã có thì skip
                store.save_raw(work_id, raw)
                total_fetched += 1
                fetched_this_run += 1

            cursor = (data.get("meta") or {}).get("next_cursor")
            pages += 1
            save_state(config.state_path, cursor, total_fetched, config.filter_str)
            logger.info("Page %d: +%d works (total %d)", pages, len(results), total_fetched)
            if cursor:
                time.sleep(config.sleep_sec)
        return fetched_this_run
    finally:
        if own_client:
            client.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Harvest OpenAlex works -> raw/")
    ap.add_argument("--filter", default=None, help="Ghi đè OPENALEX_FILTER")
    ap.add_argument("--max-pages", type=int, default=None, help="Test với 2-3 page trước")
    ap.add_argument("--raw-dir", default=None, help="Ghi đè RAW_DIR")
    args = ap.parse_args()

    config = HarvestConfig.from_env()
    if args.filter:
        config.filter_str = args.filter
    if args.max_pages is not None:
        config.max_pages = args.max_pages
    if args.raw_dir:
        config.output_dir = Path(args.raw_dir)

    n = harvest_works(config)
    print(f"Done. Fetched this run: {n}. Raw dir: {config.output_dir}")


if __name__ == "__main__":
    main()
