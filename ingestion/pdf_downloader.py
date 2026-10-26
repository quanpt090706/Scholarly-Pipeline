"""PDF Downloader — chỉ với bài có pdf_url, stream + validate magic bytes.

Theo 01_ingestion.md:
- Không phải pdf_url nào cũng tải được (redirect/login/broken).
- Stream theo chunk 8KB, không load cả file 20MB vào RAM.
- Validate 4 bytes đầu == b'%PDF', sai thì xóa + return False.
- Idempotent: file đã có (>1000 bytes) thì skip.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)


def is_valid_pdf(path: str | Path) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(4) == b"%PDF"
    except OSError:
        return False


def download_pdf(
    pdf_url: str | None,
    dest_path: str | Path,
    client: httpx.Client | None = None,
) -> bool:
    dest = Path(dest_path)
    if not pdf_url:
        return False
    if dest.exists() and dest.stat().st_size > 1000:
        return True  # đã có, skip — idempotent

    own_client = client is None
    client = client or httpx.Client(follow_redirects=True, timeout=60)
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            with client.stream("GET", pdf_url, timeout=60) as r:
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r.iter_bytes(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
        except Exception as e:  # network/HTTP lỗi -> False, không raise để harvest tiếp
            logger.warning("Download fail %s: %s", pdf_url, e)
            if dest.exists():
                try:
                    dest.unlink()
                except OSError:
                    pass
            return False

        if not is_valid_pdf(dest):
            logger.warning("File không phải PDF (magic bytes sai): %s", pdf_url)
            try:
                os.remove(dest)
            except OSError:
                pass
            return False
        return True
    finally:
        if own_client:
            client.close()