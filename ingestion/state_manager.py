"""Checkpoint/resume — khái niệm quan trọng nhất giai đoạn 1.

state.json mẫu (theo 01_ingestion.md):
    {
      "next_cursor": "IlsxNzA3...",
      "total_fetched": 3400,
      "last_updated": "2025-01-15T10:30:00Z",
      "filter": "institutions.country_code:VN,is_oa:true"
    }

- Ghi atomic (tmp + rename) để kill giữa chừng không corrupt file.
- Đây là seam cho Clean sau này: sẽ thành FileCheckpointStore implement
  CheckpointStore port mà không phải sửa harvester.
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def load_state(state_path: str | Path) -> dict | None:
    p = Path(state_path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def load_cursor(state_path: str | Path) -> str:
    """Trả về cursor để resume, '*' nghĩa là bắt đầu từ đầu (quy ước OpenAlex)."""
    state = load_state(state_path)
    if not state:
        return "*"
    return state.get("next_cursor") or "*"


def save_state(
    state_path: str | Path,
    next_cursor: str | None,
    total_fetched: int,
    filter_str: str,
) -> None:
    p = Path(state_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "next_cursor": next_cursor,
        "total_fetched": total_fetched,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "filter": filter_str,
    }
    # Atomic write: kill -9 giữa chừng cũng không để lại file nửa vời.
    fd, tmp_name = tempfile.mkstemp(dir=str(p.parent), prefix=".state-", suffix=".tmp")
    try:
        with open(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        Path(tmp_name).replace(p)
    finally:
        if Path(tmp_name).exists():
            Path(tmp_name).unlink(missing_ok=True)
