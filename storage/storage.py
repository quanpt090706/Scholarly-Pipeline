"""Raw storage local — abstraction tối thiểu cho S3/MinIO sau này.

Idempotency (khái niệm quan trọng nhất):
- Tên file = work_id (vd W2741809807.json) -> tự nhiên unique.
- save_raw() ghi đè cùng nội dung = cùng kết quả, chạy N lần không sinh trùng.
- exists() để skip trước khi fetch/download lại.

Sau này lên Clean (DIP Ch.11, ISP Ch.10):
- Class này sẽ implement RawStore port.
- Thêm S3Store/MinioStore implement cùng 3 method save_raw/exists/read_raw,
  harvester không phải sửa.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class LocalRawStore:
    def __init__(self, output_dir: str | Path):
        self.output_dir = Path(output_dir)
        self.works_dir = self.output_dir / "works"
        self.pdfs_dir = self.output_dir / "pdfs"
        self.state_dir = self.output_dir / "_state"
        self.meta_dir = self.output_dir / "_meta"

    def ensure_dirs(self) -> None:
        for d in (self.works_dir, self.pdfs_dir, self.state_dir, self.meta_dir):
            d.mkdir(parents=True, exist_ok=True)

    def work_path(self, work_id: str) -> Path:
        return self.works_dir / f"{work_id}.json"

    def exists(self, work_id: str) -> bool:
        p = self.work_path(work_id)
        return p.exists() and p.stat().st_size > 0

    def save_raw(self, work_id: str, raw: dict[str, Any]) -> Path:
        """Lưu nguyên bản response API. Ghi đè idempotent."""
        self.ensure_dirs()
        p = self.work_path(work_id)
        p.write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")
        return p

    def read_raw(self, work_id: str) -> dict[str, Any]:
        p = self.work_path(work_id)
        return json.loads(p.read_text(encoding="utf-8"))

    def count_works(self) -> int:
        if not self.works_dir.exists():
            return 0
        return sum(1 for _ in self.works_dir.glob("W*.json"))

    def save_harvest_config(self, config_dict: dict[str, Any]) -> Path:
        self.ensure_dirs()
        p = self.meta_dir / "harvest_config.json"
        p.write_text(json.dumps(config_dict, ensure_ascii=False, indent=2), encoding="utf-8")
        return p
