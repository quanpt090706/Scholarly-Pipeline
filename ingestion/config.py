"""Config thuần cho giai đoạn 1.

Nguyên tắc Clean (xem docs/reference/clean-architecture-summary.md Ch.26):
- File này là Main Component phụ: chỗ duy nhất đọc env.
- Business logic (harvester/mapper/store) KHÔNG đọc os.environ trực tiếp,
  chỉ nhận HarvestConfig truyền vào -> sau này thay bằng DI không phải sửa logic.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

@dataclass
class HarvestConfig:
    base_url: str = "https://api.openalex.org"
    api_key: str = None
    mailto: str = "your@email.com"  # polite pool, xem 01_ingestion.md
    filter_str: str = "institutions.country_code:VN,is_oa:true,publication_year:>2017"
    per_page: int = 200
    output_dir: Path = field(default_factory=lambda: Path("raw"))
    sleep_sec: float = 0.15  # ~6-7 req/s, an toàn dưới limit 10 req/s
    max_retries: int = 5
    max_pages: int | None = None  # test với 2-3 page trước, None = chạy hết

    @property
    def works_dir(self) -> Path:
        return self.output_dir / "works"

    @property
    def pdfs_dir(self) -> Path:
        return self.output_dir / "pdfs"

    @property
    def state_dir(self) -> Path:
        return self.output_dir / "_state"

    @property
    def meta_dir(self) -> Path:
        return self.output_dir / "_meta"

    @property
    def state_path(self) -> Path:
        return self.state_dir / "harvest_state.json"

    def ensure_dirs(self) -> None:
        for d in (self.works_dir, self.pdfs_dir, self.state_dir, self.meta_dir):
            d.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_env(cls) -> "HarvestConfig":
        output = os.getenv("RAW_DIR", "raw")
        max_pages_raw = os.getenv("MAX_PAGES", "")
        return cls(
            base_url=os.getenv("OPENALEX_BASE_URL", "https://api.openalex.org"),
            api_key=os.getenv("OPENALEX_API_KEY", None),
            mailto=os.getenv("OPENALEX_MAILTO", "your@email.com"),
            filter_str=os.getenv(
                "OPENALEX_FILTER",
                "institutions.country_code:VN,is_oa:true,publication_year:>2017",
            ),
            per_page=int(os.getenv("PER_PAGE", "200")),
            output_dir=Path(output),
            sleep_sec=float(os.getenv("SLEEP_SEC", "0.15")),
            max_retries=int(os.getenv("MAX_RETRIES", "5")),
            max_pages=int(max_pages_raw) if max_pages_raw.strip().isdigit() else None,
        )
