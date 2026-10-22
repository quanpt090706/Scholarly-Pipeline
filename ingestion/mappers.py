"""Mapper: OpenAlex JSON thô -> dict chuẩn hóa.

Đây là seam quan trọng nhất để lên Clean (Ch.20 Entities, Ch.23 Data Mapper):
- Hôm nay trả về dict, mai đổi thành dataclass Work mà harvester không phải sửa
  nhiều (chỉ đổi kiểu trả về của parse_work).
- Mọi chịu đựng schema thiếu field (không DOI, không abstract, không pdf_url)
  dồn hết vào đây, không để if/None lan ra harvester/storage.

Abstract OpenAlex lưu dạng inverted index: {"The": [0], "study": [1, 45]}.
"""

from __future__ import annotations

from typing import Any


def reconstruct_abstract(inverted_index: dict | None) -> str:
    if not inverted_index:
        return ""
    try:
        size = max(pos for positions in inverted_index.values() for pos in positions) + 1
    except ValueError:
        return ""
    words = [""] * size
    for word, positions in inverted_index.items():
        for pos in positions:
            if 0 <= pos < size:
                words[pos] = word
    return " ".join(words)


def extract_work_id(openalex_id: str | None) -> str | None:
    """'https://openalex.org/W2741809807' -> 'W2741809807'. Dùng làm tên file (idempotent)."""
    if not openalex_id:
        return None
    return openalex_id.rstrip("/").split("/")[-1] or None


def _get_pdf_url(raw: dict) -> str | None:
    primary = raw.get("primary_location") or {}
    # Ưu tiên pdf_url ở primary_location, fallback oa_url
    pdf_url = primary.get("pdf_url")
    if pdf_url:
        return pdf_url
    open_access = raw.get("open_access") or {}
    return open_access.get("oa_url")


def parse_work(raw: dict) -> dict[str, Any]:
    """Parse 1 work raw thành dict chuẩn. Không bao giờ raise vì thiếu field."""
    work_id = extract_work_id(raw.get("id"))
    authorships = raw.get("authorships") or []
    topics = raw.get("topics") or []
    return {
        "work_id": work_id,
        "doi": raw.get("doi"),
        "title": raw.get("title"),
        "publication_year": raw.get("publication_year"),
        "publication_date": raw.get("publication_date"),
        "pdf_url": _get_pdf_url(raw),
        "is_oa": bool((raw.get("open_access") or {}).get("is_oa", False)),
        "abstract": reconstruct_abstract(raw.get("abstract_inverted_index")),
        "cited_by_count": raw.get("cited_by_count", 0) or 0,
        "authorships_count": len(authorships),
        "topics": [t.get("display_name") for t in topics if t.get("display_name")],
        # Giữ lại raw để debug, nhưng storage chỉ cần lưu raw gốc.
        "_raw": raw,
    }
