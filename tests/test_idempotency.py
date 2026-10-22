"""Test idempotency + checkpoint — chạy 2 lần phải cùng kết quả.

Theo 01_ingestion.md mục 'Cố ý tạo tình huống':
1. Chạy harvester 2 lần -> tổng số file không đổi.
2. Xóa 5 file -> chạy lại -> chỉ 5 file đó được tạo lại.
3. Kill giữa chừng -> resume từ cursor đã lưu.

Ở đây test ở mức unit (không gọi mạng) để chạy nhanh trong CI.
Test e2e có mạng nằm ở thao tác tay: MAX_PAGES=2 chạy 2 lần.
"""

from __future__ import annotations

import json

from ingestion.mappers import extract_work_id, parse_work, reconstruct_abstract
from ingestion.state_manager import load_cursor, save_state
from storage.storage import LocalRawStore


def _fake_work(i: int) -> dict:
    return {
        "id": f"https://openalex.org/W{i}",
        "doi": f"https://doi.org/10.1234/test{i}",
        "title": f"Test paper {i}",
        "publication_year": 2023,
        "publication_date": "2023-01-01",
        "primary_location": {"pdf_url": f"https://example.com/{i}.pdf"},
        "open_access": {"is_oa": True, "oa_url": f"https://example.com/{i}.pdf"},
        "abstract_inverted_index": {"Hello": [0], "world": [1]},
        "cited_by_count": i,
        "authorships": [{"author": {"display_name": "A"}}],
        "topics": [{"display_name": "ML"}],
    }


def test_reconstruct_abstract():
    assert reconstruct_abstract({"The": [0], "study": [1, 2]}) == "The study study"
    assert reconstruct_abstract(None) == ""
    assert reconstruct_abstract({}) == ""


def test_parse_work_chiu_schema_thieu_field():
    # Bài không DOI, không abstract, không pdf_url vẫn parse được, không raise
    raw = {"id": "https://openalex.org/W999", "title": "No meta"}
    parsed = parse_work(raw)
    assert parsed["work_id"] == "W999"
    assert parsed["pdf_url"] is None
    assert parsed["abstract"] == ""
    assert parsed["cited_by_count"] == 0


def test_extract_work_id():
    assert extract_work_id("https://openalex.org/W2741809807") == "W2741809807"
    assert extract_work_id(None) is None


def test_save_raw_idempotent(tmp_path):
    """Chạy 2 lần -> cùng kết quả, không trùng."""
    store = LocalRawStore(tmp_path)
    for _ in range(2):
        for i in range(5):
            raw = _fake_work(i)
            store.save_raw(f"W{i}", raw)
    assert store.count_works() == 5
    # Chạy lại lần 2 không đổi số file
    assert store.count_works() == 5


def test_xoa_file_chay_lai_chi_tao_lai_file_da_xoa(tmp_path):
    store = LocalRawStore(tmp_path)
    for i in range(5):
        store.save_raw(f"W{i}", _fake_work(i))
    assert store.count_works() == 5
    # Xóa 2 file
    (tmp_path / "works" / "W1.json").unlink()
    (tmp_path / "works" / "W3.json").unlink()
    assert store.count_works() == 3
    # Chạy lại logic harvest: exists() thì skip, chưa có thì save
    for i in range(5):
        if not store.exists(f"W{i}"):
            store.save_raw(f"W{i}", _fake_work(i))
    assert store.count_works() == 5


def test_checkpoint_resume(tmp_path):
    state_path = tmp_path / "_state" / "harvest_state.json"
    # Chưa có state -> bắt đầu từ đầu
    assert load_cursor(state_path) == "*"
    save_state(state_path, "cursor_abc", 500, "filter=X")
    assert load_cursor(state_path) == "cursor_abc"
    # Ghi đè tiến triển, resume tiếp
    save_state(state_path, "cursor_def", 700, "filter=X")
    data = json.loads(state_path.read_text(encoding="utf-8"))
    assert data["total_fetched"] == 700
    assert data["next_cursor"] == "cursor_def"
