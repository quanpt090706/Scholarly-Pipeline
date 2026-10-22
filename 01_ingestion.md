# Giai đoạn 1: Ingestion — Thu thập dữ liệu từ OpenAlex

## Tổng quan

**Mục tiêu:** Thu thập metadata và link PDF của 10,000+ bài báo khoa học từ OpenAlex API, lưu vào raw storage một cách đáng tin cậy, có thể resume và chạy lại mà không sinh trùng lặp.

**Tại sao OpenAlex?**
- REST API JSON chuẩn, không cần scraping HTML
- 250M+ bài báo, miễn phí, không cần API key (có key thì rate limit cao hơn)
- Filter phong phú: theo năm, lĩnh vực, quốc gia, institution, open access...
- Có thể filter `institutions.country_code:VN` để giữ góc độ "nghiên cứu từ Việt Nam" — câu chuyện liên tục với context bài toán gốc
- Cursor-based pagination — không có giới hạn 10,000 record như một số API khác

---

## Về API Key

OpenAlex có hai chế độ:

| Chế độ | Cách dùng | Rate limit |
|---|---|---|
| **Không có key** | Gọi bình thường | ~10 req/giây, 100K req/ngày |
| **Có key (miễn phí)** | Thêm `?api_key=YOUR_KEY` | Cao hơn, ổn định hơn |
| **Polite pool** | Thêm `?mailto=your@email.com` | Ưu tiên hơn anonymous |

**Thực tế:** Với 10K bài, polite pool là đủ. Đăng ký key tại `openalex.org/settings/api` nếu muốn chắc chắn hơn.

---

## Endpoint và cấu trúc API

**Base URL:** `https://api.openalex.org`

### Các endpoint sẽ dùng

```bash
# Lấy danh sách bài báo (có filter + pagination)
GET /works

# Lấy một bài cụ thể theo OpenAlex ID
GET /works/W2741809807

# Lấy một bài theo DOI
GET /works/https://doi.org/10.1038/nature12373

# Lấy danh sách tác giả
GET /authors

# Lấy danh sách nguồn (tạp chí, conference...)
GET /sources
```

### Ví dụ filter thực tế

```bash
# Bài báo từ tác giả ở Việt Nam, open access, từ 2018 trở đi
GET /works?filter=institutions.country_code:VN,is_oa:true,publication_year:>2017&per_page=200

# Bài báo thuộc lĩnh vực Computer Science
GET /works?filter=primary_topic.field.id:F17,publication_year:2020-2024&per_page=200

# Bài báo có full text PDF
GET /works?filter=has_fulltext:true,publication_year:2022&per_page=200
```

### Cấu trúc response

```json
{
  "meta": {
    "count": 143820,
    "page": 1,
    "per_page": 200,
    "next_cursor": "IlsxNzA3..."
  },
  "results": [
    {
      "id": "https://openalex.org/W2741809807",
      "doi": "https://doi.org/10.7717/peerj.4375",
      "title": "...",
      "publication_year": 2018,
      "publication_date": "2018-01-01",
      "primary_location": {
        "source": { "display_name": "PeerJ", "issn": ["2167-8359"] },
        "pdf_url": "https://peerj.com/articles/4375.pdf",
        "is_oa": true
      },
      "authorships": [
        {
          "author": { "id": "...", "display_name": "Jane Smith", "orcid": "..." },
          "institutions": [{ "display_name": "MIT", "country_code": "US" }]
        }
      ],
      "abstract_inverted_index": { "The": [0], "study": [1, 45], ... },
      "cited_by_count": 312,
      "topics": [{ "display_name": "Machine Learning", ... }],
      "keywords": [{ "display_name": "neural network" }],
      "open_access": { "is_oa": true, "oa_url": "https://..." }
    }
  ]
}
```

**Lưu ý:** Abstract được lưu dạng inverted index (map từ → list vị trí). Cần reconstruct khi dùng:

```python
def reconstruct_abstract(inverted_index: dict) -> str:
    if not inverted_index:
        return ""
    words = [""] * (max(pos for positions in inverted_index.values() for pos in positions) + 1)
    for word, positions in inverted_index.items():
        for pos in positions:
            words[pos] = word
    return " ".join(words)
```

---

## Thiết kế pipeline Ingestion

```
OpenAlex API
     │
     ▼
Metadata Harvester ──► Raw Storage (MinIO/S3)
  - cursor pagination        │
  - filter theo scope        ├── raw/metadata/{work_id}.json
  - checkpoint/resume        │
     │                       └── raw/pdfs/{work_id}.pdf
     ▼
PDF Downloader (tùy chọn)
  - chỉ với bài có pdf_url
  - stream download
  - validate magic bytes
```

---

## Các thành phần cần xây dựng

### 1. Works Harvester

**Nhiệm vụ:** Gọi `/works` API với filter đã chọn, xử lý cursor pagination, lưu raw JSON.

**Kỹ thuật cần học:**
- Cursor-based pagination (khác với offset pagination)
- HTTP session với retry tự động
- Rate limit bằng `time.sleep()` hoặc `asyncio`

**Checkpoint/resume:**

```python
# state.json — lưu lại sau mỗi page
state = {
    "next_cursor": "IlsxNzA3NDI4NTkwLCAi...",
    "total_fetched": 3400,
    "last_updated": "2025-01-15T10:30:00Z",
    "filter": "institutions.country_code:VN,is_oa:true"
}
```

Khi restart: đọc `state.json`, tiếp tục từ `next_cursor`. Nếu không có state → bắt đầu từ đầu.

**Xử lý pagination:**

```python
import httpx, json, time

BASE_URL = "https://api.openalex.org"
MAILTO = "your@email.com"  # polite pool

def harvest_works(filter_str: str, output_dir: str):
    cursor = load_checkpoint(output_dir) or "*"  # "*" = bắt đầu từ đầu

    while cursor:
        url = f"{BASE_URL}/works"
        params = {
            "filter": filter_str,
            "per_page": 200,
            "cursor": cursor,
            "mailto": MAILTO,
        }
        resp = httpx.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        for work in data["results"]:
            save_work(work, output_dir)

        cursor = data["meta"].get("next_cursor")
        save_checkpoint(output_dir, cursor, data["meta"]["count"])
        time.sleep(0.15)  # ~6-7 req/giây, an toàn
```

**Tình huống cần xử lý:**
- `429 Too Many Requests` → sleep rồi retry
- `500/503` → retry với backoff
- Response thiếu field (một số bài không có abstract, DOI, pdf_url) → xử lý `None` gracefully

---

### 2. PDF Downloader (tùy chọn)

OpenAlex trả về `primary_location.pdf_url` nếu bài là open access. Không cần scraping thêm.

**Lưu ý trước khi download hàng loạt:**
- Không phải tất cả `pdf_url` đều download được (một số redirect, yêu cầu login, hoặc broken link)
- PDF từ nhiều nguồn khác nhau (publisher, repository, preprint server) → behavior không đồng nhất
- Nên download PDF ở giai đoạn sau khi đã có đủ metadata, và chỉ download bài thực sự cần

```python
def download_pdf(pdf_url: str, dest_path: str) -> bool:
    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 1000:
        return True  # đã có, skip

    try:
        with httpx.stream("GET", pdf_url, timeout=60, follow_redirects=True) as r:
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in r.iter_bytes(chunk_size=8192):
                    f.write(chunk)

        # Validate magic bytes
        with open(dest_path, "rb") as f:
            if f.read(4) != b"%PDF":
                os.remove(dest_path)
                return False
        return True
    except Exception:
        return False
```

---

### 3. Raw Storage Structure

Lưu dữ liệu thô **nguyên bản**, không transform:

```
raw/
├── works/
│   ├── W2741809807.json     # 1 file JSON = 1 bài báo (response gốc từ API)
│   ├── W2741809808.json
│   └── ...
├── pdfs/                    # tùy chọn
│   ├── W2741809807.pdf
│   └── ...
├── _state/
│   ├── harvest_state.json   # checkpoint cursor + progress
│   └── failed_pdfs.jsonl    # log các PDF tải thất bại
└── _meta/
    └── harvest_config.json  # filter đã dùng, ngày chạy, tổng số
```

**Local dev:** Thư mục trên disk.  
**Cloud:** AWS S3, GCS, Cloudflare R2, hoặc MinIO (self-hosted, docker-compose).

---

## Idempotency — Khái niệm quan trọng nhất

**Idempotency:** Chạy pipeline nhiều lần phải cho cùng kết quả. Không tạo bản ghi trùng, không tải lại file đã có.

**Cách implement với OpenAlex:**
- Dùng `work_id` (ví dụ `W2741809807`) làm tên file → tự nhiên unique
- Kiểm tra file tồn tại trước khi ghi → skip nếu đã có
- Checkpoint cursor → resume thay vì restart

**Cố ý tạo tình huống để kiểm tra:**
1. Kill harvester sau 500 bài (Ctrl+C)
2. Restart → xem nó tiếp tục từ cursor đã lưu, không fetch lại từ đầu
3. Chạy lại lần 2 hoàn chỉnh → tổng số file không thay đổi
4. Xóa 5 file JSON → chạy lại → chỉ 5 file đó được tạo lại, không phải toàn bộ

---

## Cấu trúc project đề xuất

```
scholarly-pipeline/
├── ingestion/
│   ├── harvester.py        # Fetch works từ OpenAlex API
│   ├── pdf_downloader.py   # Download PDF (tùy chọn)
│   ├── state_manager.py    # Đọc/ghi checkpoint
│   └── config.py           # BASE_URL, filter, rate limit, paths
├── storage/
│   └── storage.py          # Abstraction: local disk hoặc S3
├── tests/
│   └── test_idempotency.py # Chạy 2 lần → cùng kết quả
├── docker-compose.yml      # MinIO cho local dev
├── .env.example            # OPENALEX_MAILTO, S3_BUCKET...
└── README.md
```

---

## Những gì học được sau giai đoạn này

| Khái niệm | Bạn sẽ hiểu vì sao khi |
|---|---|
| **Idempotency** | Restart harvester và thấy nó resume thay vì chạy lại từ đầu |
| **Cursor pagination vs offset** | Offset bị "drift" khi data thay đổi, cursor thì không |
| **Checkpoint/state** | Mất kết nối giữa chừng sau 3000 bài, phải recover |
| **Retry với backoff** | Nhận `429` liên tục khi chạy quá nhanh |
| **Stream vs buffer** | Tải file 20MB vào RAM một lúc vs. stream theo chunk |
| **File validation** | `pdf_url` trả về HTML lỗi thay vì PDF thật |
| **Inverted index** | Lần đầu thấy abstract không phải string thẳng |
| **Schema không đồng nhất** | Bài không có DOI, không có abstract, không có tác giả |

---

## Gợi ý thứ tự làm

1. **Explore API trước:** Gọi thủ công vài request bằng curl/browser/httpie, đọc JSON response để hiểu cấu trúc.
2. **Chọn scope nhỏ để test:** Ví dụ filter `institutions.country_code:VN,publication_year:2023` → ước lượng sẽ có bao nhiêu bài.
3. **Viết harvester đơn giản:** Không có checkpoint, chỉ fetch 2–3 page, lưu ra file JSON.
4. **Thêm checkpoint/resume:** Kill giữa chừng, kiểm tra resume.
5. **Thêm validation + logging:** Biết bao nhiêu bài có PDF, bao nhiêu thiếu abstract.
6. **Scale lên toàn scope:** Sau khi test xong mới chạy 10K+ bài.
7. **Đóng gói Docker:** Đảm bảo chạy được trên máy khác chỉ với `docker-compose up`.

---

## Ghi chú về bản quyền và đạo đức

- Metadata từ OpenAlex là **CC0** (public domain) — được phép dùng thoải mái, kể cả public lên GitHub.
- PDF thì tùy từng bài: open access hoàn toàn (CC BY, CC0) thì ổn; một số OA vẫn có hạn chế redistribution.
- Không public toàn bộ PDF lên GitHub/public storage. Chỉ public code và metadata.
- Ghi rõ nguồn dữ liệu trong README: "Metadata from OpenAlex (CC0), https://openalex.org" — đây là điểm cộng khi nhà tuyển dụng review project.
