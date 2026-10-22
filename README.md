# scholarly-pipeline

Pipeline thu thập, xử lý và phục vụ dữ liệu bài báo khoa học từ OpenAlex.

Được xây dựng theo từng giai đoạn để học Data Engineering theo project thực tế — mỗi giai đoạn gắn với một nhóm kỹ năng cụ thể.

---

## Kiến trúc tổng quan

```
OpenAlex API
     │
     ▼
┌─────────────────┐
│  1. Ingestion   │  Fetch metadata + PDF → raw storage (MinIO/S3)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  2. Processing  │  Trích xuất text PDF, chuẩn hóa, tách metadata
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  3. Modeling    │  raw → staging → mart (PostgreSQL + dbt)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 4. Data Quality │  Validation, completeness check, báo cáo
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 5. Orchestration│  Airflow/Dagster DAG, incremental, scheduling
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 6. Monitoring   │  Structured logs → Prometheus → Grafana
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  7. Serving     │  Full-text / semantic search, dashboard thống kê
└─────────────────┘
```

---

## Các giai đoạn

### Giai đoạn 1 — Ingestion
**File:** [`01_ingestion.md`](./01_ingestion.md)

Thu thập metadata và PDF từ OpenAlex API, lưu vào raw storage.

| Kỹ năng học | Công nghệ |
|---|---|
| REST API, cursor pagination | `httpx`, `requests` |
| Checkpoint / resume | JSON state file |
| Retry với exponential backoff | Custom hoặc `tenacity` |
| Stream download, file validation | `httpx`, magic bytes |
| Idempotency | File-based dedup |
| Object storage | MinIO (local), S3/GCS (cloud) |

---

### Giai đoạn 2 — Processing
**File:** `02_processing.md` *(chưa viết)*

Xử lý dữ liệu thô: trích xuất text từ PDF, reconstruct abstract từ inverted index, chuẩn hóa trường metadata, phát hiện và ghi nhận lỗi.

| Kỹ năng học | Công nghệ |
|---|---|
| PDF text extraction | `pdfplumber`, `pymupdf` |
| OCR cho PDF scan | `tesseract`, `pytesseract` |
| Text normalization | `underthesea` (nếu có tiếng Việt) |
| Schema chuẩn hóa | Pydantic |
| Xử lý lỗi và partial failure | Logging, dead-letter queue |

**Tình huống cần xử lý:**
- PDF chỉ chứa ảnh scan (không có text layer)
- Abstract bị thiếu hoặc encoding sai
- Tác giả không có ORCID, institution không có ROR

---

### Giai đoạn 3 — Data Modeling
**File:** `03_modeling.md` *(chưa viết)*

Thiết kế schema database, load dữ liệu đã xử lý vào PostgreSQL theo mô hình raw → staging → mart, dùng dbt để transform.

| Kỹ năng học | Công nghệ |
|---|---|
| Normalization (1NF, 2NF, 3NF) | PostgreSQL |
| Quan hệ nhiều-nhiều (bài báo ↔ tác giả) | Foreign key, junction table |
| Star schema / mart layer | dbt |
| Index, EXPLAIN, query optimization | PostgreSQL |
| Schema evolution (thêm cột, đổi kiểu) | dbt migrations |
| SCD Type 2 (lịch sử thay đổi) | dbt snapshots |

**Tình huống cần tạo:**
- Query 10K+ bài không có index → thêm index → so sánh tốc độ
- Một tạp chí đổi tên → xem lịch sử được lưu thế nào
- Thêm một trường mới vào giữa pipeline đang chạy

---

### Giai đoạn 4 — Data Quality
**File:** `04_data_quality.md` *(chưa viết)*

Định nghĩa và kiểm tra các quy tắc chất lượng dữ liệu: độ đầy đủ, tính duy nhất, tính hợp lệ. Sinh báo cáo chất lượng tự động.

| Kỹ năng học | Công nghệ |
|---|---|
| Các chiều chất lượng dữ liệu | Lý thuyết |
| Test trên model | dbt tests (built-in + custom) |
| Expectation-based testing | Great Expectations |
| Báo cáo chất lượng | dbt docs, GE Data Docs |

**Các check quan trọng:**
- DOI không được trùng lặp
- `publication_year` phải trong khoảng hợp lý
- Mỗi bài phải có ít nhất 1 tác giả
- `cited_by_count` không được âm

**Tình huống cần tạo:**
- Cố tình inject bản ghi trùng DOI → xem check có bắt không
- Bài có `publication_year: 1800` → xem range check có hoạt động không

---

### Giai đoạn 5 — Orchestration
**File:** `05_orchestration.md` *(chưa viết)*

Đưa toàn bộ pipeline vào DAG, chạy incremental theo lịch (chỉ fetch bài mới), xử lý retry và dependency giữa các task.

| Kỹ năng học | Công nghệ |
|---|---|
| DAG, task dependency | Airflow hoặc Dagster |
| Incremental load | `from_updated_date` filter của OpenAlex |
| Backfill | Airflow backfill command |
| Retry policy per task | Airflow / Dagster config |
| Sensor (chờ điều kiện) | Airflow Sensor |

**Tình huống cần tạo:**
- Kill một task giữa DAG → xem downstream task xử lý thế nào
- Backfill 3 tháng dữ liệu cũ
- Task dbt test fail → downstream serving task không chạy

---

### Giai đoạn 6 — Monitoring
**File:** `06_monitoring.md` *(chưa viết)*

Thêm structured logging và metrics vào pipeline. Dựng dashboard quan sát tình trạng hệ thống và chất lượng dữ liệu.

| Kỹ năng học | Công nghệ |
|---|---|
| Structured logging | `structlog` hoặc `loguru` |
| Metrics (counter, gauge, histogram) | Prometheus |
| Alerting | Alertmanager hoặc Grafana alert |
| Dashboard | Grafana |

**Metrics quan trọng:**
- Số bài fetch được mỗi lần chạy
- Tỉ lệ lỗi download PDF
- Thời gian chạy mỗi giai đoạn
- Số bản ghi fail data quality check

**Tình huống cần tạo:**
- Cấu hình alert khi tỉ lệ lỗi > 5%
- Xem Grafana dashboard trong khi pipeline đang chạy

---

### Giai đoạn 7 — Serving
**File:** `07_serving.md` *(chưa viết)*

Xây dựng lớp phục vụ dữ liệu: full-text search, semantic search (tùy chọn), dashboard thống kê.

| Kỹ năng học | Công nghệ |
|---|---|
| Full-text search | PostgreSQL FTS hoặc Elasticsearch |
| Semantic search (vector) | pgvector, Qdrant, Milvus |
| Embedding | SentenceTransformers hoặc OpenAlex topic |
| Dashboard | Metabase, Superset hoặc Streamlit |

**Câu hỏi bài toán mẫu cho dashboard:**
- Số bài báo theo năm, lĩnh vực, quốc gia
- Top institution theo số bài và cited_by_count
- Tỉ lệ open access theo lĩnh vực
- Trend citation theo topic

---

### Giai đoạn 8 — Packaging & Cloud Deploy
**File:** `08_deploy.md` *(chưa viết)*

Đóng gói toàn hệ thống, deploy lên cloud, CI/CD.

| Kỹ năng học | Công nghệ |
|---|---|
| Containerization | Docker, Docker Compose |
| CI pipeline | GitHub Actions |
| Cloud storage | AWS S3 hoặc GCS |
| Cloud database | AWS RDS hoặc Cloud SQL |
| IaC cơ bản | Terraform (tùy chọn) |

**Mục tiêu:** Dựng lại toàn hệ thống từ đầu chỉ bằng `docker-compose up` hoặc một lệnh deploy duy nhất.

---

## Thứ tự học đề xuất

```
1 → 2 → 3  (end-to-end bản đơn giản, ưu tiên chạy được trước)
         ↓
         4  (thêm data quality)
         ↓
         5  (orchestration)
         ↓
         6  (monitoring)
         ↓
    7 hoặc 8  (tùy mục tiêu: showcase hay production-ready)
```

Nguyên tắc: hoàn thiện từng giai đoạn ở mức đủ dùng, sau đó mới sang giai đoạn tiếp theo — không cố làm hoàn hảo từng phần trước khi có end-to-end chạy được.

---

## Cấu trúc thư mục project

```
scholarly-pipeline/
├── ingestion/
├── processing/
├── modeling/
│   └── dbt/
├── quality/
├── orchestration/
│   └── dags/
├── monitoring/
│   ├── prometheus/
│   └── grafana/
├── serving/
├── storage/
├── tests/
├── docs/                    # các file .md này
│   ├── README.md
│   ├── 01_ingestion.md
│   └── ...
├── docker-compose.yml
├── .env.example
└── README.md
```

---

## Stack tóm tắt

| Tầng | Công nghệ |
|---|---|
| Nguồn dữ liệu | OpenAlex API |
| Raw storage | MinIO (local) / S3 (cloud) |
| Database / Warehouse | PostgreSQL |
| Transform | dbt |
| Data quality | dbt tests + Great Expectations |
| Orchestration | Airflow hoặc Dagster |
| Monitoring | Prometheus + Grafana |
| Serving | PostgreSQL FTS + Metabase/Streamlit |
| Packaging | Docker Compose |
| CI | GitHub Actions |

---

*Ghi chú: Metadata từ OpenAlex được cấp phép CC0 (public domain). Xem thêm: https://openalex.org*
