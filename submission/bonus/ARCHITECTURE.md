
# [BONUS] Architecture Brief — Multimodal Legal RAG Lakehouse

## 1. Problem statement

Một văn phòng luật Việt Nam muốn xây dựng hệ thống RAG trên khoảng 10 triệu
PDF pháp lý, bao gồm văn bản text, bản scan và bảng biểu. Corpus có thể lên
tới hàng chục tỷ tokens. Mỗi document có metadata về nguồn, thời điểm,
jurisdiction, quyền sử dụng và version.

Hệ thống phải đáp ứng hai yêu cầu có vẻ mâu thuẫn:

1. Online retrieval có p95 < 200 ms.
2. Kết quả retrieval phải reproducible trong ít nhất 5 năm.

Embedding sẽ được regenerate ít nhất hai lần khi model embedding thay đổi.
Do đó, embedding cũ không được overwrite một cách không kiểm soát.

Tôi chọn Delta Lake làm system-of-record cho document, chunk, metadata,
provenance và embedding versions. Vector index là derived index có thể rebuild,
không phải nguồn dữ liệu canonical.

Mục tiêu kiến trúc là tách rõ:

- storage/reproducibility,
- governance/provenance,
- và low-latency vector serving.

---

## 2. Architecture

```text
                              ┌──────────────────────┐
PDF / Scan / Tables ────────► │ BRONZE               │
                              │ Raw PDF + metadata    │
                              │ source + checksum     │
                              └──────────┬───────────┘
                                         │
                                  parse / OCR / chunk
                                         │
                                         ▼
                              ┌──────────────────────┐
                              │ SILVER               │
                              │ document_version     │
                              │ chunk + page         │
                              │ provenance + ACL     │
                              │ content_hash         │
                              └──────────┬───────────┘
                                         │
                         ┌───────────────┴────────────────┐
                         │                                │
                         ▼                                ▼
              ┌──────────────────┐             ┌──────────────────┐
              │ GOLD / RAG STORE  │             │ EMBEDDINGS       │
              │ citations         │             │ model=v1         │
              │ legal metadata    │             │ model=v2         │
              │ retrieval config  │             │ float32 / int8   │
              └─────────┬────────┘             └────────┬─────────┘
                        │                               │
                        │                         rebuildable
                        │                               │
                        │                               ▼
                        │                     ┌──────────────────┐
                        │                     │ VECTOR INDEX     │
                        │                     │ HNSW / IVF       │
                        │                     │ derived only     │
                        │                     └────────┬─────────┘
                        │                              │
                        └──────────────┬───────────────┘
                                       ▼
                                RAG QUERY PATH
                                       │
                          metadata / ACL filtering
                                       │
                               vector retrieval
                                       │
                                rerank candidates
                                       │
                                       ▼
                         answer + document/chunk citation
                         + document_version + embedding_version
```
