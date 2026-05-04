# Topic D - Multimodal RAG tren 10 trieu document phap ly

## 1) Problem statement

Mot van phong luat tai Viet Nam muon xay he thong Multimodal RAG tren 10 trieu ho so phap ly (ban an, hop dong, quy dinh, cong van). Kho du lieu gom PDF text-native, PDF scan, hinh anh dinh kem, va bang bieu. Uoc tinh khoi luong dau vao ban dau ~250 TB raw, tang them 1-2 TB/ngay.

Rang buoc kinh doanh:
- p95 retrieval latency < 200 ms cho truy van tu ung dung nghiep vu.
- Can tai tao ket qua sau 5 nam: khi mot ho so trich dan version cu the cua tai lieu, he thong phai tra lai dung bo chunks va embeddings da duoc phe duyet o thoi diem do.
- Embedding model du kien nang cap it nhat 2 lan trong vong doi du an; moi lan upgrade can re-embed co kiem soat, khong lam "silent drift" ket qua.
- Tai lieu co nhieu thong tin nhay cam (PII, so CCCD, dia chi, thong tin tranh chap), can kiem soat truy cap theo tenant va audit truy cap.

Do kho nam o ba diem: (1) kha nang tai lap version dai han, (2) can bang freshness va do on dinh ket qua, (3) toi uu chi phi khi luu text + image + vector index o quy mo lon.

## 2) Architecture diagram (Bronze -> Silver -> Gold)

```text
[Nguon du lieu]
  - Batch upload PDF/hinh
  - Delta CDC metadata tu DMS/ECM
         |
         v
+------------------------------ BRONZE ------------------------------+
| Delta table: bronze_documents                                       |
| - document_id, tenant_id, source_uri, ingest_ts, doc_type          |
| - raw_binary_ptr (object storage), raw_text_extract (neu co)        |
| - checksum_sha256, parse_status, pii_detect_score                   |
| Partition: ingest_date ; CDF ON ; retention 30 ngay                 |
+---------------------------------------------------------------------+
         |
         | OCR + parser + table extractor + image captioning
         v
+------------------------------ SILVER ------------------------------+
| Delta table: silver_chunks                                          |
| - chunk_id, document_id, page_no, modality(text/image/table)        |
| - normalized_text, bbox_meta, language, legal_entity_tags           |
| - pii_masked_text, token_count, quality_score                       |
| Delta table: silver_embeddings                                      |
| - chunk_id, embedding_model_version, embedding_vector_ptr           |
| - embed_ts, feature_hash, lineage_run_id                            |
| Partition: tenant_id, model_version ; ZORDER(document_id, chunk_id) |
| Time travel 5 nam ; schema evolution co gate                        |
+---------------------------------------------------------------------+
         |
         | Index builder (HNSW/IVF) + hybrid BM25 + ACL projector
         v
+------------------------------- GOLD -------------------------------+
| gold_retrieval_index_manifest                                       |
| - index_id, tenant_id, model_version, snapshot_version, status      |
| - active_from, active_to, rollback_pointer                          |
| gold_query_serving_features                                         |
| - query_template_stats, rerank_features, cache_signals              |
| Serving path: API Gateway -> Retriever -> Reranker -> LLM           |
| SLA: p95 < 200 ms, cache+ANN+filter tenant                          |
+---------------------------------------------------------------------+
         |
         v
[Observability & Governance]
- OpenLineage/Marquez (lineage theo cot va job)
- Audit log moi lan doc chunk goc va PII
- Cost dashboard theo tenant/model_version
```

## 3) Quyết định chính và alternatives đã loại

### QD1. Table format cho lakehouse metadata

Toi chon **Delta Lake** cho Bronze/Silver/Gold metadata va chunk store.
- Toi loai **Iceberg** vi he thong hien tai da chay pipeline Delta o Track 2, va can `MERGE` + `time travel` de rollback nhanh theo version retrieval. Iceberg lam duoc, nhung doi team hien tai chi phi migration van hanh cao hon.
- Toi loai **Hudi** vi bai toan nay uu tien reproducibility retrieval version dai han hon ingestion upsert sieu nhanh; he sinh thai query da team dang dung (Spark/DuckDB) phu hop Delta hon.

Tradeoff: Delta khong phai format toi uu de luu vector thuan tuy, nen toi tach vector binary/index artifact ra object storage va giu metadata version trong Delta.

### QD2. Kho vector cho truy hoi ANN

Toi chon **hybrid**: metadata trong Delta + vector index artifact (HNSW) trong object storage + online ANN service.
- Toi loai **chi Delta + scan brute force** vi khong dap ung p95 < 200 ms o quy mo 30 ty token chunks.
- Toi loai **chi dung vector DB dong hoan toan** vi kho giu reproducibility 5 nam neu index rebuild va schema metadata thay doi ma khong co transaction log ro rang nhu Delta.

Tradeoff: Hybrid tang do phuc tap van hanh, doi lai dat duoc latency va auditability.

### QD3. Embedding versioning strategy

Toi chon **version song song theo model_version** (khong overwrite embedding cu).
- Toi loai **overwrite in-place** vi mat kha nang tai tao ket qua phap ly trong tuong lai.
- Toi loai **full reindex cutover 1 lan duy nhat** vi rui ro lon; neu model moi gay regression se kho rollback nhanh.

Tradeoff: Tang storage 1.8-2.2x trong giai doan chong lap, nhung cho phep A/B va rollback bang con tro `active_index_id`.

### QD4. Chunking va partitioning

Toi chon chunk theo **cau truc phap ly + page window** (dieu/khoan/muc + overlap nho), partition theo `tenant_id` va `ingest_month`, ZORDER theo `document_id, chunk_id`.
- Toi loai **chunk co dinh theo so token duy nhat** vi lam mat ngu canh dieu khoan va citation boundary.
- Toi loai **partition theo ngay ingest duy nhat** vi query retrieval thuong loc theo tenant + legal domain, khong loc theo ngay.

Tradeoff: Chunking theo cau truc phuc tap hon parser, nhung cai thien precision retrieval va giam hallucination khi trich dan.

### QD5. Governance va bao mat

Toi chon **PII masking tai Silver truoc khi embedding**, giu ban goc ma hoa chi trong Bronze voi ACL chat + audit.
- Toi loai **masking sau embedding** vi vector da co the ro ri thong tin nhay cam.
- Toi loai **khong luu raw sau parse** vi quy trinh phap ly can doi chieu tai lieu goc khi tranh chap.

Tradeoff: Chi phi governance tang (tokenization, KMS, audit), nhung dap ung yeu cau tuan thu va giam rui ro ro ri.

### QD6. Catalog va lineage

Toi chon **open catalog + OpenLineage/Marquez** de giu vendor-neutral metadata.
- Toi loai **catalog proprietary don le** vi rui ro lock-in khi can doi engine query va index service.
- Toi loai **lineage tu build ad-hoc** vi kho bao tri va kho truy vet tac dong khi schema thay doi.

Tradeoff: Them cong doan tich hop, doi lai co kha nang truy vet "chunk nao -> embedding nao -> index nao -> query nao".

## 4) Failure modes (3 gio sang)

### FM1. Parser update lam vo schema chunk (schema drift)
- Dau hieu: ti le `parse_status=failed` tang dot bien > 15%, so cot null cua `normalized_text` tang cao.
- Detect: canh bao tu quality job + schema contract check truoc khi ghi Silver.
- Rollback: dung ingestion moi, `RESTORE` Silver ve version truoc, chay lai parser o sandbox; day la Day 18 concept **schema evolution + time travel**.

### FM2. Embedding model moi lam giam recall nghiem trong
- Dau hieu: online nDCG@10 giam > 8%, query fallback BM25 tang manh.
- Detect: shadow traffic A/B giua `model_version=v_current` va `v_next`.
- Rollback: doi `gold_retrieval_index_manifest.active_index_id` ve version cu trong < 5 phut, khong can reprocess Bronze/Silver.

### FM3. Loi ACL projection lam lo du lieu cross-tenant
- Dau hieu: audit log phat hien query tenant A tra chunk tenant B.
- Detect: policy-as-code test truoc deploy + real-time detector tren access log.
- Rollback: khoa endpoint retrieval tenant bi anh huong, revoke token, replay query logs de danh gia impact, rotate key neu can.

### FM4. Index artifact bi hong sau compact/object lifecycle
- Dau hieu: ANN service tra loi loi I/O, p95 latency > 2s.
- Detect: health check index checksum theo `index_manifest`.
- Rollback: mount lai artifact snapshot truoc (pointer rollback), rebuild index async tu `silver_embeddings` version da pin.

## 5) Uoc luong chi phi back-of-envelope (USD/thang)

Gia dinh:
- 10 trieu docs, trung binh 25 MB/doc raw binary => 250 TB Bronze raw.
- Sau parse/chunk con 12% text + metadata huu ich => ~30 TB Silver chunks.
- Embeddings: 30 ty tokens chunked, vector float16 + metadata ~18 TB/moi version.
- Duy tri 2 model versions song song => 36 TB embeddings.
- Index artifacts + cache snapshots ~20 TB.

### Storage
- Object storage Standard cho hot (90 TB) @ $23/TB-thang = **$2,070**
- Object storage IA cho warm (180 TB) @ $12.5/TB-thang = **$2,250**
- Archive cho cold/legal hold (66 TB) @ $4/TB-thang = **$264**
- Metadata requests + lifecycle overhead (uoc tinh 15%) = **$687**

Tong storage ~ **$5,271/thang**

### Compute
- OCR + parsing batch + embedding incremental: ~14,000 vCPU-gio/thang @ $0.04 = **$560**
- GPU embedding reindex dinh ky (chia binh quan thang): 600 GPU-gio @ $1.8 = **$1,080**
- ANN serving + reranker online: 6 nodes x $180 = **$1,080**
- Monitoring, lineage, orchestration overhead = **$500**

Tong compute ~ **$3,220/thang**

### Tong cong
- **~$8,491/thang** (chua gom egress lien vung).
- Muc tieu toi uu giai doan 2: giam 12-18% bang compaction cadence va cache query theo tenant.

## 6) MVP 1 tuan (slice nho nhat shippable)

Muc tieu MVP: chung minh "versioned retrieval reproducible" cho 1 tenant, 100k documents.

Phạm vi:
1. Bronze ingest + checksum + metadata CDF.
2. Silver parser text-only (bo qua hinh/bang giai doan dau), chunking theo heading + overlap.
3. Embedding cho 2 model versions (`v1`, `v2`) va luu `index_manifest`.
4. Retriever API co filter tenant + trich dan `document_id/page_no/chunk_id`.
5. Nut rollback nhanh: chuyen active index `v2 -> v1` trong <= 5 phut.
6. Dashboard mini: p95 latency, recall@k proxy, chi phi compute/ngay.

Definition of done MVP:
- p95 retrieval < 250 ms tren tap query noi bo.
- 100% query tra citation co the truy vet den Delta version cu the.
- Rollback pointer thanh cong, khong can rebuild lai du lieu.

