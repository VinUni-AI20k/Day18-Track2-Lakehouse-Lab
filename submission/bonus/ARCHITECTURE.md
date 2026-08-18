# Architecture Brief: Lakehouse cho LLM Observability ở quy mô 1 tỷ request/ngày

## 1. Problem Statement

Hệ thống phục vụ 1 tỷ LLM request mỗi ngày, trung bình 5 KB cho mỗi request/response, tương đương khoảng 5 TB dữ liệu thô mỗi ngày. Dashboard cost, latency và error rate theo tenant phải cập nhật trong 5 phút. Prompt và response đầy đủ được giữ 7 ngày để điều tra sự cố; sau đó chỉ giữ dữ liệu tổng hợp trong một năm. PII phải được phát hiện và token hóa trước khi bất kỳ analyst nào có thể đọc dữ liệu.

Thách thức chính là duy trì ingestion liên tục, truy vấn theo tenant có độ trễ thấp và khả năng điều tra lại đúng phiên bản dữ liệu, trong khi chi phí storage không vượt quá 5.000 USD/tháng. Micro-batch có thể tạo hàng triệu file nhỏ, làm tăng chi phí metadata và thời gian lập kế hoạch truy vấn. Schema log cũng thay đổi theo phiên bản model và SDK; yêu cầu xóa dữ liệu của một tenant phải được truyền tới mọi bảng dẫn xuất. Kiến trúc vì vậy cần ACID, schema evolution có kiểm soát, time travel, lineage, phân quyền, compaction và lifecycle tự động.

## 2. Mục tiêu và phạm vi

Thiết kế ưu tiên các mục tiêu sau:

- Gold dashboard có độ mới dữ liệu không quá 5 phút.
- Dữ liệu chi tiết của một tenant trong 7 ngày có thể được truy xuất để điều tra sự cố.
- Analyst không được đọc prompt/response chứa PII dạng rõ.
- Có thể cô lập hoặc rollback một lần ghi lỗi mà không dừng toàn bộ hệ thống.
- Truy vấn phổ biến theo `tenant_id`, thời gian và model không phải quét toàn bộ dữ liệu.
- Storage, transaction log, snapshot và orphan file đều có chính sách vòng đời đo được.
- Chi phí storage nằm trong giới hạn 5.000 USD/tháng; compute được theo dõi bằng ngân sách riêng.

Ngoài phạm vi của tài liệu là thiết kế chi tiết model serving, huấn luyện model, giao diện dashboard và lựa chọn nhà cung cấp cloud cuối cùng.

## 3. Kiến trúc đề xuất

```text
                               CONTROL PLANE
                ┌──────────────────────────────────────┐
                │ REST Catalog · RBAC · KMS · Lineage │
                │ Data contracts · Audit · Monitoring │
                └───────────────┬──────────────────────┘
                                │ quản trị tất cả bảng
                                ▼
┌───────────────┐     ┌──────────────────────┐     ┌────────────────────────┐
│ LLM Gateway   │────▶│ Kafka llm-events     │────▶│ PII Ingestion Gateway  │
│ request log   │     │ key = tenant_id      │     │ detect/tokenize/encrypt│
└───────────────┘     └──────────────────────┘     └────────────┬───────────┘
                                                               │ ≤ 1 phút
                                                               ▼
                  ┌─────────────────────────────────────────────────────────┐
                  │ BRONZE — Delta                                         │
                  │ envelope đã token hóa + encrypted payload pointer      │
                  │ partition: event_date/hour · retention: 7 ngày         │
                  └──────────────────────────┬──────────────────────────────┘
                                             │ validate · dedup · MERGE
                                             ▼
                  ┌─────────────────────────────────────────────────────────┐
                  │ SILVER — Delta                                         │
                  │ typed schema · canonical request · quality flags       │
                  │ partition: event_date · cluster: tenant_id             │
                  └──────────────────────────┬──────────────────────────────┘
                                             │ aggregate mỗi 5 phút
                                             ▼
                  ┌─────────────────────────────────────────────────────────┐
                  │ GOLD — Delta                                           │
                  │ tenant/model/window · p50/p95 · token · cost · errors  │
                  │ partition: event_date · retention: 365 ngày            │
                  └──────────────┬─────────────────────────┬────────────────┘
                                 │                         │
                                 ▼                         ▼
                        Dashboard/SQL             Incident Review
                        chỉ đọc Gold       quyền tạm thời Bronze/Silver
```

### 3.1 Luồng ingestion

LLM Gateway phát một event sau khi request kết thúc. Event mang `event_id`, `tenant_id`, timestamp, model, token usage, latency, status và payload. Kafka dùng `tenant_id` làm key để duy trì thứ tự tương đối trong từng tenant. PII Ingestion Gateway kiểm tra data contract, token hóa định danh và mã hóa payload nhạy cảm trước khi dữ liệu đi vào vùng lakehouse dùng chung. Event không hợp lệ được chuyển sang quarantine thay vì làm dừng cả stream.

Streaming job ghi micro-batch 30–60 giây vào Bronze. Silver chuẩn hóa schema, loại duplicate theo `event_id`, xử lý late data bằng watermark và `MERGE` chỉ khi event mới hơn bản hiện có. Gold tổng hợp theo cửa sổ 5 phút, tenant và model. Dashboard chỉ đọc Gold; điều tra viên muốn đọc payload chi tiết phải có quyền tạm thời, lý do truy cập và audit record.

### 3.2 Luồng truy vấn và xóa dữ liệu

Dashboard lọc theo `tenant_id` và thời gian trên Gold. Incident review bắt đầu từ metric bất thường, lấy `event_id` ở Silver rồi mới dereference payload được mã hóa ở Bronze. Quyền giải mã không được cấp mặc định cho analyst.

Khi có yêu cầu xóa, control plane ghi một deletion request có `tenant_id`, phạm vi và deadline. Job xóa cập nhật Bronze/Silver, phát thay đổi qua Change Data Feed, rebuild Gold bị ảnh hưởng và xác nhận không còn bản ghi sống. Dữ liệu ở snapshot cũ chỉ biến mất vật lý sau khi hết retention; trạng thái này phải được phản ánh trong audit thay vì tuyên bố xóa hoàn tất ngay lập tức.

## 4. Các quyết định kiến trúc và phương án bị loại

### 4.1 Chọn Delta Lake làm table format cho hot path

**Tôi chọn Delta Lake** vì pipeline cần transaction ACID, schema enforcement, `MERGE`, Change Data Feed và time travel. Đây là các cơ chế trực tiếp cho dedup, late data, rollback lần ghi lỗi và truyền sự kiện xóa xuống bảng dẫn xuất.

**Tôi loại Parquet thuần** vì file không cung cấp transaction log hoặc snapshot nhất quán. Reader có thể quan sát trạng thái nửa cũ nửa mới khi writer đang thay file; MERGE và rollback phải tự xây dựng.

**Tôi chưa chọn Iceberg cho hot path** vì yêu cầu quan trọng là Delta CDF và đội vận hành đã có kinh nghiệm với Delta. Iceberg mạnh về catalog mở và multi-engine; nó sẽ được đánh giá lại nếu portability trở thành ràng buộc cao hơn CDF. Quyết định này không khẳng định Delta luôn tốt hơn Iceberg, mà chỉ phù hợp hơn với workload hiện tại.

### 4.2 Chọn catalog tập trung tương thích REST

**Tôi chọn một catalog tập trung có REST API** để quản lý namespace, schema, table version, owner, quyền truy cập và lineage. Compute engine không được tự suy đoán bảng bằng cách duyệt object storage.

**Tôi loại metastore cục bộ theo từng engine** vì Spark, DuckDB và các job vận hành có thể nhìn thấy các phiên bản schema khác nhau. Phân quyền và audit cũng bị phân mảnh.

**Tôi loại đăng ký bảng thủ công theo từng team** vì dễ tạo tên trùng, bảng mồ côi và ownership không rõ. Catalog là control plane; object storage chỉ là data plane.

### 4.3 Chọn tokenization trước Bronze dùng chung

**Tôi chọn phát hiện và token hóa PII tại ingestion gateway**, trước khi event được ghi vào vùng Bronze mà analyst hoặc pipeline thông thường có thể truy cập. Payload nguyên bản chỉ được lưu khi thật sự cần cho incident review, phải mã hóa bằng khóa tách biệt và được tham chiếu qua URI/token.

**Tôi loại redaction tại Silver** vì PII đã tồn tại ở Bronze, transaction log, snapshot và có thể đã bị sao chép trước khi job Silver chạy.

**Tôi loại redaction tại query time** vì kết quả phụ thuộc vào từng client và dữ liệu dạng rõ vẫn tồn tại trong storage. Một query hoặc export cấu hình sai có thể làm lộ toàn bộ payload.

### 4.4 Chọn partition theo thời gian, clustering theo tenant

**Tôi chọn Bronze partition theo `event_date/hour`, Silver và Gold theo `event_date`; `tenant_id` là clustering/Z-ORDER key.** Thời gian có cardinality hữu hạn và phù hợp retention. Clustering hỗ trợ hot path lọc tenant mà không sinh một thư mục partition cho từng khách hàng.

**Tôi loại partition trực tiếp theo `tenant_id`** vì cardinality cao và tenant không cân bằng. Tenant nhỏ sẽ tạo file rất nhỏ, tenant lớn gây skew và số partition tăng không kiểm soát.

**Tôi loại chỉ partition Bronze theo ngày** vì mỗi ngày có 5 TB raw. Một lần backfill hoặc incident scan theo vài giờ sẽ đọc phạm vi quá rộng; compaction và cleanup cũng khó cô lập hơn.

### 4.5 Chọn micro-batch kèm compaction bắt buộc

**Tôi chọn micro-batch 30–60 giây**, ghi file staging rồi compact định kỳ về target 256–512 MB. Z-ORDER/clustering chạy theo `tenant_id` trên các partition hot sau khi dữ liệu đủ lớn. Metrics bắt buộc gồm file count, median file size, metadata bytes và files-scanned ratio.

**Tôi loại một file cho từng request hoặc batch cực nhỏ** vì có thể tiến tới hàng tỷ object/ngày. Chi phí mở file, list object và lập kế hoạch truy vấn tăng phi tuyến dù tổng byte không đổi.

**Tôi loại batch theo ngày hoặc theo giờ dài** vì dashboard phải mới trong 5 phút. Kiến trúc tách freshness của ingestion khỏi kích thước file tối ưu bằng compaction nền.

### 4.6 Chọn Parquet + ZSTD và projection có kiểm soát

**Tôi chọn Parquet với ZSTD** vì dữ liệu metric dạng cột có độ lặp cao, thường nén tốt; column pruning cho phép dashboard không đọc prompt/response. Row-group statistics hỗ trợ data skipping.

**Tôi loại JSON làm định dạng phân tích lâu dài** vì lặp key, parse tốn CPU và không có column pruning hiệu quả. JSON chỉ phù hợp làm envelope đầu vào hoặc quarantine.

**Tôi loại nén cực mạnh cho toàn bộ dữ liệu** nếu nó làm CPU giải nén khiến dashboard vi phạm SLA. Compression level phải được benchmark; payload lạnh có thể dùng mức cao hơn bảng Gold nóng.

### 4.7 Chọn lifecycle theo giá trị dữ liệu, không chỉ theo tuổi

**Tôi chọn giữ Bronze/Silver chi tiết 7 ngày và Gold aggregate 365 ngày.** Snapshot retention ngắn hơn retention nghiệp vụ nhưng đủ cửa sổ rollback. Snapshot expiry luôn đi cùng orphan scan; checkpoint và transaction log có lịch bảo trì riêng.

**Tôi loại giữ prompt/response đầy đủ một năm** vì vượt nhu cầu incident review, làm tăng rủi ro PII và tiêu tốn ngân sách cho dữ liệu ít được truy vấn.

**Tôi loại cách chỉ chạy `VACUUM` rồi coi storage đã sạch**. Kết quả Day 18 cho thấy file do writer crash trước commit không xuất hiện trong transaction log, nên có thể vô hình với vacuum. Vận hành phải so sánh tập file trên storage với tập file được snapshot tham chiếu, có minimum-age an toàn trước khi xóa.

## 5. Data contract, chất lượng và governance

Schema bắt buộc gồm `event_id`, `event_ts`, `tenant_id_token`, `model`, `latency_ms`, `prompt_tokens`, `completion_tokens`, `status`, `sdk_version`, `schema_version` và tham chiếu payload. `event_id` phải duy nhất; token không âm; latency không âm; timestamp không được vượt quá ngưỡng clock-skew. Cột mới chỉ được thêm bằng quy trình review và schema evolution opt-in. Thay đổi kiểu hoặc xóa cột được xem là breaking change.

Mỗi bảng có owner, SLA, retention, data classification và lineage. Analyst chỉ có quyền Gold mặc định. Quyền đọc Silver hoặc giải mã payload được cấp theo thời gian, gắn ticket và ghi audit. Training hoặc phân tích ngoại tuyến phải pin `table_version` để có thể tái hiện đúng tập dữ liệu đã dùng.

Các metric vận hành chính:

- Kafka consumer lag và end-to-end freshness p50/p95.
- Số record bị quarantine, duplicate và late event.
- Tỷ lệ phát hiện PII trên canary cố định.
- Số file, median file size và tỷ lệ file được data skipping.
- Transaction duration, compaction backlog và failed commit.
- Logical table bytes, physical storage bytes và orphan bytes.
- Số lượt giải mã payload, người truy cập và lý do truy cập.

## 6. Failure modes lúc 3 giờ sáng

| Failure mode | Cách phát hiện | Cô lập và rollback |
|---|---|---|
| SDK mới đổi kiểu `latency_ms` từ số sang chuỗi, làm stream lỗi | Schema-contract alert, consumer lag tăng và quarantine rate tăng | Không tự merge kiểu xung đột; đưa event vào quarantine, rollback SDK hoặc thêm parser tương thích rồi replay từ Kafka offset đã commit |
| Deployment lỗi ghi `latency_ms = -1` hoặc tính sai token cost | Constraint check, anomaly trên error budget và so sánh canary tenant | Dừng writer, xác định version đầu tiên bị lỗi, time travel về snapshot sạch; sửa job rồi replay đúng khoảng event |
| Micro-batch sinh hàng triệu file nhỏ | File count tăng, median file size xuống dưới ngưỡng, query planning p95 tăng | Giới hạn writer concurrency, tăng batch interval, compact partition hot và điều chỉnh target file size |
| PII detector không hoạt động nhưng pipeline vẫn chạy | Canary chứa số điện thoại/email mẫu không bị đánh dấu; detection rate giảm đột ngột | Fail closed: chuyển event vào quarantine, thu hồi quyền đọc partition liên quan, sửa detector và replay trước khi publish Silver |
| Gold consumer chậm hơn SLA 5 phút | Kafka lag, watermark age và freshness SLO alert | Tăng consumer capacity, tạm dừng compaction tranh tài nguyên, replay từ checkpoint; dashboard tiếp tục đọc snapshot Gold nhất quán cuối cùng |
| Xóa snapshot nhưng physical storage không giảm | Snapshot count giảm nhưng storage bytes/orphan bytes không giảm | Chạy orphan discovery sau minimum age; dry-run, đối chiếu file đang được snapshot tham chiếu rồi mới xóa |
| Yêu cầu xóa tenant chỉ được áp dụng ở Delta nhưng external export còn bản sao | CDF reconciliation thấy delete chưa có acknowledgement từ downstream | Chặn export mới, phát lại deletion event, rebuild partition Gold và chỉ đóng ticket khi mọi consumer xác nhận |

Runbook không được dùng `VACUUM retention=0` trong production. Mọi xóa vật lý cần dry-run, minimum age, danh sách snapshot tham chiếu và audit record. Rollback bằng time travel chỉ là bước khôi phục logic; sau đó vẫn phải sửa nguồn lỗi và replay để trạng thái hội tụ.

## 7. Ước lượng chi phí back-of-envelope

Đây là estimate kiến trúc, không phải báo giá nhà cung cấp. Tôi giả định object storage hot có giá **23 USD/TB-tháng**, storage aggregate cũng dùng cùng mức giá để estimate bảo thủ, Parquet + ZSTD nén **2,5 lần**, và dành **15%** cho transaction log, metadata, snapshot cùng headroom. Giá thực tế phải được thay bằng provider/region tại thời điểm triển khai.

### 7.1 Dữ liệu chi tiết 7 ngày

```text
1 tỷ request/ngày × 5 KB/request       = 5 TB/ngày raw
5 TB/ngày × 7 ngày                     = 35 TB raw
35 TB / 2,5 (tỷ lệ nén giả định)       = 14 TB
14 TB × 1,15 (log + snapshot + dư địa) = 16,1 TB
16,1 TB × 23 USD/TB-tháng              = 370,30 USD/tháng
```

Nếu tỷ lệ nén chỉ đạt 1,5 lần, footprint sẽ là `35 / 1,5 × 1,15 = 26,83 TB`, tương đương khoảng `617 USD/tháng`. Cả hai trường hợp vẫn dưới cap, nhưng cần đo bằng dữ liệu thật vì prompt đã mã hóa hoặc nén sẵn có thể nén kém.

### 7.2 Gold aggregate một năm

Giả định có 10.000 tenant hoạt động, 3 nhóm model, một dòng aggregate cho mỗi cửa sổ 5 phút và mỗi dòng nén còn 100 byte:

```text
10.000 tenant × 3 model × 288 cửa sổ/ngày × 365 ngày
    = 3.153.600.000 dòng
3.153.600.000 × 100 byte                ≈ 315,36 GB
Cộng metadata, version và headroom       ≈ 0,40 TB
0,40 TB × 23 USD/TB-tháng               = 9,20 USD/tháng
```

### 7.3 Budget envelope

```text
Dữ liệu chi tiết 7 ngày                 ≈   371 USD/tháng
Gold aggregate 365 ngày                 ≈    10 USD/tháng
Catalog, log, checkpoint, headroom       =   150 USD/tháng
Object requests và replication dự phòng =   500 USD/tháng
Subtotal                                ≈ 1.031 USD/tháng
Dự phòng 100% sai số                    ≈ 1.031 USD/tháng
Budget storage đề xuất                  ≈ 2.062 USD/tháng
Cap                                     = 5.000 USD/tháng
```

Estimate còn khoảng 2.938 USD/tháng biên an toàn. Tuy nhiên, cap đề bài chỉ nói storage; Kafka, streaming compute, SQL warehouse, egress, KMS và PII detection phải có budget riêng. Tôi không gộp compute vào storage để tạo cảm giác giả rằng toàn hệ thống chỉ tốn 2.062 USD. Trước production, PoC phải đo compression ratio, số object request, snapshot amplification và chi phí replication bằng billing calculator của region thực tế.

## 8. MVP có thể giao trong một tuần

MVP xử lý 10 triệu event mô phỏng thay vì cố chứng minh ngay throughput 1 tỷ request/ngày. Slice nhỏ nhất phải đi xuyên suốt từ ingestion đến dashboard và rollback:

| Ngày | Deliverable |
|---|---|
| 1 | Chốt data contract; sinh dữ liệu có PII, duplicate, late event và schema lỗi |
| 2 | Kafka-to-Bronze micro-batch; tokenization, encryption pointer và quarantine |
| 3 | Bronze-to-Silver bằng dedup, validation và `MERGE` có điều kiện timestamp |
| 4 | Silver-to-Gold aggregation 5 phút theo tenant/model; dashboard query mẫu |
| 5 | Benchmark truy vấn tenant trước/sau clustering; đo file count và compression |
| 6 | Inject bad data; thực hành time travel rollback, replay và orphan cleanup |
| 7 | Chạy load test, đo freshness/cost, hoàn thiện runbook và design review |

MVP được coi là đạt khi:

- P95 end-to-end freshness không quá 5 phút.
- Canary PII không xuất hiện dạng rõ trong bảng analyst có quyền đọc.
- Duplicate `event_id` không làm tăng metric Gold.
- Query theo tenant chứng minh data skipping và không full scan.
- Bad deployment được rollback về đúng snapshot rồi replay thành công.
- Compaction giảm rõ số file mà không làm mất record.
- Estimate nội suy từ số byte thực đo vẫn nằm dưới storage cap.

MVP chưa cần multi-region, dashboard hoàn chỉnh hay autoscaling production. Nếu slice này không chứng minh được privacy boundary, freshness và rollback, mở rộng quy mô chỉ làm failure đắt hơn.

## 9. Kết luận

Thiết kế sử dụng medallion để tách dữ liệu chi tiết khỏi workload dashboard, Delta Lake cho ACID/CDF/time travel, catalog làm control plane, tokenization trước Bronze dùng chung và lifecycle theo giá trị dữ liệu. Quyết định quan trọng nhất không phải chọn một table format, mà là biến các nghĩa vụ vận hành—compaction, snapshot expiry, orphan removal, schema review, deletion propagation và audit—thành job có metric và owner cụ thể.

Kiến trúc chấp nhận thêm độ phức tạp ở ingestion và control plane để giảm rủi ro PII, giữ truy vấn ổn định và phục hồi được khi có bad write. Các giả định về compression, tenant count và giá storage sẽ được thay bằng số đo của MVP trước khi phê duyệt production.
