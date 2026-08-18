# CDC từ ride-hailing → Lakehouse, dưới ràng buộc Nghị định 13

**Bonus Challenge topic C** · Track 2 Day 18 · Trần Nguyễn Thế Nhật

> **Phạm vi.** Đây là quyết định kiến trúc, không phải ý kiến pháp lý. Phần tuân thủ
> viết ở mức thiết kế hệ thống; việc diễn giải Nghị định 13/2023/NĐ-CP cho một sản
> phẩm cụ thể cần bộ phận pháp chế. Mọi con số $ là giá niêm yết công khai nhân với
> giả định được ghi rõ — kiểm lại được, không phải đo trên hệ thật.

---

## 1. Problem statement

Một hãng gọi xe Việt Nam chạy Oracle làm OLTP: 100 triệu chuyến/năm, **30K writes/s
lúc cao điểm**. Analytics hiện đọc trực tiếp replica, đã tới hạn. Cần chuyển sang
lakehouse qua Debezium CDC với ba ràng buộc đồng thời.

**Độ trễ:** dashboard phải phản ánh một commit ở Oracle trong **60 giây**; ad-hoc
query p95 **< 1 giây**. Hai con số này kéo về hai hướng ngược nhau — cái đầu đòi ghi
liên tục, cái sau đòi file lớn và đã được sắp xếp.

**Dữ liệu đến muộn:** tài xế ở tỉnh xa mất sóng hàng giờ. Sự kiện CDC tới **sai thứ
tự** là chuyện thường ngày, không phải ngoại lệ. Một `MERGE` viết ngây thơ sẽ để bản
ghi cũ đè lên trạng thái mới — và chạy *thành công*, không báo lỗi.

**PII:** số điện thoại, CMND/CCCD, toạ độ GPS của cả tài xế lẫn hành khách nằm trong
phạm vi NĐ13. Nghị định đòi giới hạn truy cập, ghi nhật ký mọi lần đọc, và thực thi
quyền xoá của chủ thể dữ liệu. Ràng buộc cuối xung đột trực tiếp với time travel —
thứ mà chính yêu cầu audit lại cần.

Cái khó không nằm ở một ràng buộc nào cả. Nó nằm ở chỗ **giải pháp cho ràng buộc này
làm hỏng ràng buộc kia**.

*(198 từ)*

---

## 2. Kiến trúc

```
 OLTP                    INGEST                      LAKEHOUSE                      TIÊU THỤ
┌──────────┐   redo    ┌──────────┐  Kafka    ┌─────────────────────────────┐   ┌──────────────┐
│  Oracle  │──log───▶  │ Debezium │──topic──▶ │ ▓ BRONZE  append-only        │   │ Dashboard    │
│  (prod)  │           │ connector│  (7d      │   tokenize PII TẠI ĐÂY       │   │ refresh 5s   │
│ 30K w/s  │           └──────────┘  retain)  │   partition: ingest_hour     │   │ p95 < 1s     │
└──────────┘                │                 │   giữ 30d hot → Glacier IR   │   └──────▲───────┘
     │                      │                 └──────────────┬──────────────┘          │
     │ ┌────────────────────▼──────────┐                     │ MERGE 30s                │
     │ │ KMS ─ khoá HMAC               │                     │ guard: s.src_ts>t.src_ts │
     │ │ (KHÔNG nằm trong lakehouse)   │                     ▼                          │
     │ └───────────────────────────────┘      ┌─────────────────────────────┐           │
     │                                        │ ▓ SILVER                    │           │
     │                                        │  drivers_current  (SCD2 cur)│           │
     │                                        │  drivers_history  (append)  │           │
     │                                        │  trips_fact                 │           │
     │                                        │  partition: city / event_dt │           │
     │                                        │  z-order: (city, driver_id) │           │
     │                                        └──────────────┬──────────────┘           │
     │                                                       │ agg 5 phút               │
     │                                                       ▼                          │
     │                                        ┌─────────────────────────────┐           │
     │                                        │ ▓ GOLD  metrics theo         │───────────┘
     │                                        │  (city, hour, driver_tier)  │
     │                                        └─────────────────────────────┘
     │                                                       │
     │  ┌──────────────────┐   ┌──────────────────┐         │ CDF (delete events)
     └─▶│ VAULT tái định   │   │ AUDIT mọi lần    │◀────────┘         │
        │ danh (tách biệt, │   │ đọc PII + mọi    │                   ▼
        │ audit riêng)     │   │ yêu cầu xoá      │      ┌──────────────────────────┐
        └──────────────────┘   └──────────────────┘      │ Search index / cache /   │
                                                          │ ML feature store         │
        QUARANTINE ◀── sự kiện muộn > 24h watermark       │ (ĐĂNG KÝ delete, không   │
             │         (job đối soát hằng ngày)           │  đoán — xem NB7)         │
             └────────▶ MERGE bù vào Silver               └──────────────────────────┘
```

Ba điểm cần đọc kỹ trên sơ đồ: **tokenization nằm ở Bronze landing** (không phải ở
Silver — xem QĐ 4); **guard `src_ts` nằm trên đường MERGE** (không phải kiểm sau —
xem QĐ 5); và **quarantine là một nhánh có thật**, không phải xử lý ngoại lệ (QĐ 5).

---

## 3. Sáu quyết định chính

### QĐ 1 — Table format: **Delta Lake**

**Loại Apache Hudi** — lựa chọn khó bỏ nhất, và bỏ vì lý do vận hành chứ không phải
kỹ thuật. Hudi MOR với record-level index được xây cho đúng bài toán upsert tần suất
cao này. Nhưng bề mặt vận hành của nó (timeline service, COW/MOR per table, cấu hình
clustering, compaction inline vs async) cần một người sở hữu toàn thời gian. SLA của
ta là 60 giây chứ không phải 5 giây — tức là ta **không cần** phần Hudi giỏi nhất mà
vẫn trả trọn chi phí vận hành nó.

**Loại Apache Iceberg.** Catalog story và tính trung lập engine tốt hơn Delta rõ rệt,
và hidden partitioning giải quyết đúng cái bẫy mà QĐ 3 phải xử lý thủ công. Bỏ vì hai
lý do đo được: (a) ở 30K writes/s, equality delete của v2 tạo read amplification mà
mọi query sau đó phải trả; (b) NB6 đo được `expire_snapshots` **không xoá một file
avro nào** (20→3 snapshot, 0 file bị xoá, metadata còn phình 337.6→345.4 KB) — nghĩa
vụ chain expiry với orphan sweep rơi lên đội vận hành.

**Chọn Delta vì:** Change Data Feed là *native* và chúng ta cần nó ở hai chỗ khác nhau
(lan truyền xoá xuống downstream, và feed cho feature store); deletion vector làm phép
xoá lẻ rẻ, mà xoá lẻ chính là hình dạng của yêu cầu NĐ13; và `MERGE` của Delta là thứ
PoC đã chạy thật.

### QĐ 2 — Đường ingest: **Kafka → Spark Structured Streaming, micro-batch 30s**

**Loại Kafka Connect S3 sink + job batch riêng.** Thêm một chặng, biến exactly-once
thành at-least-once cộng một bước dedup ta phải tự viết đúng; và sink flush + batch
schedule đã ăn hết ngân sách 60 giây trước khi tính thời gian MERGE.

**Loại kéo JDBC incremental thẳng từ Oracle.** Rẻ và đơn giản, nhưng đặt tải lên chính
OLTP production, không thấy `DELETE`, và không thấy thứ tự commit thật — mà thứ tự
commit chính là dữ kiện QĐ 5 dựa vào.

**Chọn micro-batch 30s** vì nó chia đôi ngân sách 60s: 30s tích luỹ + ~15s MERGE +
~15s dự phòng. Cái giá phải trả là 2.880 commit/ngày/bảng — tức là chính bài toán
small-files của NB6, được xử lý ở QĐ phụ về lifecycle bên dưới.

### QĐ 3 — Partition: **`city` + `event_date`** (thời điểm sự kiện, không phải thời điểm nạp)

**Loại partition theo `ingest_date`.** Đây là lựa chọn *cám dỗ* vì nó làm ghi nhanh và
không bao giờ phải sửa partition cũ. Nó cũng là nguồn của lỗi 3 giờ sáng: một sự kiện
chuyến ngày 5 tới vào ngày 7 sẽ nằm ở partition ngày 7, nên câu hỏi "doanh thu ngày 5"
buộc phải quét mọi partition ingest — đúng thứ partition sinh ra để tránh.

**Loại `hash(driver_id)` 1024 bucket.** Phân bố ghi đều, tránh hotspot Hà Nội/TP.HCM.
Nhưng nó phá nát pattern query chủ đạo ("thành phố X, ngày Y"), và mỗi micro-batch
MERGE sẽ chạm cả 1024 bucket thay vì vài cái.

**Thừa nhận tradeoff:** vì đã chọn Delta ở QĐ 1, `city`/`event_date` là **cột thật**,
nên người viết query vẫn có thể quên predicate — chính cái bẫy Hive mà NB5 chỉ ra
Iceberg đã xoá bỏ bằng hidden partitioning. Bù bằng cách bắt buộc analyst đi qua một
lớp view có sẵn predicate, và cảnh báo khi query quét quá N partition. Đây là bù đắp
bằng quy trình cho một thiếu sót của format — nói thẳng như vậy chứ không giả vờ là
không có.

### QĐ 4 — PII: **tokenization HMAC tất định tại Bronze landing**

**Loại mã hoá at-rest đơn thuần.** Thoả "mã hoá khi lưu trữ" trên giấy, nhưng mọi
analyst có quyền đọc bảng đều thấy số rõ lúc query. NĐ13 đòi *giới hạn truy cập*.

**Loại masking động lúc query.** Bản rõ vẫn nằm trên đĩa: một snapshot, một backup,
hay một grant sai là lộ — và time travel giữ version cũ chứa bản rõ vô thời hạn.

**Loại token ngẫu nhiên.** An toàn hơn về mật mã nhưng phá join giữa các bảng; ở 30K
events/s mỗi join thành một lượt tra vault.

**Chọn HMAC-SHA256 có khoá ở KMS**, không phải SHA256 trần: dải số điện thoại VN chỉ
~10⁹ giá trị, dựng bảng cầu vồng mất vài phút. **PoC đã chứng minh** cả ba tính chất
cần: token tất định qua các lần gọi, hai số khác cho hai token khác, và **0 file
Parquet nào chứa byte số điện thoại thô** — phép kiểm này được đối chứng bằng cách cố
tình ghi số thô để xác nhận nó thật sự bắt được rò rỉ, chứ không pass rỗng.

### QĐ 5 — Dữ liệu đến muộn: **guard `s.src_ts > t.src_ts` + watermark 24h + quarantine**

**Loại "last write wins".** Không phải vì nó xấu về lý thuyết mà vì **PoC đo được nó
hỏng**: một sự kiện `offline` lúc 10:00 tới sau đã đè lên trạng thái `online` lúc
10:05. Kết quả là tài xế đang chạy bị đánh dấu offline, hệ điều phối ngừng gán chuyến,
và **không có lỗi nào được báo** vì MERGE chạy thành công.

**Loại "gom cả ngày, sort rồi dựng lại partition".** Đúng về mặt correctness và đơn
giản để suy luận. Nhưng dựng lại partition một ngày ở khối lượng này không thể nằm
trong 60 giây, và biến một phép ghi tăng dần thành một phép ghi lại toàn bộ mỗi ngày.

**Loại watermark vô hạn.** Giữ mọi thứ để chờ bản ghi muộn nghĩa là state lớn không
giới hạn.

**Chọn:** gom batch về 1 dòng/khoá (bắt buộc — xem E2b mục 5), guard trên đường MERGE
để bảng current chỉ nhận bản ghi mới hơn, bảng history append-only nhận *mọi* bản ghi
kể cả muộn, watermark 24h, và mọi thứ tới sau đó rơi vào quarantine cho job đối soát
hằng ngày. Quarantine là một nhánh có thiết kế, không phải một cái `try/except`.

### QĐ 6 — Quyền xoá vs time travel: **deletion vector + retention 30 ngày là quyết định có văn bản**

Đây là chỗ hai yêu cầu của cùng một nghị định đánh nhau.

**Loại "cứ DELETE rồi để time travel tự hết hạn".** NB8 đo được chính xác vấn đề: sau
khi xoá dòng của `user_007`, bảng hiện tại trả về 0 — nhưng version cũ **vẫn chứa
nguyên** dữ liệu đó. Việc xoá chưa hoàn tất cho tới khi retention hết hạn. Nếu bạn trả
lời chủ thể dữ liệu là "đã xoá" ngay lúc đó, bạn đang nói sai.

**Loại "đặt retention = 0 cho chắc".** NB6 in cảnh báo này ra từ chính output:
`retention_hours=0` huỷ khả năng time travel và có thể làm gãy reader đang đọc dở. Mà
khả năng đó chính là thứ yêu cầu audit của cùng nghị định dựa vào, và là đường rollback
của failure mode F1 bên dưới.

**Chọn:** retention 30 ngày, ghi thành văn bản có chữ ký của pháp chế chứ không để giá
trị mặc định; một **sổ yêu cầu xoá** ghi nhận thời điểm nhận và thời điểm dữ liệu thật
sự biến mất khỏi mọi version; và `VACUUM` cưỡng bức cho riêng lô erasure khi deadline
pháp lý ngắn hơn 30 ngày. Xoá được lan truyền xuống downstream bằng CDF — cơ chế mà
PoC đã chạy và NB7 đã cho thấy hậu quả khi thiếu nó (0 hit trong bảng, 8 hit trong
index ngoài).

### Các quyết định phụ

| Hạng mục | Chọn | Loại, và vì sao |
|---|---|---|
| Catalog | Unity Catalog (hoặc Polaris nếu tự vận hành) | Loại Hive Metastore: không có column-level grant, mà NĐ13 cần chính thứ đó. Loại "không catalog, trỏ path": mất kiểm soát truy cập và mất kiểm kê bảng chứa PII. |
| Nén | zstd level 3 | Loại snappy: nhanh hơn ~15% khi ghi nhưng tốn ~35% dung lượng — sai hướng khi 90% hoá đơn là compute chứ không phải storage. Loại gzip: nén tốt, giải nén chậm, đánh thẳng vào SLA p95 < 1s. |
| Lifecycle | Bronze 30d hot → Glacier IR; Silver 90d Standard → IA; compaction 02:00 hằng đêm | Loại giữ tất cả ở Standard: lãng phí. Loại compact mỗi giờ: NB6 đo được compaction **tăng** dung lượng tạm thời (10.1 → 16.1 MB trước khi vacuum thu hồi) — chạy càng dày càng chồng chi phí kép. |

---

## 4. Năm failure mode

### F1 — Debezium snapshot restart lúc 3:12 sáng

Connector restart và đọc lại snapshot toàn bảng. Kafka nhận vài trăm triệu sự kiện
mang `src_ts` cũ. Guard ở QĐ 5 chặn được phần lớn ghi đè sai, nhưng cụm streaming
nghẽn và độ trễ vọt qua 60s.

*Phát hiện:* tỉ lệ commit/phút và `num_target_rows_updated` mỗi MERGE so với median 7
ngày; cảnh báo khi vượt 10×. Đây là metric có sẵn trong `operationMetrics` của Delta —
NB3 in ra chính các trường này.
*Rollback:* dừng consumer, `RESTORE` bảng Silver về version ngay trước cơn bão, tua
Kafka offset về mốc đã ghi, chạy lại. Time travel ở đây không phải tính năng cho vui —
nó là đường lui.

### F2 — DDL trên Oracle: ai đó thêm cột lúc 2 giờ sáng

*Phát hiện:* Bronze **fail closed**. Đây là lựa chọn có chủ ý: NB1 cho thấy schema
enforcement chặn ghi sai kiểu bằng `Cast error`, và im lặng nới schema nguy hiểm hơn
là dừng. Cộng thêm một job so schema Oracle với schema Bronze mỗi giờ.
*Rollback:* sự kiện không parse được rơi vào quarantine thay vì mất; sau khi người
trực xem xét thì áp `schema_mode="merge"` một cách có chủ đích. Tiến hoá schema là
quyết định của con người, không phải mặc định của hệ thống.

### F3 — Job đối soát quarantine chết âm thầm

Sự kiện muộn vẫn được ghi vào quarantine, nhưng không ai bù vào Silver. Không có lỗi
nào — chỉ là số liệu trôi dần khỏi sự thật.

*Phát hiện:* job đối soát hằng ngày so `count(*)` theo (city, event_date) giữa Oracle
và Silver, cảnh báo khi lệch > 0.1%; cộng thêm alert riêng cho "tuổi bản ghi cũ nhất
trong quarantine". Failure mode nguy hiểm nhất là loại không tạo ra lỗi.
*Rollback:* MERGE bù có guard — cùng đúng phép toán, chạy lại được nhiều lần.

### F4 — Small files bóp nghẹt query path

2.880 commit/ngày/bảng × 50 partition. Sau một tháng không compaction, dashboard tụt
khỏi p95 < 1s mà không có sự kiện nào để đổ lỗi.

*Phát hiện:* số file/partition và kích thước file trung bình là metric hạng nhất, có
ngưỡng cảnh báo. NB6 đo được baseline 200 file với trung bình **51.5 KB/file**, so với
mục tiêu production 128–512 MB.
*Rollback:* compaction là idempotent, chạy được bất cứ lúc nào — nhưng phải dự trù
dung lượng chồng đôi tạm thời (đo được ở NB6: 10.1 → 16.1 MB trước khi vacuum về 6.2 MB).

### F5 — Yêu cầu xoá đến, nhưng dữ liệu vẫn sống ở version cũ *(liên hệ Day 18)*

Chủ thể dữ liệu yêu cầu xoá. `DELETE` chạy, bảng hiện tại trả về 0 dòng, ticket được
đóng. Nhưng `VERSION AS OF` cách đó ba ngày vẫn trả về đầy đủ PII — **và bất kỳ ai có
quyền đọc bảng đều time-travel được**.

*Phát hiện:* sổ erasure có bộ đếm; một job xác minh chạy `VERSION AS OF` trên toàn bộ
version còn trong retention và assert rằng chủ thể đã biến mất. Không đo thì không
biết.
*Rollback:* không có "rollback" cho việc này — chỉ có `VACUUM` cưỡng bức cho lô đó,
và một cuộc trao đổi với pháp chế về việc retention 30 ngày có nằm trong deadline
pháp lý hay không. Đây là failure mode duy nhất mà **câu trả lời đúng là thay đổi
chính sách, không phải sửa code**.

---

## 5. Chứng minh độ tin cậy

Mục 3 và 4 mới chỉ *tuyên bố* rằng đường ingest chịu được sự kiện sai thứ tự, chịu
được retry và chịu được ghi đồng thời. Ba câu đó là giả thuyết. Mục này biến chúng
thành mệnh đề kiểm chứng được rồi đo — `poc/reliability_proof.py`, output đầy đủ ở
`poc/output-reliability.txt`.

Nguyên tắc xuyên suốt: **mỗi tính chất đều chạy kèm phương án SAI trên cùng dữ liệu**,
để con số "tin cậy" có cái để so. Một phép kiểm luôn xanh không chứng minh gì cả.

### E1 — Bất biến theo thứ tự đến *(thí nghiệm quan trọng nhất)*

Cùng một tập 12 sự kiện CDC cho 4 tài xế, xáo trộn thứ tự đến 200 lần. Trạng thái
current cuối cùng được băm thành một vân tay để so sánh chính xác.

| Cấu hình | Số trạng thái cuối khác nhau / 200 lần | Số lần sai |
|---|---:|---:|
| **Có guard `s.src_ts > t.src_ts`** | **1** | **0 / 200** |
| Không guard (last-write-wins) | 74 | 198 / 200 |

Và để loại trừ khả năng "lấy mẫu may mắn", chạy thêm **vét cạn toàn bộ 720 hoán vị**
của 6 sự kiện đầu: vẫn đúng **1** trạng thái cuối duy nhất.

Đây là bằng chứng mạnh nhất trong cả tài liệu. Nó nói rằng với guard, **"sự kiện đến
muộn" thôi không còn là một ca đặc biệt** — kết quả không phụ thuộc vào chất lượng
sóng ở tỉnh xa. Không có guard, 99% số lần chạy cho ra trạng thái sai, và mỗi lần sai
một kiểu.

### E2 — Idempotent dưới retry

Phát lại y nguyên cùng một batch 5 lần liên tiếp (mô phỏng Kafka at-least-once và
connector restart): vân tay `61d9cdff13b2`, 4 dòng — **không đổi qua cả 5 lần**. Guard
`>` (không phải `>=`) từ chối luôn bản ghi có `src_ts` bằng nhau, nên phát lại không
sinh dòng trùng.

*Vân tay này trùng với vân tay của E1 — hai thí nghiệm độc lập hội tụ về cùng một
trạng thái, đúng như phải thế.*

### E2b — Một ràng buộc của format mà thiết kế phải tính đến

Thí nghiệm này phát hiện ra một điều **không nằm trong dự tính ban đầu**: Delta từ
chối `MERGE` khi một dòng đích khớp nhiều dòng nguồn cùng thoả mệnh đề `WHEN MATCHED`:

```
DeltaError: MERGE matched a target row with multiple source rows
that satisfy duplicate relevant WHEN MATCHED clauses
```

Một micro-batch 30 giây trong thực tế **gần như luôn** chứa nhiều sự kiện cho cùng một
tài xế. Nghĩa là bước gom về một dòng/khoá (`ROW_NUMBER() OVER (PARTITION BY key ORDER
BY src_ts DESC) = 1` — đúng phép dedup NB4 dùng ở Silver) nằm trên **đường chạy chính**,
không phải trên nhánh ngoại lệ. Nếu tôi bỏ qua chi tiết này khi thiết kế, pipeline sẽ
chết ngay batch đầu tiên có hai sự kiện cùng tài xế.

Đáng nói hơn: đối chứng đầu tiên tôi viết chạy trên **bảng rỗng** nên mọi dòng đi vào
nhánh `INSERT`, xung đột không bao giờ lộ ra, và phép kiểm báo xanh sai. Phải seed
bảng trước thì lỗi mới hiện. *Một phép kiểm âm tính chưa được đối chứng thì chưa phải
bằng chứng.*

### E3 — Ghi đồng thời

8 writer cùng `MERGE` vào một bảng, có retry khi xung đột optimistic-concurrency:

```
Writer commit thành công : 12/12
Version trong log        : 10
Dòng đọc lại được        :  4   (đúng 1 dòng/tài xế — không nhân bản)
Trạng thái cuối          : {1: busy, 2: online, 3: busy, 4: busy}  ← khớp trạng thái đúng
```

Không mất update, không hỏng bảng. Writer thua cuộc đọc lại snapshot mới rồi commit
lại, chứ không ghi đè mù — đây là ACID của Delta làm việc, và là lý do QĐ 2 dám cho
nhiều task streaming ghi song song.

### E4 — Từ chối ghi đè không được đồng nghĩa với mất dữ liệu

12 sự kiện vào → bảng current giữ 4 dòng (đúng thiết kế), bảng history append-only giữ
đủ **12/12**. Tám bản ghi bị guard từ chối khỏi current vẫn truy vết được đầy đủ.

Đây là điều kiện để yêu cầu audit của NĐ13 còn khả thi: nếu guard làm mất luôn bản ghi
đến muộn, hệ thống sẽ đúng về trạng thái nhưng không trả lời được câu "tài xế này đã
khai báo những gì, lúc nào".

### Tổng kết bằng chứng

```
[PASS] E1  có guard: 1 trạng thái duy nhất qua 200 thứ tự ngẫu nhiên
[PASS] E1  đối chứng: không guard thì kết quả phân kỳ (74 trạng thái, sai 198/200)
[PASS] E1  vét cạn 720 hoán vị: vẫn 1 trạng thái
[PASS] E2b batch thô bị Delta từ chối (không ghi mù)
[PASS] E2  idempotent qua 5 lần phát lại
[PASS] E3  8 writer đồng thời: không mất update, không hỏng bảng
[PASS] E4  history giữ đủ mọi sự kiện kể cả bị từ chối
```

### Cái này KHÔNG chứng minh

Nói rõ để không ai đọc quá lời:

- **Không chứng minh throughput.** Mọi thí nghiệm chạy trên bảng vài dòng, một tiến
  trình. 30K writes/s cần một cụm thật; con số đó vẫn là giả định chưa kiểm.
- **E3 chạy đa luồng trong một tiến trình**, không phải nhiều máy qua object store
  thật. S3 không có put-if-absent nguyên tử như filesystem cục bộ, nên concurrency
  đa-writer trên S3 cần một commit coordinator (DynamoDB log store hoặc catalog quản
  lý commit). Đây là rủi ro đã biết, chưa được kiểm ở đây.
- **Không chứng minh guard là đủ cho mọi loại sự kiện.** Nó bảo vệ phép cập nhật theo
  khoá. Sự kiện `DELETE` từ CDC cần một nhánh riêng (`when_matched_delete`) với cùng
  logic guard — đã thiết kế, chưa đo.
- **Không chứng minh phần tuân thủ.** Nó chứng minh cơ chế kỹ thuật (token không lộ
  byte thô, CDF phát ra delete). Việc các cơ chế đó có thoả NĐ13 hay không là kết luận
  pháp lý, không phải kết luận từ một assert.

---

## 6. Chi phí — hiện phép tính

**Giả định** (ghi rõ để kiểm lại): trung bình = 30% đỉnh → 9K events/s → 778 triệu
events/ngày. Sự kiện CDC trung bình 400 B thô. Parquet + zstd nén ~4×. GPS ping chiếm
~80% khối lượng và **không** đi vào Silver. Giá S3 us-east-1 niêm yết 2026.

| Tầng | Phép tính | Dung lượng | $/tháng |
|---|---|---:|---:|
| Bronze hot (30d, Standard) | 778M × 400 B ÷ 4 = 78 GB/ngày × 30 | 2,3 TB | $54 |
| Bronze lạnh (335d, Glacier IR) | 78 GB × 335 | 26 TB | $106 |
| Silver 0–90d (Standard) | 20 GB/ngày × 90 | 1,8 TB | $42 |
| Silver 90–365d (IA) | 20 GB/ngày × 275 | 5,5 TB | $70 |
| Gold (Standard) | 2 GB/ngày × 365 | 0,7 TB | $17 |
| PUT request | 2.880 batch × 50 partition × 30 ngày = 4,3M | — | $22 |
| **Storage tổng** | | **~36 TB** | **$311** |
| Streaming 24/7 | 6 node × $0,40/h × 730 h | — | $1.752 |
| Compaction hằng đêm | 8 node × 2 h × $0,40 × 30 | — | $192 |
| Query warehouse (ad-hoc) | ước lượng | — | $800 |
| **Compute tổng** | | | **$2.744** |
| **TỔNG** | | | **≈ $3.055/tháng** |

**Con số đáng chú ý không phải tổng, mà là tỉ lệ: compute chiếm 90% hoá đơn.** Mọi
giờ tối ưu bỏ vào S3 tiering là bỏ vào 10% còn lại. Đòn bẩy thật nằm ở việc cụm
streaming có ngủ được lúc thấp điểm không, và ở việc giữ file đủ lớn để query không
đốt compute.

Ràng buộc này khớp với thứ NB6 đo được về compaction có quản lý: thành phần tính theo
**số object** chiếm 24% hoá đơn ($240 trên tổng $990/tháng ở kịch bản 500 GB /
2 triệu file) — nó bị điều khiển bởi *số lượng file*, không phải khối lượng dữ liệu.
Sửa trigger interval của writer rẻ hơn thuê người dọn dẹp sau nó.

**Chưa tính:** truyền dữ liệu liên vùng, license Debezium/Kafka có quản lý, chi phí
lưu trữ audit log, và nhân sự. Nếu đưa vào design review thật, nhân sự nhiều khả năng
vượt cả ba dòng compute cộng lại.

---

## 7. Lát cắt tuần đầu

**Không** dựng cả medallion. Dựng **một bảng, một thành phố, đủ đầu-cuối**:

`drivers` (Oracle) → Debezium → Kafka → Bronze đã tokenize → Silver `drivers_current`
qua MERGE có guard → một câu query dashboard duy nhất: "số tài xế online theo phường,
Hà Nội".

**Tiêu chí nghiệm thu, đo chứ không cảm nhận:**

1. p95 độ trễ từ commit ở Oracle tới dashboard **< 60 giây**, đo trên 24 giờ liên tục.
2. Bơm một sự kiện cố tình sai thứ tự vào luồng thật; `drivers_current` **không** bị
   hỏng. Đây là chính assert của PoC, chạy trên hạ tầng thật thay vì trên bảng tạm.
3. Quét toàn bộ file Parquet của Bronze: **0 byte số điện thoại thô**. Cùng phép kiểm,
   cùng đối chứng.
4. Gửi một yêu cầu xoá giả lập; xác nhận CDF phát ra sự kiện delete và sổ erasure ghi
   nhận cả hai mốc thời gian.

**Vì sao chọn đúng lát này:** nó chạm cả bốn thứ khó cùng lúc — độ trễ, thứ tự sự
kiện, PII, và vòng đời xoá — trên khối lượng nhỏ nhất có thể. Nếu một trong bốn tiêu
chí trượt ở một bảng một thành phố, thiết kế sai, và ta biết điều đó sau một tuần chứ
không phải sau một quý.

**Chưa làm trong tuần đầu, có chủ ý:** `trips_fact` (khối lượng lớn hơn nhiều, nhưng
ngữ nghĩa dễ hơn — chỉ append), tầng Gold, tiering lạnh, và multi-city. Không cái nào
trong số đó thay đổi kết luận về tính khả thi.

---

## Phụ lục — Hai PoC

Cả hai chạy offline bằng đúng venv của lab, không thêm dependency, không cần Kafka.

**PoC 1 — `poc/late_cdc_merge.py`** (cơ chế):

```
[PASS] MERGE ngây thơ bị hỏng bởi sự kiện đến muộn      → status = offline (SAI)
[PASS] MERGE có guard giữ đúng trạng thái               → status = online  (ĐÚNG)
[PASS] tokenize tất định                                → cùng số → cùng token
[PASS] không byte số điện thoại thô nào trên đĩa        → 0 file Parquet
[PASS] CDF phát ra sự kiện delete                       → 1 event, mang theo token
```

**PoC 2 — `poc/reliability_proof.py`** (độ tin cậy, xem mục 5):

```
[PASS] E1 có guard: 1 trạng thái duy nhất qua 200 thứ tự ngẫu nhiên
[PASS] E1 đối chứng: không guard thì kết quả phân kỳ
[PASS] E1 vét cạn 720 hoán vị: vẫn 1 trạng thái
[PASS] E2b batch thô bị Delta từ chối (không ghi mù)
[PASS] E2 idempotent qua 5 lần phát lại
[PASS] E3 8 writer đồng thời: không mất update, không hỏng
[PASS] E4 history giữ đủ mọi sự kiện kể cả bị từ chối
```

```bash
.venv/bin/python submission/bonus/poc/late_cdc_merge.py      # → poc/output.txt
.venv/bin/python submission/bonus/poc/reliability_proof.py   # → poc/output-reliability.txt
```

**Giới hạn của PoC, nói rõ:** luồng Debezium được mô phỏng bằng list Python. Thứ được
chứng minh là **ngữ nghĩa MERGE và tokenization**, không phải throughput 30K writes/s
— cái đó cần một cụm thật và nằm ngoài phạm vi một spike.
