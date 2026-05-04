**Anti-pattern dễ vướng nhất: #3 — Bỏ qua OPTIMIZE và để xảy ra vấn đề small-file.**

Các notebook trong lab (đặc biệt `02_optimize_zorder`) tái hiện rõ ràng tình huống này: khi append nhiều file nhỏ liên tục, số lượng files tăng vọt (`numFiles` rất lớn) và query trở nên cực chậm nếu không chạy Delta `OPTIMIZE + ZORDER`. Vì pipeline này xây trên Delta Lake với workload ingest/append thường xuyên, nếu team không compact files thì sẽ gặp:
- Performance query tệ hại (chậm 10× trở lên)
- Lãng phí storage (nhiều metadata, file overhead)

Khuyến cáo: **chạy daily OPTIMIZE cron job** để giữ file count và query latency ổn định.
