# REFLECTION

## Anti-pattern #4 — "VACUUM 0 HOURS để tiết kiệm storage"

Slide: retention=0 xoá file quá sớm → mất time travel, concurrent reader vỡ; fix là giữ tối thiểu 168h (7 ngày, default).

NB6 (`_lakehouse/scratch/maint_events`, delta-rs, 100,000 dòng / 200 commit nhỏ) đo trực tiếp cái giá này. `compact()` gộp 200 → 11 file (18× ít hơn), sinh 200 file cũ chờ tombstone. `VACUUM` ở retention mặc định dọn đúng 211 file tombstoned, giải phóng 16.1 MB — nhưng log cảnh báo: "Time travel to v0 is now GONE". Đúng hậu quả anti-pattern #4.

Lab còn lộ lớp slide không nói: VACUUM chỉ xoá file đã tombstone trong log. 5 file "orphan" (ghi crash, chưa từng commit) vẫn còn trên đĩa sau VACUUM. Job dọn orphan riêng (age-guard) chỉ xoá được 3/5 (21.2 KB); 2 file còn lại bị giữ vì có thể writer đang ghi dở.

Kết luận: retention=0 vừa mất time travel, vừa khiến file chưa kịp tombstone bị coi là rác sớm. Giữ ≥168h và không mặc định tin VACUUM dọn luôn orphan.
