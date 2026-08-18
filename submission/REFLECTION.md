# REFLECTION — Day 18

**Anti-pattern team mình dễ mắc nhất: chạy expiry nhưng không quét orphan.**

Pipeline RAG/agent của team (ingest tai lieu -> embedding -> vector store) co
the crash giua chung. Khi do, mot so file da ghi xuong storage nhung chua vao
transaction log. Truoc day team chi dat retention, thay snapshot giam la nghi
du lieu da duoc don.

NB6 cho thay gia dinh do sai. `expire_snapshots` cua Iceberg co the giam so
snapshot nhung khong chac xoa file metadata/data bi mac ket. `VACUUM` cua Delta
cung khong thay orphan chua tung duoc commit, vi no chi xu ly file da xuat hien
trong log va bi tombstone. Vi vay dashboard van "xanh" nhung hoa don luu tru
khong giam.

Huong khac phuc la ghep Job 3 (expiry) va Job 4 (orphan sweep) thanh mot cap
bat buoc. Sau moi lan expiry, chay phep hieu `Disk \ Log` de liet ke file mo coi,
canh bao khi so luong > 0, va doi chieu dung luong thuc te tren storage voi tong
size trong metadata. Chenh lech keo dai se duoc xu ly nhu mot su co FinOps.
