# Screenshot checklist

Đặt ảnh minh chứng vào thư mục này trước khi nộp. Nên chụp hai ảnh riêng để
chữ đủ rõ, không cắt mất đường dẫn repository.

## 1. Cây lưu trữ lakehouse

Mở PowerShell tại thư mục gốc repository, phóng to cửa sổ rồi chạy:

```powershell
tree.com /A _lakehouse | Select-Object -First 80
```

Chụp cả prompt có đường dẫn repository và phần cây hiển thị các lớp
`bronze`, `silver`, `gold`, `scratch`, `iceberg`. Lưu thành
`01-lakehouse-tree.png`.

## 2. Delta transaction log

```powershell
$log = Get-ChildItem _lakehouse\scratch\users_delta\_delta_log -Filter *.json |
  Sort-Object Name | Select-Object -First 1
$log.FullName
Get-Content -Raw $log.FullName
```

Chụp đường dẫn `_delta_log`, tên file JSON và phần JSON có các action như
`protocol`, `metaData`, `add`, `commitInfo`. Lưu thành `02-delta-log.png`.

## 3. Ảnh kết quả nên có thêm

Trong JupyterLab, mở NB2, NB6 hoặc NB7 và chụp cell kết luận có số đo cùng
dòng `NBx complete`. Các ảnh này không thay thế hai ảnh bằng chứng storage ở
trên nhưng giúp người chấm đọc kết quả nhanh hơn.
