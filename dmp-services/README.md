# DMP Battery Data Bridge — Client Service

## Tổng quan
`dmp_service.py` chạy trên máy Windows cài phần mềm **Ee-Share DMP**. Service đọc file `.mdb` local (qua pyodbc + shadow copy), tự đăng ký về server Voniko trung tâm, và cho phép truy cập dữ liệu qua web interface.

> **Lưu ý:** Đây là service RIÊNG BIỆT với `hardware-services/battery_service.py`.  
> Không chạy `start_hardware.bat` trên máy DMP. Dùng `start_dmp.bat`.

## Yêu cầu
- Windows (bắt buộc — cần MS Access ODBC driver)
- Python 3.9+
- Microsoft Access Database Engine 2016 Redistributable
  Tải: https://www.microsoft.com/en-us/download/details.aspx?id=54920

## Cài đặt
```bash
cd dmp-services
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

## Cấu hình & Chạy
Chỉnh `start_dmp.bat` (2 dòng đầu):

| Biến | Mô tả | Ví dụ |
|------|-------|-------|
| `DMP_STATION_NAME` | Tên trạm hiển thị | `DMP - Day chuyen A` |
| `VONIKO_SERVER_URL` | URL server trung tâm | `http://10.4.1.11:3001` |
| `DMP_DATA_DIR` | Đường dẫn thư mục chứa DMPDATA.mdb | `C:\DMP\Data` |
| `DMP_STATION_PORT` | Port service lắng nghe | `8766` |

## So sánh với Battery Service

| | `hardware-services/` | `dmp-services/` |
|---|---|---|
| Chạy trên | Máy trạm đo pin (IT8511A+) | Máy cài DMP software |
| Khởi động bằng | `start_hardware.bat` | `start_dmp.bat` |
| Đọc | Hardware serial/VISA | File .mdb (Access DB) |
| Đăng ký về | `/api/battery/register` | `/api/dmp/register` |
| Port mặc định | 8765 | 8766 |
