# Voniko-Website — Hệ Thống Quản Lý Sản Xuất Nội Bộ

> 🏭 Nền tảng web nội bộ cho nhà máy — Quản lý file PLC, kiểm tra pin, đếm pin AI, tích hợp dữ liệu DMP/DM2000. Chạy 24/7 trong mạng LAN, không cần Internet.

[![JavaScript](https://img.shields.io/badge/JavaScript-F7DF1E?logo=javascript&logoColor=black)](https://nodejs.org)
[![Node.js](https://img.shields.io/badge/Node.js-18+-339933?logo=node.js&logoColor=white)](https://nodejs.org)
[![React](https://img.shields.io/badge/React-18-61DAFB?logo=react&logoColor=black)](https://react.dev)
[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?logo=python&logoColor=white)](https://python.org)
[![License](https://img.shields.io/badge/License-Private-red)](#)

---

## 📋 Tổng Quan

**Voniko-Website** là hệ thống web nội bộ toàn diện dành cho môi trường sản xuất công nghiệp, bao gồm các chức năng:

- **Quản lý phiên bản file PLC** — Mini Git Server nội bộ, lưu lịch sử, so sánh và khôi phục file.
- **Kiểm tra pin OCV/CCV** — Tích hợp thiết bị IT8511A+ điều khiển qua giao diện web.
- **Đếm pin bằng AI** — YOLOv8/ONNX nhận diện và đếm pin trên khay trực tiếp từ ảnh chụp.
- **Tích hợp DMP/DM2000** — Đọc và hiển thị dữ liệu từ phần mềm Ee-Share DMP và cơ sở dữ liệu DM2000.

### ✨ Tính Năng Chính

| Tính năng | Mô tả |
|---|---|
| 📁 **Quản lý file & thư mục** | Cấu trúc Line → Machine, hỗ trợ mọi định dạng file PLC |
| 🔢 **Version Control** | Mỗi upload tạo version mới, lưu lịch sử đầy đủ với commit message |
| 🔍 **Diff View** | So sánh nội dung 2 phiên bản với highlight thay đổi, hỗ trợ fullscreen |
| 📄 **Office Diff** | Trích xuất và so sánh nội dung file Word, Excel, PowerPoint, CSV, RTF |
| ↩️ **Restore** | Khôi phục về bất kỳ phiên bản cũ nào, tự động tạo backup WAL |
| 🔒 **File Lock/Unlock** | Khóa file khi đang chỉnh sửa, ngăn xung đột giữa nhiều kỹ sư |
| 🔔 **Thông báo Real-time** | SSE (Server-Sent Events) hiển thị hoạt động tức thì |
| 🟢 **Trạng thái Online** | Xem ai đang online trong hệ thống theo thời gian thực |
| 💾 **Backup tự động** | Tự động backup DB theo lịch, có thể duyệt và khôi phục từ snapshot |
| 👥 **Phân quyền** | Admin / Kỹ sư (Editor) / Chỉ xem (Viewer) |
| 📊 **Dashboard & Audit Log** | Thống kê tổng quan và lịch sử toàn bộ hoạt động hệ thống |
| 🌐 **Đa ngôn ngữ** | Tiếng Việt 🇻🇳 · English 🇬🇧 · 中文 🇨🇳 |
| 📤 **Upload lớn** | Hỗ trợ file lên đến **5 GB** |
| 🔋 **Kiểm tra pin OCV/CCV** | Điều khiển máy IT8511A+ qua giao diện web, xuất báo cáo Excel |
| 🤖 **Đếm pin AI** | YOLOv8/ONNX đếm pin trên khay từ ảnh chụp, lưu lịch sử theo PO |
| 📈 **Dữ liệu DMP/DM2000** | Xem và truy xuất dữ liệu đo pin từ phần mềm Ee-Share DMP |

---

## 🏗️ Kiến Trúc Hệ Thống

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          Voniko-Website Server                           │
│                                                                          │
│  ┌───────────────────┐      ┌──────────────────────────────────────────┐ │
│  │     Frontend      │      │               Backend                    │ │
│  │  React 18 + Vite  │◀────▶│  Node.js + Express                       │ │
│  │  Ant Design 5     │      │  REST API + SSE + WebSocket (/ws)        │ │
│  │  ECharts          │      │  (Port 3001)                             │ │
│  │  (Port 3000)      │      └───────┬───────────┬──────────┬───────────┘ │
│  └───────────────────┘              │           │          │             │
│                              ┌──────▼──────┐    │          │             │
│                              │  SQLite DB  │    │          │             │
│                              │  ./data/    │    │          │             │
│                              └─────────────┘    │          │             │
│                                            ┌────▼────┐ ┌───▼──────────┐ │
│                              ┌──────────┐  │ Python  │ │   Python     │ │
│                              │  File    │  │FastAPI  │ │  FastAPI     │ │
│                              │ Storage  │  │Port 8765│ │  Port 8001   │ │
│                              │./uploads/│  │hardware-│ │count-batts-  │ │
│                              └──────────┘  │services/│ │service/      │ │
│                                            │IT8511A+ │ │YOLOv8/ONNX  │ │
│                                            └─────────┘ └──────────────┘ │
└──────────────────────────────────────────────────────────────────────────┘

Máy cài DMP software (mạng LAN):
  dmp-services/dmp_service.py  →  đăng ký về /api/dmp  (Port 8766)
```

### Stack Công Nghệ

| Layer | Technology |
|---|---|
| **Backend** | Node.js 18+ · Express 4 · better-sqlite3 |
| **Auth** | JWT (access token + refresh token) |
| **Real-time** | SSE (Server-Sent Events) · WebSocket (`ws`) |
| **Frontend** | React 18 · Vite · Ant Design 5 · ECharts |
| **Diff Engine** | diff · diff2html |
| **Office Parser** | xlsx · mammoth · pptx2json |
| **Hardware Service** | Python 3.9+ · FastAPI · pyvisa · pyserial · openpyxl |
| **AI Count Service** | Python 3.9+ · FastAPI · ONNX Runtime · OpenCV · YOLOv8 |
| **DMP Service** | Python 3.9+ · FastAPI · pyodbc (MS Access) |
| **Process Manager** | PM2 (`ecosystem.config.js`) |

---

## 📁 Cấu Trúc Project

```
Voniko-Website/
├── backend/                    # Node.js + Express
│   ├── src/
│   │   ├── config/             # Port, JWT, storage config
│   │   ├── middleware/         # Auth, error handler, SSE
│   │   ├── models/             # Database schema & init
│   │   ├── routes/             # API routes
│   │   ├── controllers/        # Business logic
│   │   └── utils/              # Logger, diff, backup, batterySocket
│   ├── .env.example
│   ├── package.json
│   └── server.js               # HTTP server + WebSocket init
│
├── frontend/                   # React 18 + Vite
│   ├── src/
│   │   ├── api/                # Axios client
│   │   ├── components/         # Layout, CommitGraph, FileDiff
│   │   ├── contexts/           # AuthContext, LangContext
│   │   ├── locales/            # vi.js · en.js · zh.js
│   │   └── pages/              # Login, Dashboard, Files, FileDetail,
│   │                           # History, Users, Profile, BackupViewer,
│   │                           # Barcode, Battery, CountBatteries, DMP
│   ├── index.html
│   ├── package.json
│   └── vite.config.js          # Proxy: /api + /ws → localhost:3001
│
├── hardware-services/          # Python - Kiểm tra pin IT8511A+
│   ├── battery_service.py      # FastAPI — SCPI, OCV/CCV, SSE, Excel
│   ├── requirements.txt
│   └── README.md
│
├── count-batteries-service/    # Python - Đếm pin bằng AI
│   ├── main.py                 # FastAPI app
│   ├── models/                 # Chứa best.onnx (model YOLOv8)
│   ├── routers/
│   │   ├── predict.py          # POST /predict — nhận ảnh, trả về số pin
│   │   └── history.py          # GET /history — lịch sử đếm
│   ├── services/
│   │   └── ai_engine.py        # ONNX Runtime + SAHI inference engine
│   ├── retrain/                # Scripts & templates cho việc retrain model
│   ├── requirements.txt
│   └── README.md
│
├── dmp-services/               # Python - Cầu nối dữ liệu DMP
│   ├── dmp_service.py          # Đọc .mdb, đăng ký về backend trung tâm
│   ├── dmp_watchdog.py
│   ├── requirements.txt
│   ├── start_dmp.bat
│   └── README.md
│
├── ecosystem.config.js         # PM2 config: chạy tất cả service cùng lúc
├── start.bat                   # Khởi động nhanh (Windows)
├── stop.bat
└── README.md
```

---

## ⚙️ Yêu Cầu Môi Trường

| Software | Version | Ghi chú |
|---|---|---|
| Node.js | 18+ | LTS recommended |
| npm | 9+ | Đi kèm Node.js |
| Python | 3.9+ | Cần cho 3 service Python |
| PM2 | Latest | `npm install -g pm2` — quản lý process |
| OS | Windows / Linux | Đã test trên Windows Server & Ubuntu |

> **DMP Service**: Chỉ chạy được trên **Windows** (cần MS Access ODBC driver).

---

## 🚀 Cài Đặt & Chạy

### 1. Clone repo

```bash
git clone https://github.com/Voniko-GPPD/Voniko-Website.git
cd Voniko-Website
```

### 2. Cài đặt Backend

```bash
cd backend
npm install
cp .env.example .env
# Chỉnh sửa .env: JWT_SECRET, BATTERY_SERVICE_URL, v.v.
```

### 3. Cài đặt Frontend

```bash
cd ../frontend
npm install
```

### 4. Cài đặt Python Services

**Hardware Service** (kiểm tra pin IT8511A+):
```bash
cd ../hardware-services
python -m venv venv
venv\Scripts\activate          # Windows
# hoặc: source venv/bin/activate   # Linux
pip install -r requirements.txt
```

**Count Batteries Service** (đếm pin AI):
```bash
cd ../count-batteries-service
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
# Đặt file model: count-batteries-service/models/best.onnx
```

**DMP Service** (chỉ máy cài phần mềm DMP — Windows):
```bash
cd ../dmp-services
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
# Xem dmp-services/README.md để cấu hình start_dmp.bat
```

### 5. Chạy toàn bộ hệ thống (PM2)

```bash
# Từ thư mục gốc
pm2 start ecosystem.config.js
pm2 save
pm2 startup   # Tự khởi động cùng Windows/Linux
```

Truy cập: `http://localhost:3000` hoặc `http://<IP-máy-chủ>:3000`

### 6. Chạy thủ công (development)

**Backend** (port 3001):
```bash
cd backend && node server.js
```

**Frontend** (port 3000):
```bash
cd frontend && npm run dev
```

**Hardware Service** (port 8765):
```bash
cd hardware-services
venv\Scripts\activate
uvicorn battery_service:app --host 127.0.0.1 --port 8765
```

**Count Batteries Service** (port 8001):
```bash
cd count-batteries-service
venv\Scripts\activate
uvicorn main:app --host 127.0.0.1 --port 8001
```

### 7. Tài khoản mặc định

| Vai trò | Username | Password |
|---|---|---|
| Admin | `admin` | `Admin@123456` |

> ⚠️ **Đổi mật khẩu ngay sau lần đăng nhập đầu tiên!**

---

## 🔋 Module Kiểm Tra Pin OCV/CCV (hardware-services)

Tích hợp trực tiếp vào giao diện web, điều khiển máy **IT8511A+** để đo OCV/CCV mà không cần phần mềm desktop riêng.

### Kiến Trúc

```
[Browser — BatteryPage.jsx]
        │  WebSocket  ws://host/ws/battery
        │  REST       /api/battery/*
        ▼
[Node.js — batterySocket.js + routes/battery.js]
        │  HTTP proxy  →  localhost:8765
        │  SSE relay   ←  localhost:8765/stream
        ▼
[Python FastAPI — hardware-services/battery_service.py]
        │  SCPI qua cổng COM/USB  →  IT8511A+
        │  hoặc Simulation Mode
        ▼
[Excel Report — hardware-services/reports/{order}_{date}.xlsx]
```

### API Endpoints (Battery Test)

| Method | Endpoint | Mô tả |
|---|---|---|
| `GET` | `/api/battery/ports` | Danh sách cổng COM khả dụng |
| `GET` | `/api/battery/status` | Trạng thái phiên kiểm tra |
| `GET` | `/api/battery/health` | Kết nối tới Python service |
| `GET` | `/api/battery/report/download` | Tải báo cáo Excel |
| `WS` | `/ws/battery` | WebSocket: live data + điều khiển |

### Sử Dụng

1. Khởi động backend + hardware service
2. Vào **🔋 Kiểm tra Pin** trong menu trái
3. Chọn cổng COM, kết nối (hoặc bật **Simulation Mode**)
4. Nhập thông số: Mã đơn hàng, Điện trở (Ω), Thời gian OCV/Load, Hệ số K
5. Nhấn **Bắt đầu** — hệ thống tự động: Chờ pin → OCV → Đặt tải → CCV → Lưu Excel
6. Nhấn **Tải báo cáo Excel** để tải file kết quả

---

## 🤖 Module Đếm Pin AI (count-batteries-service)

Nhận ảnh khay pin, chạy YOLOv8 qua ONNX Runtime để đếm số lượng pin, trả về ảnh kết quả có đánh dấu và lưu lịch sử theo số PO.

### Model AI

- Model: YOLOv8 xuất ONNX (`best.onnx`)
- Đặt tại: `count-batteries-service/models/best.onnx`
- Inference: ONNX Runtime (ưu tiên DirectML GPU > CUDA > CPU)
- Kỹ thuật: SAHI (Slicing Aided Hyper Inference) cho ảnh khay lớn

### API Endpoints (Count Batteries)

| Method | Endpoint | Mô tả |
|---|---|---|
| `POST` | `/api/count-batteries/predict` | Upload ảnh, nhận số pin + ảnh kết quả |
| `GET` | `/api/count-batteries/history` | Lịch sử đếm pin |
| `GET` | `/api/count-batteries/health` | Trạng thái service và model |
| `POST` | `/api/count-batteries/reload-model` | Tải lại model không cần restart |

### Cài Đặt Model

```
count-batteries-service/
└── models/
    └── best.onnx     ← đặt file model ở đây
```

Xem hướng dẫn retrain model đầy đủ tại [`count-batteries-service/README.md`](count-batteries-service/README.md).

### Accuracy Tuning

- Dùng thanh trượt **Confidence** trên UI để điều chỉnh ngưỡng phát hiện.
- Tối ưu điều kiện chụp: góc máy ảnh cố định top-down, ánh sáng đồng đều, không bị mờ.
- Retrain model bằng dữ liệu thực tế nếu sai số vẫn còn lớn (xem `count-batteries-service/README.md`).

---

## 📈 Module Dữ Liệu DMP (dmp-services)

Service chạy trên máy Windows cài phần mềm **Ee-Share DMP**, đọc file `.mdb` (MS Access) và đẩy dữ liệu về backend trung tâm.

> Xem cấu hình chi tiết tại [`dmp-services/README.md`](dmp-services/README.md).

---

## 🗂️ Phân Quyền

| Quyền | Admin | Editor | Viewer |
|---|:---:|:---:|:---:|
| Xem file & lịch sử | ✅ | ✅ | ✅ |
| Upload version mới | ✅ | ✅ | ❌ |
| Khóa / Mở khóa file | ✅ | ✅ | ❌ |
| Khôi phục phiên bản | ✅ | ✅ | ❌ |
| Quản lý người dùng | ✅ | ❌ | ❌ |
| Xem Audit Log | ✅ | ❌ | ❌ |
| Backup & Restore DB | ✅ | ❌ | ❌ |
| Kiểm tra Pin (OCV/CCV) | ✅ | ✅ | ✅ |
| Đếm Pin AI | ✅ | ✅ | ✅ |

---

## 🔌 API Endpoints Chính (Backend)

| Method | Endpoint | Mô tả |
|---|---|---|
| `POST` | `/api/auth/login` | Đăng nhập |
| `POST` | `/api/auth/refresh` | Gia hạn token |
| `GET` | `/api/files` | Danh sách file |
| `POST` | `/api/files` | Upload file / version mới |
| `GET` | `/api/files/:id` | Chi tiết file + lịch sử |
| `GET` | `/api/versions/diff` | So sánh 2 phiên bản |
| `POST` | `/api/versions/:id/restore` | Khôi phục phiên bản |
| `GET` | `/api/versions/:id/download` | Tải về |
| `POST` | `/api/files/:id/lock` | Khóa file |
| `POST` | `/api/files/:id/unlock` | Mở khóa file |
| `GET` | `/api/activity` | Audit log |
| `GET` | `/api/sse/events` | SSE stream real-time |
| `GET` | `/api/backups` | Danh sách backup |
| `POST` | `/api/backups/restore` | Khôi phục từ backup |
| `GET` | `/api/battery/ports` | Danh sách cổng COM |
| `GET` | `/api/battery/report/download` | Báo cáo Excel pin |
| `WS` | `/ws/battery` | WebSocket kiểm tra pin |
| `POST` | `/api/count-batteries/predict` | Đếm pin AI (proxy → port 8001) |
| `GET` | `/api/count-batteries/history` | Lịch sử đếm pin AI |

---

## 🖥️ Giao Diện

- **Dashboard** — Thống kê tổng quan: file, phiên bản, dung lượng, hoạt động gần đây
- **Quản lý File** — Duyệt file theo cấu trúc Line/Machine, tìm kiếm, lọc
- **Chi tiết File** — Timeline phiên bản dạng Git graph, so sánh diff, khóa file
- **Diff Fullscreen** — Mở rộng toàn màn hình để đọc diff dễ hơn
- **Lịch sử Hoạt động** — Audit log toàn hệ thống
- **Quản lý Người dùng** — Tạo, phân quyền, vô hiệu hoá tài khoản (Admin)
- **Backup Viewer** — Duyệt và khôi phục file từ snapshot backup
- **Hồ sơ cá nhân** — Đổi tên, avatar, mật khẩu
- **Tạo Barcode** — Tạo PDF barcode từ file CSV/Excel đơn hàng
- **🔋 Kiểm tra Pin** — Kết nối IT8511A+, đo OCV/CCV real-time, biểu đồ ECharts, báo cáo Excel
- **🤖 Đếm Pin AI** — Upload ảnh khay, xem kết quả nhận diện, lịch sử theo PO
- **📈 DMP / DM2000** — Xem dữ liệu đo pin từ phần mềm Ee-Share DMP

---

## 🌐 Đa Ngôn Ngữ

Hệ thống hỗ trợ 3 ngôn ngữ, chuyển đổi ngay lập tức không cần reload trang:

| | Tiếng Việt 🇻🇳 | English 🇬🇧 | 中文 🇨🇳 |
|---|---|---|---|
| Nút chuyển ngôn ngữ | VI | EN | 中文 |
| Lưu lựa chọn | `localStorage` | `localStorage` | `localStorage` |

---

## 🌐 Triển Khai Trong Mạng LAN

Hệ thống được thiết kế chạy trên HTTP thuần (không cần HTTPS) trong mạng nội bộ:

```bash
# Backend lắng nghe tất cả interface (đã cấu hình trong ecosystem.config.js)
HOST=0.0.0.0 node server.js

# Kỹ sư truy cập từ máy khác trong mạng
http://192.168.1.100:3000
```

- ✅ Không cần Internet
- ✅ Không cần domain hay SSL
- ✅ Hỗ trợ tên file CJK (Tiếng Trung, Tiếng Việt có dấu)
- ✅ Tương thích Windows Server & Ubuntu
- ✅ Các Python service chạy cục bộ, không cần mạng phụ

---

## 📝 Biến Môi Trường

```env
# backend/.env

# Server
PORT=3001
HOST=0.0.0.0

# JWT
JWT_SECRET=change-this-to-a-long-random-secret-key-in-production
JWT_EXPIRES_IN=8h
JWT_REFRESH_EXPIRES_IN=7d

# Storage
UPLOAD_DIR=./uploads
DATA_DIR=./data
BACKUP_DIR=./data/backups

# Admin mặc định (đổi sau lần đăng nhập đầu tiên)
ADMIN_USERNAME=admin
ADMIN_PASSWORD=Admin@123456

# Python Services
BATTERY_SERVICE_URL=http://127.0.0.1:8765
COUNT_BATTERIES_URL=http://127.0.0.1:8001
COUNT_BATTERIES_TIMEOUT_MS=120000
```

```env
# count-batteries-service (env vars)
COUNT_BATTERIES_PORT=8001
COUNT_BATTERIES_DATA_DIR=./data
COUNT_BATTERIES_STATIC_DIR=./static
COUNT_BATTERIES_MODELS_DIR=./models
```

---

## 📄 License

Dự án nội bộ — All rights reserved © 2026 Voniko-GPPD

