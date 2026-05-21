# QC System (BQMS) - Battery Quality Management Module

Module quan ly chat luong PIN, tich hop vao Voniko-Website.

## Cong mac dinh

- Service: `127.0.0.1:8002`

## Du lieu

- Database: `../backend/data/bqms.db`
- Upload anh: `../backend/data/qc-uploads/`

## Seed du lieu he thong

- Bo dictionary mac dinh duoc luu trong source code tai `app/default_dictionaries.py`
- Khi cai dat moi va khoi tao `bqms.db` lan dau, he thong se auto seed du lieu nay vao database
- Seed chi chay 1 lan cho moi database thong qua marker `default_dictionary_seed_v1`
- Sau khi cai dat, nguoi dung van co the sua/xoa du lieu trong app ma he thong khong tu dong chen lai o moi lan khoi dong

## Cai dat

```bat
cd qc-system
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

## Chay thu cong

```bat
venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8002
```

## API Docs

- `http://127.0.0.1:8002/docs`

## Truy cap qua Voniko-Website

- Tat ca API duoc proxy qua `http://localhost:3001/api/qc/*`
