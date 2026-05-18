# Hardware Services — Battery Test Service

A headless **FastAPI** microservice that drives the **IT8511A+ electronic load** over serial/VISA, performs OCV/CCV battery testing, and saves results to Excel.

It runs **locally on the same machine** as the Node.js backend and is called by Node.js via HTTP. Live readings are streamed back to Node.js via SSE, which Node.js relays to the browser over WebSocket.

## Requirements

- Python 3.9+
- IT8511A+ connected via USB-Serial (or Simulation Mode for development)

## Setup

```bash
cd hardware-services
python -m venv venv

# Windows
venv\Scripts\activate
# Linux/macOS
source venv/bin/activate

pip install -r requirements.txt
```

## Run

```bash
uvicorn battery_service:app --host 127.0.0.1 --port 8765 --reload
```

The service will be available at `http://127.0.0.1:8765`.  
Interactive API docs: `http://127.0.0.1:8765/docs`

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/ports` | List available COM ports |
| POST | `/connect` | Test connection to device (or enter sim mode) |
| POST | `/disconnect` | Disconnect from device |
| POST | `/start` | Start auto-test loop |
| POST | `/stop` | Stop test loop |
| GET | `/stream` | SSE stream of live readings |
| GET | `/status` | Current session status + data |
| GET | `/report/download` | Download the generated Excel report |
| DELETE | `/session` | Clear current session data |

## Simulation Mode

Send `POST /connect` with `{ "simulation": true }` to use random data without hardware.

## Excel Reports

Reports are auto-saved to `./reports/{order_id}_{YYYY-MM}.xlsx` using the same openpyxl template logic as the original desktop app.
