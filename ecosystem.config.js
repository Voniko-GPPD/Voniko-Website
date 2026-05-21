module.exports = {
  apps: [
    {
      name: 'voniko-backend',
      script: 'server.js',
      cwd: './backend',
      env: {
        NODE_ENV: 'production',
        PORT: 3001,
        // Bind to all interfaces so the backend is reachable from other machines
        // on the LAN (same as the frontend which uses --host 0.0.0.0).
        // Ensure the host machine's firewall restricts port 3001 to trusted
        // networks only — the application requires a valid JWT for every API
        // call, but defence-in-depth at the network level is recommended.
        // On Windows, if port 3001 falls in a Hyper-V/WSL/Docker reserved range
        // you may get EACCES; in that case either change PORT or set HOST=127.0.0.1
        // in backend/.env to restrict to localhost only.
        HOST: '0.0.0.0',
      },
      instances: 1,
      exec_mode: 'fork',
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      error_file: '../logs/backend-error.log',
      out_file: '../logs/backend-out.log',
      merge_logs: true,
    },
    {
      name: 'voniko-frontend',
      script: 'npx',
      args: 'vite --host 0.0.0.0 --port 3000',
      cwd: './frontend',
      interpreter: 'none',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '512M',
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      error_file: '../logs/frontend-error.log',
      out_file: '../logs/frontend-out.log',
      merge_logs: true,
    },
    {
      name: 'voniko-hardware',
      script: 'venv/Scripts/pythonw.exe',
      args: '-m uvicorn battery_service:app --host 127.0.0.1 --port 8765',
      cwd: './hardware-services',
      interpreter: 'none',
      env: {
        PYTHONUNBUFFERED: '1',
      },
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '512M',
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      error_file: '../logs/hardware-error.log',
      out_file: '../logs/hardware-out.log',
      merge_logs: true,
    },
    {
      name: 'voniko-count-batteries',
      script: 'venv/Scripts/pythonw.exe',
      args: '-m uvicorn main:app --host 127.0.0.1 --port 8001',
      cwd: './count-batteries-service',
      interpreter: 'none',
      env: {
        PYTHONUNBUFFERED: '1',
      },
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      error_file: '../logs/count-batteries-error.log',
      out_file: '../logs/count-batteries-out.log',
      merge_logs: true,
    },
    {
      name: 'qc-system',
      script: 'venv\\Scripts\\pythonw.exe',
      args: '-m uvicorn app.main:app --host 127.0.0.1 --port 8002',
      cwd: './qc-system',
      interpreter: 'none',
      env: {
        QC_PORT: '8002',
      },
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '200M',
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      error_file: '../logs/qc-system-error.log',
      out_file: '../logs/qc-system-out.log',
      merge_logs: true,
    },
  ],
};
