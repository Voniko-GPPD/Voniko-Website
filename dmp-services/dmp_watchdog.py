"""
dmp_watchdog.py — Watchdog for dmp-service.

Periodically polls the dmp-service health endpoint.  If the service does not
respond within the configured timeout, the watchdog issues a PM2 restart so
that hung (non-crashing) processes are automatically recovered.

Environment variables (all optional):
  WATCHDOG_PORT            Port the dmp-service listens on (default: 8766)
  WATCHDOG_INTERVAL        Seconds between health checks           (default: 60)
  WATCHDOG_TIMEOUT         HTTP request timeout in seconds         (default: 10)
  WATCHDOG_FAIL_THRESHOLD  Consecutive failures before restart     (default: 3)
  WATCHDOG_SERVICE_NAME    PM2 process name to restart             (default: dmp-service)
"""

import logging
import os
import subprocess
import sys
import time

import requests
from requests.exceptions import ConnectionError as _ConnError
from requests.exceptions import RequestException, Timeout

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [watchdog] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

PORT: int = int(os.environ.get("WATCHDOG_PORT", os.environ.get("DMP_STATION_PORT", "8766")))
INTERVAL: int = int(os.environ.get("WATCHDOG_INTERVAL", "60"))
TIMEOUT: float = float(os.environ.get("WATCHDOG_TIMEOUT", "10"))
FAIL_THRESHOLD: int = int(os.environ.get("WATCHDOG_FAIL_THRESHOLD", "3"))
SERVICE_NAME: str = os.environ.get("WATCHDOG_SERVICE_NAME", "dmp-service")

# How many multiples of INTERVAL to wait after triggering a restart before
# resuming normal health checks.  Allows the service time to come back up.
POST_RESTART_WAIT_MULTIPLIER: int = 2

HEALTH_URL = f"http://127.0.0.1:{PORT}/"


def _restart_service() -> None:
    logger.warning("Restarting PM2 service: %s", SERVICE_NAME)
    try:
        result = subprocess.run(
            ["pm2", "restart", SERVICE_NAME],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            logger.info("PM2 restart succeeded: %s", result.stdout.strip())
        else:
            logger.error("PM2 restart failed (rc=%d): %s", result.returncode, result.stderr.strip())
    except FileNotFoundError:
        logger.error("pm2 command not found — service cannot be restarted automatically")
    except subprocess.TimeoutExpired:
        logger.error("pm2 restart timed out")
    except Exception as exc:  # noqa: BLE001
        logger.error("Unexpected error during pm2 restart: %s", exc)


def main() -> None:
    logger.info(
        "Watchdog started — service=%s url=%s interval=%ds timeout=%ds threshold=%d",
        SERVICE_NAME,
        HEALTH_URL,
        INTERVAL,
        int(TIMEOUT),
        FAIL_THRESHOLD,
    )

    # Wait for the dmp-service to finish its initial startup before monitoring.
    time.sleep(INTERVAL)

    consecutive_failures = 0

    while True:
        try:
            resp = requests.get(HEALTH_URL, timeout=TIMEOUT)
            if resp.status_code == 200:
                if consecutive_failures > 0:
                    logger.info("Service recovered after %d failure(s)", consecutive_failures)
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                logger.warning(
                    "Health check returned HTTP %d (failure %d/%d)",
                    resp.status_code,
                    consecutive_failures,
                    FAIL_THRESHOLD,
                )
        except Timeout as exc:
            consecutive_failures += 1
            logger.warning(
                "Health check timed out (%s) — failure %d/%d",
                exc,
                consecutive_failures,
                FAIL_THRESHOLD,
            )
        except _ConnError as exc:
            consecutive_failures += 1
            logger.warning(
                "Health check connection error (%s) — failure %d/%d",
                exc,
                consecutive_failures,
                FAIL_THRESHOLD,
            )
        except RequestException as exc:
            consecutive_failures += 1
            logger.warning(
                "Health check request error (%s) — failure %d/%d",
                exc,
                consecutive_failures,
                FAIL_THRESHOLD,
            )
        except Exception as exc:  # noqa: BLE001
            consecutive_failures += 1
            logger.warning(
                "Health check unexpected error (%s) — failure %d/%d",
                exc,
                consecutive_failures,
                FAIL_THRESHOLD,
            )

        if consecutive_failures >= FAIL_THRESHOLD:
            _restart_service()
            consecutive_failures = 0
            # Wait longer after a restart to give the service time to come back up.
            time.sleep(INTERVAL * POST_RESTART_WAIT_MULTIPLIER)
        else:
            time.sleep(INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Watchdog stopped")
        sys.exit(0)
