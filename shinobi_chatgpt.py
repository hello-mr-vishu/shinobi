import requests
import os
import json
import logging
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
from typing import List, Optional, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("shinobi_monitor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Shinobi Configuration
SHINOBI_HOST = os.getenv("SHINOBI_HOST", "192.168.1.15")
SHINOBI_PORT = int(os.getenv("SHINOBI_PORT", 8080))
API_KEY = os.getenv("SHINOBI_API_KEY", "UfUL1AcqYC9OkNDpj2jeESeAkaOPRj")
GROUP_KEY = os.getenv("SHINOBI_GROUP_KEY", "bS1gixfvdu")
MONITOR_IDS = os.getenv("SHINOBI_MONITOR_IDS", "cedXRBYLBP80,u3YA9NhAI080,Fi92SLk9lL80,CWBp7pbFZO80").split(',')
BASE_URL = f"http://{SHINOBI_HOST}:{SHINOBI_PORT}/{API_KEY}"

# Google Sheets Configuration
SHEET_ID = os.getenv("SHINOBI_SHEET_ID", "16co0aMiaJjEoLZr6YYN7ujjSYx9HpVxFwTicY6E2M_c")
CREDENTIALS_FILE = os.getenv("SHINOBI_CREDENTIALS_FILE", "shinobi-sheets-credentials.json")
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Output directory
OUTPUT_DIR = os.getenv("SHINOBI_OUTPUT_DIR", "shinobi_output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

class ShinobiAPI:
    def __init__(self):
        self.session = requests.Session()
        self.timeout = 5

    def _make_request(self, endpoint: str) -> Optional[Any]:
        url = f"{BASE_URL}/{endpoint}"
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and not data.get("ok", True):
                logger.error(f"API error: {data.get('msg', 'Unknown error')} for {url}")
                return None
            return data
        except requests.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            return None
        except ValueError as e:
            logger.error(f"Invalid JSON response for {url}: {e}")
            return None

    def get_all_monitors(self) -> Optional[List[Dict[str, Any]]]:
        return self._make_request(f"monitor/{GROUP_KEY}")

class GoogleSheetsClient:
    def __init__(self):
        self.sheet = None
        try:
            if not os.path.exists(CREDENTIALS_FILE):
                raise FileNotFoundError(f"Credentials file not found: {CREDENTIALS_FILE}")
            creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPES)
            self.client = gspread.authorize(creds)
            self.sheet = self.client.open_by_key(SHEET_ID).sheet1
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets client: {e}")

    def append_row(self, row: List[Any]) -> None:
        if not self.sheet:
            logger.error("Google Sheets client not initialized, cannot append row")
            return
        try:
            self.sheet.append_row(row, value_input_option='RAW')
            logger.info("Successfully appended row to Google Sheet")
        except Exception as e:
            logger.error(f"Failed to append row to Google Sheet: {e}")

def main():
    api = ShinobiAPI()
    sheets_client = GoogleSheetsClient()

    logger.info("Fetching monitor statuses...")
    monitors_data = api.get_all_monitors()
    if not monitors_data or not isinstance(monitors_data, list):
        logger.error("Failed to fetch monitor data or invalid response")
        return

    seen_ids = set()
    monitor_statuses = []
    for monitor in monitors_data:
        monitor_id = monitor.get("mid")
        if monitor_id in MONITOR_IDS and monitor_id not in seen_ids:
            seen_ids.add(monitor_id)
            operational = monitor.get("mode") == "record" and monitor.get("status") == "Recording"
            status = {
                "id": monitor_id,
                "name": monitor.get("name", "Unknown"),
                "recording": monitor.get("mode") == "record",
                "operational": operational,
                "mode": monitor.get("mode", "Unknown"),
                "status": monitor.get("status", "Unknown")
            }
            monitor_statuses.append(status)
            if not operational:
                logger.warning(f"Monitor {monitor_id} ({status['name']}) is not operational (Mode: {status['mode']}, Status: {status['status']})")

    missing_monitors = [mid for mid in MONITOR_IDS if mid not in seen_ids]
    if missing_monitors:
        logger.warning(f"Missing monitors in API response: {missing_monitors}")

    total_cameras = len(MONITOR_IDS)
    recording_count = sum(1 for status in monitor_statuses if status["operational"])
    not_recording_count = total_cameras - recording_count
    percentage_recording = round((recording_count / total_cameras * 100), 2) if total_cameras > 0 else 0.0
    threshold_met = "Yes" if percentage_recording >= 75.0 else "No"

    now = datetime.now()
    metrics = {
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"),
        "total_cameras": total_cameras,
        "recording": recording_count,
        "not_recording": not_recording_count,
        "percentage_recording": percentage_recording,
        "threshold_met": threshold_met,
        "monitors": monitor_statuses
    }

    output_path = os.path.join(OUTPUT_DIR, f"monitor_statuses_{now.strftime('%Y%m%d_%H%M%S')}.json")
    try:
        with open(output_path, "w") as f:
            json.dump(metrics, f, indent=2)
        logger.info(f"Monitor statuses saved: {output_path}")
    except IOError as e:
        logger.error(f"Failed to save monitor statuses to file: {e}")

    sheets_row = [
        metrics["date"],
        metrics["time"],
        metrics["total_cameras"],
        metrics["recording"],
        metrics["percentage_recording"],
        metrics["threshold_met"]
    ]
    sheets_client.append_row(sheets_row)

    logger.info("Monitor Metrics:")
    logger.info(json.dumps(metrics, indent=2))

if __name__ == "__main__":
    logger.info("Starting Shinobi monitor tracker loop...")
    while True:
        try:
            start_time = time.time()
            main()
            elapsed_time = time.time() - start_time
            sleep_time = max(60.0 - elapsed_time, 0)
            logger.info(f"Sleeping for {sleep_time:.2f} seconds before next cycle...")
            time.sleep(sleep_time)
        except KeyboardInterrupt:
            logger.info("Script interrupted by user. Exiting...")
            break
        except Exception as e:
            logger.exception(f"Unexpected error occurred: {e}")
            time.sleep(60)
