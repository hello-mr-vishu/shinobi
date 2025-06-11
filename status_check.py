import requests
import os
import json
import logging
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Shinobi Configuration
SHINOBI_HOST = "10.10.10.10"
SHINOBI_PORT = 8080
API_KEY = os.getenv("SHINOBI_API_KEY", "UfUL1AcqYC9OkNDpj2jeESeAkaOPRj")
GROUP_KEY = os.getenv("SHINOBI_GROUP_KEY", "bS1gixfvdu")
MONITOR_IDS = ["cedXRBYLBP80", "u3YA9NhAI080", "Fi92SLk9lL80", "CWBp7pbFZO80"]
BASE_URL = f"http://{SHINOBI_HOST}:{SHINOBI_PORT}/{API_KEY}"

# Google Sheets Configuration
SHEET_ID = "16co0aMiaJjEoLZr6YYN7ujjSYx9HpVxFwTicY6E2M_c"
CREDENTIALS_FILE = "shinobi-sheets-credentials.json"
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Output directory
OUTPUT_DIR = "shinobi_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

class ShinobiAPI:
    def __init__(self):
        self.session = requests.Session()
        self.session.timeout = 10 

    def _make_request(self, endpoint):
        """Generic request handler."""
        url = f"{BASE_URL}/{endpoint}"
        try:
            resp = self.session.get(url)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and not data.get("ok"):
                logger.error(f"API error: {data.get('msg', 'Unknown error')} for {url}")
                return None
            return data
        except requests.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            return None
        except ValueError as e:
            logger.error(f"Invalid JSON response for {url}: {e}")
            return None

    def get_all_monitors(self):
        """Fetch all monitors."""
        return self._make_request(f"monitor/{GROUP_KEY}")

class GoogleSheetsClient:
    def __init__(self):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPES)
            self.client = gspread.authorize(creds)
            self.sheet = self.client.open_by_key(SHEET_ID).sheet1
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets client: {e}")
            self.sheet = None

    def append_row(self, row):
        """Append a row to the Google Sheet."""
        if not self.sheet:
            logger.error("Google Sheets client not initialized, cannot append row")
            return
        try:
            self.sheet.append_row(row)
            logger.info("Successfully appended row to Google Sheet")
        except Exception as e:
            logger.error(f"Failed to append row to Google Sheet: {e}")

def main():
    """Fetch monitor statuses, calculate metrics, and upload to Google Sheet."""
    api = ShinobiAPI()
    sheets_client = GoogleSheetsClient()

    # Fetch all monitors
    logger.info("Fetching monitor statuses...")
    monitors_data = api.get_all_monitors()
    if not monitors_data or not isinstance(monitors_data, list):
        logger.error("Failed to fetch monitor data or invalid response")
        return

    # Process monitors, ensuring no duplicates
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
                logger.warning(f"Monitor {monitor_id} ({monitor.get('name')}) is not operational (Mode: {status['mode']}, Status: {status['status']})")

    # Check for missing monitors
    missing_monitors = [mid for mid in MONITOR_IDS if mid not in seen_ids]
    if missing_monitors:
        logger.warning(f"Missing monitors in API response: {missing_monitors}")

    # Calculate metrics
    total_cameras = len(MONITOR_IDS)
    recording_count = sum(1 for status in monitor_statuses if status["operational"])
    not_recording_count = total_cameras - recording_count
    percentage_recording = (recording_count / total_cameras * 100) if total_cameras > 0 else 0
    percentage_recording = round(percentage_recording, 2)
    threshold_met = "Yes" if percentage_recording >= 75.0 else "No"

    # Prepare data for output
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

    # Save to JSON file
    output_path = os.path.join(OUTPUT_DIR, f"monitor_statuses_{now.strftime('%Y%m%d_%H%M%S')}.json")
    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=2)
    logger.info(f"Monitor statuses saved: {output_path}")

    # Append to Google Sheet
    sheets_row = [
        metrics["date"],
        metrics["time"],
        metrics["total_cameras"],
        metrics["recording"],
        metrics["percentage_recording"],
        metrics["threshold_met"]
    ]
    sheets_client.append_row(sheets_row)

    # Print metrics
    print("\nMonitor Metrics:")
    print(f"  Date: {metrics['date']}")
    print(f"  Time: {metrics['time']}")
    print(f"  Total Cameras: {metrics['total_cameras']}")
    print(f"  Recording: {metrics['recording']}")
    print(f"  Not Recording: {metrics['not_recording']}")
    print(f"  Percentage Recording: {metrics['percentage_recording']}%")
    print(f"  Threshold Met: {metrics['threshold_met']}")
    print("\nMonitor Statuses:")
    for status in monitor_statuses:
        print(f"  ID: {status['id']}  Name: {status['name']}  Recording: {status['recording']}  Operational: {status['operational']} (Mode: {status['mode']}, Status: {status['status']})")
    if missing_monitors:
        print(f"\nWarning: Missing monitors: {missing_monitors}")

if __name__ == "__main__":
    logger.info("Starting infinite loop to update Google Sheet every minute")
    while True:
        try:
            start_time = time.time()
            main()
            elapsed_time = time.time() - start_time
            sleep_time = max(60.0 - elapsed_time, 0)  # Ensure 60-second interval
            logger.info(f"Waiting {sleep_time:.2f} seconds until next update")
            time.sleep(sleep_time)
        except KeyboardInterrupt:
            logger.info("Script interrupted by user")
            break
        except Exception as e:
            logger.error(f"Unexpected error in loop: {e}")
            time.sleep(60)  # Wait 60 seconds before retrying