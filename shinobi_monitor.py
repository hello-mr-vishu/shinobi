import os
import json
import logging
import signal
from typing import Optional, List, Dict, Any
from datetime import datetime
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pydantic import BaseModel, ValidationError
import yaml

# Configuration model
class Config(BaseModel):
    shinobi_host: str
    shinobi_port: int
    api_key: str
    group_key: str
    monitor_ids: List[str]
    sheet_id: str
    credentials_file: str
    scopes: List[str]
    output_dir: str
    update_interval: float
    max_retries: int
    retry_backoff_factor: float

# Structured log formatter
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno
        }
        return json.dumps(log_record)

# Initialize logging
def setup_logging():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    return logger

logger = setup_logging()

class ShinobiAPI:
    def __init__(self, config: Config):
        self.config = config
        self.base_url = f"http://{config.shinobi_host}:{config.shinobi_port}/{config.api_key}"
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a session with retry configuration."""
        session = requests.Session()
        retries = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get_all_monitors(self) -> Optional[List[Dict[str, Any]]]:
        """Fetch all monitors with retry handling."""
        endpoint = f"monitor/{self.config.group_key}"
        try:
            resp = self.session.get(f"{self.base_url}/{endpoint}", timeout=5)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and not data.get("ok"):
                logger.error({"endpoint": endpoint, "error": data.get("msg", "Unknown error")})
                return None
            return data
        except requests.RequestException as e:
            logger.error({"endpoint": endpoint, "error": str(e)})
            return None

class GoogleSheetsClient:
    def __init__(self, config: Config):
        self.config = config
        self.client = None
        self.sheet = None
        self._initialize_client()

    def _initialize_client(self):
        """Initialize Google Sheets client with connection pooling."""
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                self.config.credentials_file, self.config.scopes
            )
            self.client = gspread.authorize(creds)
            self.sheet = self.client.open_by_key(self.config.sheet_id).sheet1
            logger.info({"message": "Google Sheets client initialized successfully"})
        except Exception as e:
            logger.error({"error": f"Failed to initialize Google Sheets client: {str(e)}"})
            self.sheet = None

    def append_row(self, row: List[Any]) -> bool:
        """Append a row to the Google Sheet with retry logic."""
        if not self.sheet:
            logger.error({"error": "Google Sheets client not initialized"})
            return False
        for attempt in range(self.config.max_retries):
            try:
                self.sheet.append_row(row)
                logger.info({"message": "Successfully appended row to Google Sheet", "row": row})
                return True
            except Exception as e:
                logger.warning({"attempt": attempt + 1, "error": f"Failed to append row: {str(e)}"})
                time.sleep(self.config.retry_backoff_factor * (2 ** attempt))
        logger.error({"error": "Max retries reached for appending to Google Sheet"})
        return False

def load_config(config_path: str = "config.yaml") -> Config:
    """Load configuration from YAML file or environment variables."""
    try:
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                config_data = yaml.safe_load(f)
        else:
            config_data = {}
        
        env_config = {
            "shinobi_host": os.getenv("SHINOBI_HOST", config_data.get("shinobi_host", "192.168.1.15")),
            "shinobi_port": int(os.getenv("SHINOBI_PORT", config_data.get("shinobi_port", 8080))),
            "api_key": os.getenv("SHINOBI_API_KEY", config_data.get("api_key", "UfUL1AcqYC9OkNDpj2jeESeAkaOPRj")),
            "group_key": os.getenv("SHINOBI_GROUP_KEY", config_data.get("group_key", "bS1gixfvdu")),
            "monitor_ids": json.loads(os.getenv("MONITOR_IDS", json.dumps(config_data.get("monitor_ids", ["cedXRBYLBP80", "u3YA9NhAI080", "Fi92SLk9lL80", "CWBp7pbFZO80"])))),
            "sheet_id": os.getenv("SHEET_ID", config_data.get("sheet_id", "16co0aMiaJjEoLZr6YYN7ujjSYx9HpVxFwTicY6E2M_c")),
            "credentials_file": os.getenv("CREDENTIALS_FILE", config_data.get("credentials_file", "shinobi-sheets-credentials.json")),
            "scopes": json.loads(os.getenv("SCOPES", json.dumps(config_data.get("scopes", ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])))),
            "output_dir": os.getenv("OUTPUT_DIR", config_data.get("output_dir", "shinobi_output")),
            "update_interval": float(os.getenv("UPDATE_INTERVAL", config_data.get("update_interval", 60.0))),
            "max_retries": int(os.getenv("MAX_RETRIES", config_data.get("max_retries", 3))),
            "retry_backoff_factor": float(os.getenv("RETRY_BACKOFF_FACTOR", config_data.get("retry_backoff_factor", 1.0)))
        }
        return Config(**env_config)
    except (ValidationError, yaml.YAMLError, ValueError) as e:
        logger.error({"error": f"Configuration error: {str(e)}"})
        raise

def process_monitors(monitors_data: Optional[List[Dict[str, Any]]], config: Config) -> Dict[str, Any]:
    """Process monitor data and calculate metrics."""
    if not monitors_data or not isinstance(monitors_data, list):
        logger.error({"error": "Invalid or no monitor data received"})
        return {"monitors": [], "metrics": {}}

    seen_ids = set()
    monitor_statuses = []
    for monitor in monitors_data:
        monitor_id = monitor.get("mid")
        if monitor_id in config.monitor_ids and monitor_id not in seen_ids:
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
                logger.warning({
                    "monitor_id": monitor_id,
                    "name": status["name"],
                    "mode": status["mode"],
                    "status": status["status"],
                    "message": "Monitor not operational"
                })

    missing_monitors = [mid for mid in config.monitor_ids if mid not in seen_ids]
    if missing_monitors:
        logger.warning({"missing_monitors": missing_monitors})

    total_cameras = len(config.monitor_ids)
    recording_count = sum(1 for status in monitor_statuses if status["operational"])
    metrics = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "time": datetime.now().strftime("%H:%M:%S"),
        "total_cameras": total_cameras,
        "recording": recording_count,
        "not_recording": total_cameras - recording_count,
        "percentage_recording": round((recording_count / total_cameras * 100) if total_cameras > 0 else 0, 2),
        "threshold_met": "Yes" if (recording_count / total_cameras * 100) >= 75.0 else "No"
    }

    return {"monitors": monitor_statuses, "metrics": metrics, "missing_monitors": missing_monitors}

def save_metrics(metrics: Dict[str, Any], config: Config) -> str:
    """Save metrics to JSON file."""
    os.makedirs(config.output_dir, exist_ok=True)
    output_path = os.path.join(config.output_dir, f"monitor_statuses_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    try:
        with open(output_path, "w") as f:
            json.dump(metrics, f, indent=2)
        logger.info({"output_path": output_path, "message": "Metrics saved successfully"})
        return output_path
    except Exception as e:
        logger.error({"output_path": output_path, "error": f"Failed to save metrics: {str(e)}"})
        return ""

def print_metrics(data: Dict[str, Any]):
    """Print metrics and monitor statuses."""
    metrics = data["metrics"]
    print("\nMonitor Metrics:")
    print(f"  Date: {metrics['date']}")
    print(f"  Time: {metrics['time']}")
    print(f"  Total Cameras: {metrics['total_cameras']}")
    print(f"  Recording: {metrics['recording']}")
    print(f"  Not Recording: {metrics['not_recording']}")
    print(f"  Percentage Recording: {metrics['percentage_recording']}%")
    print(f"  Threshold Met: {metrics['threshold_met']}")
    print("\nMonitor Statuses:")
    for status in data["monitors"]:
        print(f"  ID: {status['id']}  Name: {status['name']}  Recording: {status['recording']}  Operational: {status['operational']} (Mode: {status['mode']}, Status: {status['status']})")
    if data["missing_monitors"]:
        print(f"\nWarning: Missing monitors: {data['missing_monitors']}")

def main():
    """Main processing loop."""
    config = load_config()
    api = ShinobiAPI(config)
    sheets_client = GoogleSheetsClient(config)
    shutdown = False

    def signal_handler(sig, frame):
        nonlocal shutdown
        logger.info({"message": "Shutdown signal received"})
        shutdown = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    while not shutdown:
        try:
            start_time = time.time()
            monitors_data = api.get_all_monitors()
            processed_data = process_monitors(monitors_data, config)
            
            if processed_data["metrics"]:
                save_metrics(processed_data, config)
                sheets_client.append_row([
                    processed_data["metrics"]["date"],
                    processed_data["metrics"]["time"],
                    processed_data["metrics"]["total_cameras"],
                    processed_data["metrics"]["recording"],
                    processed_data["metrics"]["percentage_recording"],
                    processed_data["metrics"]["threshold_met"]
                ])
                print_metrics(processed_data)
            
            elapsed_time = time.time() - start_time
            sleep_time = max(config.update_interval - elapsed_time, 0)
            logger.info({"sleep_time": sleep_time, "message": "Waiting for next update"})
            time.sleep(sleep_time)
        except Exception as e:
            logger.error({"error": f"Unexpected error in main loop: {str(e)}"})
            time.sleep(config.update_interval)

if __name__ == "__main__":
    main()