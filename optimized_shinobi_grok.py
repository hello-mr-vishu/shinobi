import os
import sys
import json
import time
import signal
import logging
import requests
from datetime import datetime
from typing import Optional, List, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import gspread
from gspread import Client, Worksheet
from oauth2client.service_account import ServiceAccountCredentials
from pydantic import BaseModel, ValidationError
from dotenv import load_dotenv
import pytz
from tenacity import retry, stop_after_attempt, wait_exponential

# Check dependencies
try:
    import requests
    import gspread
    import oauth2client
    import pydantic
    import dotenv
    import pytz
except ImportError as e:
    print(f"Error: Missing required package: {e.name}. Install with 'pip install requests python-dotenv gspread oauth2client pydantic pytz tenacity'")
    sys.exit(1)

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
    timezone: str

# Structured log formatter
class JsonFormatter(logging.Formatter):
    def __init__(self, timezone: str):
        super().__init__()
        self.tz = pytz.timezone(timezone)

    def format(self, record):
        log_record = {
            "timestamp": datetime.now(self.tz).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno
        }
        return json.dumps(log_record)

# Initialize logger
def setup_logging(timezone: str) -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(JsonFormatter(timezone))
    logger.addHandler(console_handler)
    
    try:
        file_handler = logging.FileHandler("shinobi_monitor.log")
        file_handler.setFormatter(JsonFormatter(timezone))
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"Warning: Failed to set up file logging: {e}")
    
    logger.propagate = False
    return logger

def load_config(env_path: str = ".env") -> Config:
    logger = setup_logging("Asia/Kolkata")
    logger.info("Loading configuration from .env")
    if not os.path.exists(env_path):
        logger.error(f".env file not found at {env_path}")
        raise FileNotFoundError(f".env file not found at {env_path}")

    load_dotenv(env_path)
    
    try:
        shinobi_port = os.getenv("SHINOBI_PORT")
        if shinobi_port is None:
            raise ValueError("Missing required environment variable: SHINOBI_PORT")
        
        monitor_ids_str = os.getenv("MONITOR_IDS")
        if monitor_ids_str is None:
            raise ValueError("Missing required environment variable: MONITOR_IDS")
        monitor_ids = [id.strip() for id in monitor_ids_str.split(",")]
        
        scopes_str = os.getenv("SCOPES")
        if scopes_str is None:
            raise ValueError("Missing required environment variable: SCOPES")
        scopes = [scope.strip() for scope in scopes_str.split(",")]
        
        update_interval = os.getenv("UPDATE_INTERVAL")
        if update_interval is None:
            raise ValueError("Missing required environment variable: UPDATE_INTERVAL")
        
        max_retries = os.getenv("MAX_RETRIES")
        if max_retries is None:
            raise ValueError("Missing required environment variable: MAX_RETRIES")
        
        retry_backoff_factor = os.getenv("RETRY_BACKOFF_FACTOR")
        if retry_backoff_factor is None:
            raise ValueError("Missing required environment variable: RETRY_BACKOFF_FACTOR")

        env_config = {
            "shinobi_host": os.getenv("SHINOBI_HOST") or "",
            "shinobi_port": int(shinobi_port),
            "api_key": os.getenv("SHINOBI_API_KEY") or "",
            "group_key": os.getenv("SHINOBI_GROUP_KEY") or "",
            "monitor_ids": monitor_ids,
            "sheet_id": os.getenv("SHEET_ID") or "",
            "credentials_file": os.getenv("CREDENTIALS_FILE") or "",
            "scopes": scopes,
            "output_dir": os.getenv("OUTPUT_DIR") or "",
            "update_interval": float(update_interval),
            "max_retries": int(max_retries),
            "retry_backoff_factor": float(retry_backoff_factor),
            "timezone": os.getenv("TIMEZONE", "Asia/Kolkata")
        }
        
        for key, value in env_config.items():
            if isinstance(value, str) and not value:
                logger.error(f"Missing required environment variable: {key.upper()}")
                raise ValueError(f"Missing required environment variable: {key.upper()}")

        try:
            pytz.timezone(env_config["timezone"])
        except pytz.exceptions.UnknownTimeZoneError:
            logger.error(f"Invalid timezone: {env_config['timezone']}")
            raise ValueError(f"Invalid timezone: {env_config['timezone']}")

        logger.info("Configuration loaded successfully")
        return Config(**env_config)
    except (ValidationError, ValueError) as e:
        logger.error(f"Configuration error: {str(e)}")
        raise

class ShinobiAPI:
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.base_url = f"http://{config.shinobi_host}:{config.shinobi_port}/{config.api_key}"
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
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

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def get_all_monitors(self) -> Optional[List[Dict[str, Any]]]:
        endpoint = f"monitor/{self.config.group_key}"
        try:
            resp = self.session.get(f"{self.base_url}/{endpoint}", timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and not data.get("ok"):
                self.logger.error(f"API error: {data.get('msg', 'Unknown error')}")
                return None
            return data
        except requests.RequestException as e:
            self.logger.error(f"Request error: {str(e)}")
            return None

class GoogleSheetsClient:
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.client: Optional[Client] = None
        self.sheet: Optional[Worksheet] = None
        self._initialize_client()

    def _initialize_client(self) -> None:
        try:
            if not os.path.exists(self.config.credentials_file):
                self.logger.error(f"Credentials file not found: {self.config.credentials_file}")
                raise FileNotFoundError(f"Credentials file not found: {self.config.credentials_file}")
            
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                self.config.credentials_file, self.config.scopes
            )
            self.client = gspread.authorize(creds)
            self.sheet = self.client.open_by_key(self.config.sheet_id).sheet1
        except Exception as e:
            self.logger.error(f"Failed to initialize Google Sheets client: {str(e)}")
            self.client = None
            self.sheet = None

    def append_row(self, row: List[Any]) -> bool:
        if self.sheet is None:
            self.logger.error("Google Sheets client not initialized")
            return False
        for attempt in range(self.config.max_retries):
            try:
                self.sheet.append_row(row)
                return True
            except Exception as e:
                self.logger.warning(f"Failed to append row on attempt {attempt + 1}: {str(e)}")
                time.sleep(self.config.retry_backoff_factor * (2 ** attempt))
        self.logger.error("Max retries reached for appending to Google Sheet")
        return False

def process_monitors(monitors_data: Optional[List[Dict[str, Any]]], config: Config, logger: logging.Logger) -> Dict[str, Any]:
    if not monitors_data or not isinstance(monitors_data, list):
        logger.error("Invalid or no monitor data received")
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
                logger.warning(json.dumps({
                    "monitor_id": status["id"],
                    "name": status["name"],
                    "recording": status["recording"],
                    "operational": status["operational"],
                    "mode": status["mode"],
                    "status": status["status"],
                    "message": "Monitor not operational"
                }))

    missing_monitors = [mid for mid in config.monitor_ids if mid not in seen_ids]
    if missing_monitors:
        logger.warning(f"Missing monitors: {missing_monitors}")

    total_cameras = len(config.monitor_ids)
    recording_count = sum(1 for status in monitor_statuses if status["operational"])
    percentage_recording = 0.0
    if total_cameras > 0:
        percentage_recording = round((recording_count / total_cameras) * 100, 2)
    
    tz = pytz.timezone(config.timezone)
    now_utc = datetime.now(pytz.utc)
    now_local = now_utc.astimezone(tz)
    metrics = {
        "date": now_local.strftime("%Y-%m-%d"),
        "time": now_local.strftime("%H:%M:%S"),
        "total_cameras": total_cameras,
        "recording": recording_count,
        "not_recording": total_cameras - recording_count,
        "percentage_recording": percentage_recording,
        "threshold_met": "Yes" if percentage_recording >= 75.0 else "No"
    }

    return {"monitors": monitor_statuses, "metrics": metrics, "missing_monitors": missing_monitors}

def save_metrics(metrics: Dict[str, Any], config: Config, logger: logging.Logger) -> str:
    try:
        os.makedirs(config.output_dir, exist_ok=True)
        timestamp = datetime.now(pytz.timezone(config.timezone)).strftime("%Y%m%d_%H%M%S")
        output_path = os.path.normpath(os.path.join(config.output_dir, f"monitor_data_{timestamp}.json"))
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)
        return output_path
    except Exception as e:
        logger.error(f"Failed to save metrics to {output_path}: {str(e)}")
        return ""

def print_metrics(data: Dict[str, Any]) -> None:
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

def health_check(api: ShinobiAPI) -> str:
    return "OK" if api.get_all_monitors() is not None else "ERROR"

def main() -> None:
    print("Starting Shinobi Monitor Script...")
    try:
        config = load_config()
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to load configuration: {str(e)}")
        print(f"Error: Failed to load configuration: {e}")
        return

    logger = setup_logging(config.timezone)
    logger.info("Shinobi Monitor Script started")
    logger.info(f"Configuration loaded: {config.dict(exclude={'api_key', 'credentials_file'})}")

    api = ShinobiAPI(config, logger)
    sheets_client = GoogleSheetsClient(config, logger)
    shutdown = False

    def signal_handler(sig: int, frame: Optional[object]) -> None:
        nonlocal shutdown
        logger.info("Shutdown signal received")
        shutdown = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    while not shutdown:
        try:
            start_time = time.time()
            monitors_data = api.get_all_monitors()
            if monitors_data is None:
                logger.error("Failed to fetch monitor data from Shinobi API")
                continue
            processed_data = process_monitors(monitors_data, config, logger)
            
            if processed_data["metrics"]:
                save_metrics(processed_data, config, logger)
                if not sheets_client.append_row([
                    processed_data["metrics"]["date"],
                    processed_data["metrics"]["time"],
                    processed_data["metrics"]["total_cameras"],
                    processed_data["metrics"]["recording"],
                    processed_data["metrics"]["percentage_recording"],
                    processed_data["metrics"]["threshold_met"]
                ]):
                    logger.error("Failed to append row to Google Sheets")
                print_metrics(processed_data)
            
            elapsed_time = time.time() - start_time
            sleep_time = max(config.update_interval - elapsed_time, 0)
            time.sleep(sleep_time)
        except requests.RequestException as e:
            logger.error(f"Network error while fetching data: {str(e)}")
            time.sleep(config.update_interval)
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {str(e)}")
            time.sleep(config.update_interval)

if __name__ == "__main__":
    main()