import os
import sys
import json
import time
import signal
import logging
import requests
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple, NoReturn

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import gspread
from gspread import Client, Worksheet
from oauth2client.service_account import ServiceAccountCredentials
from pydantic import BaseModel, ValidationError
from dotenv import load_dotenv
import pytz

# Constants
DEFAULT_ENV_PATH = ".env"
CONFIG_REQUIRED_KEYS = [
    "SHINOBI_HOST", "SHINOBI_PORT", "SHINOBI_API_KEY", "SHINOBI_GROUP_KEY",
    "MONITOR_IDS", "SHEET_ID", "CREDENTIALS_FILE", "SCOPES", "OUTPUT_DIR",
    "UPDATE_INTERVAL", "MAX_RETRIES", "RETRY_BACKOFF_FACTOR", "TIMEZONE"
]
MIN_UPDATE_INTERVAL = 10.0
MAX_RETRIES = 5
RETRY_BACKOFF_BASE = 1.5

# Check dependencies
try:
    import requests
    import gspread
    import oauth2client
    import pydantic
    import dotenv
    import pytz
except ImportError as e:
    print(f"Error: Missing required package: {e.name}. Install with 'pip install "
          "requests python-dotenv gspread oauth2client pydantic pytz'")
    sys.exit(1)

# Configuration model
class AppConfig(BaseModel):
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

    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": datetime.now(self.tz).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)

# Initialize logger
def setup_logging(timezone: str) -> logging.Logger:
    logger = logging.getLogger("shinobi_monitor")
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers to prevent duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(JsonFormatter(timezone))
    logger.addHandler(console_handler)
    
    try:
        file_handler = logging.FileHandler("shinobi_monitor.log", encoding="utf-8")
        file_handler.setFormatter(JsonFormatter(timezone))
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"Failed to set up file logging: {e}")
    
    logger.propagate = False
    return logger

def validate_env_vars(env_vars: Dict[str, str]) -> Tuple[bool, List[str]]:
    missing = [key for key in CONFIG_REQUIRED_KEYS if key not in env_vars or not env_vars[key]]
    return (len(missing) == 0, missing

def load_config(env_path: str = DEFAULT_ENV_PATH) -> AppConfig:
    if not os.path.exists(env_path):
        raise FileNotFoundError(f".env file not found at {env_path}")

    load_dotenv(env_path)
    env_vars = {key: os.getenv(key) for key in CONFIG_REQUIRED_KEYS}
    valid, missing = validate_env_vars(env_vars)
    if not valid:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    try:
        # Parse and validate values
        config = AppConfig(
            shinobi_host=env_vars["SHINOBI_HOST"],
            shinobi_port=int(env_vars["SHINOBI_PORT"]),
            api_key=env_vars["SHINOBI_API_KEY"],
            group_key=env_vars["SHINOBI_GROUP_KEY"],
            monitor_ids=json.loads(env_vars["MONITOR_IDS"]),
            sheet_id=env_vars["SHEET_ID"],
            credentials_file=env_vars["CREDENTIALS_FILE"],
            scopes=json.loads(env_vars["SCOPES"]),
            output_dir=env_vars["OUTPUT_DIR"],
            update_interval=max(float(env_vars["UPDATE_INTERVAL"]), MIN_UPDATE_INTERVAL),
            max_retries=min(int(env_vars["MAX_RETRIES"]), MAX_RETRIES),
            retry_backoff_factor=float(env_vars["RETRY_BACKOFF_FACTOR"]),
            timezone=env_vars["TIMEZONE"]
        )
        
        # Validate timezone
        pytz.timezone(config.timezone)
        return config
    except (json.JSONDecodeError, pytz.UnknownTimeZoneError, ValueError) as e:
        raise ValueError(f"Configuration error: {str(e)}") from e

class ShinobiAPI:
    def __init__(self, config: AppConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.base_url = f"http://{config.shinobi_host}:{config.shinobi_port}/{config.api_key}"
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get_all_monitors(self) -> Optional[List[Dict[str, Any]]:
        endpoint = f"monitor/{self.config.group_key}"
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, dict) and not data.get("ok", True):
                self.logger.error(f"API error: {data.get('msg', 'Unknown error')}")
                return None
            return data
        except requests.RequestException as e:
            self.logger.error(f"Request failed: {str(e)}")
        except json.JSONDecodeError:
            self.logger.error("Failed to parse API response")
        return None

    def close(self) -> None:
        self.session.close()

class GoogleSheetsClient:
    def __init__(self, config: AppConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.client: Optional[Client] = None
        self.sheet: Optional[Worksheet] = None
        self._initialize_client()

    def _initialize_client(self) -> None:
        try:
            if not os.path.exists(self.config.credentials_file):
                raise FileNotFoundError("Credentials file not found")
            
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                self.config.credentials_file, self.config.scopes
            )
            self.client = gspread.authorize(creds)
            self.sheet = self.client.open_by_key(self.config.sheet_id).sheet1
        except Exception as e:
            self.logger.error(f"Google Sheets init failed: {str(e)}")
            self.client = None
            self.sheet = None

    def append_row(self, row: List[Any]) -> bool:
        if not self.sheet:
            self.logger.warning("Reinitializing Google Sheets client")
            self._initialize_client()
            if not self.sheet:
                return False

        try:
            self.sheet.append_row(row)
            return True
        except gspread.exceptions.APIError as e:
            self.logger.error(f"Google Sheets API error: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected Sheets error: {str(e)}")
        return False

    def close(self) -> None:
        if self.client:
            try:
                self.client.session.close()
            except Exception:
                pass

def process_monitors(
    monitors_data: Optional[List[Dict[str, Any]]], 
    config: AppConfig, 
    logger: logging.Logger
) -> Dict[str, Any]:
    if not monitors_data:
        logger.error("No monitor data received")
        return {"monitors": [], "metrics": {}}

    monitor_statuses = []
    seen_ids = set()
    for monitor in monitors_data:
        try:
            monitor_id = monitor["mid"]
            if monitor_id not in config.monitor_ids or monitor_id in seen_ids:
                continue

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
                logger.warning(
                    f"Monitor {monitor_id} not operational: "
                    f"Mode={status['mode']}, Status={status['status']}"
                )
        except KeyError as e:
            logger.error(f"Invalid monitor data: missing key {str(e)}")

    missing_monitors = [mid for mid in config.monitor_ids if mid not in seen_ids]
    if missing_monitors:
        logger.warning(f"Missing monitor data for IDs: {', '.join(missing_monitors)}")

    total_cameras = len(config.monitor_ids)
    recording_count = sum(1 for s in monitor_statuses if s["operational"])
    percentage_recording = round((recording_count / total_cameras) * 100, 2) if total_cameras else 0.0
    
    tz = pytz.timezone(config.timezone)
    now = datetime.now(tz)
    metrics = {
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"),
        "total_cameras": total_cameras,
        "recording": recording_count,
        "not_recording": total_cameras - recording_count,
        "percentage_recording": percentage_recording,
        "threshold_met": percentage_recording >= 75.0
    }

    return {
        "monitors": monitor_statuses,
        "metrics": metrics,
        "missing_monitors": missing_monitors
    }

def save_metrics(metrics: Dict[str, Any], config: AppConfig, logger: logging.Logger) -> bool:
    try:
        os.makedirs(config.output_dir, exist_ok=True)
        filename = f"monitor_statuses_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = os.path.join(config.output_dir, filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"Failed to save metrics: {str(e)}")
        return False

def handle_shutdown(signum: int, frame: Any, shutdown_flag: List[bool]) -> None:
    shutdown_flag[0] = True
    logger = logging.getLogger("shinobi_monitor")
    logger.info("Shutdown signal received, exiting gracefully")

def main() -> NoReturn:
    print("🚀 Starting Shinobi Monitor")
    
    try:
        config = load_config()
    except Exception as e:
        print(f"❌ Configuration error: {str(e)}")
        sys.exit(1)

    logger = setup_logging(config.timezone)
    logger.info("Application started")
    
    shutdown_flag = [False]
    signal.signal(signal.SIGINT, lambda s, f: handle_shutdown(s, f, shutdown_flag))
    signal.signal(signal.SIGTERM, lambda s, f: handle_shutdown(s, f, shutdown_flag))

    api = ShinobiAPI(config, logger)
    sheets_client = GoogleSheetsClient(config, logger)
    
    while not shutdown_flag[0]:
        iteration_start = time.monotonic()
        
        try:
            # Data collection and processing
            monitors_data = api.get_all_monitors()
            processed_data = process_monitors(monitors_data, config, logger)
            
            # Data persistence
            if processed_data["metrics"]:
                save_success = save_metrics(processed_data, config, logger)
                if save_success:
                    logger.info("Metrics saved successfully")
                
                # Google Sheets update
                metrics = processed_data["metrics"]
                sheets_client.append_row([
                    metrics["date"],
                    metrics["time"],
                    metrics["total_cameras"],
                    metrics["recording"],
                    metrics["percentage_recording"],
                    "Yes" if metrics["threshold_met"] else "No"
                ])
        except Exception as e:
            logger.error(f"Unhandled exception in main loop: {str(e)}")
        
        # Calculate sleep time with overflow protection
        elapsed = time.monotonic() - iteration_start
        sleep_duration = max(config.update_interval - elapsed, 0)
        
        if sleep_duration > 0:
            logger.debug(f"Sleeping for {sleep_duration:.1f} seconds")
            time.sleep(sleep_duration)
    
    # Cleanup resources
    api.close()
    sheets_client.close()
    logger.info("Application shutdown complete")
    sys.exit(0)

if __name__ == "__main__":
    main()