import os
import sys
import json
import logging
import signal
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import gspread
from google.auth.exceptions import RefreshError, TransportError
from google.oauth2.service_account import Credentials
from pydantic import BaseModel, ValidationError, field_validator
import yaml
import sentry_sdk  # For production error tracking

# Initialize Sentry for error monitoring (optional)
sentry_sdk.init(dsn=os.getenv("SENTRY_DSN"), traces_sample_rate=1.0)

# Configuration model with enhanced validation
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
    threshold_percentage: float = 75.0
    request_timeout: float = 10.0
    max_log_files: int = 100
    log_retention_days: int = 7

    @field_validator('update_interval', 'retry_backoff_factor', 'request_timeout')
    def validate_positive_floats(cls, value):
        if value <= 0:
            raise ValueError("Must be positive")
        return value

    @field_validator('max_retries')
    def validate_non_negative_ints(cls, value):
        if value < 0:
            raise ValueError("Must be non-negative")
        return value

    @field_validator('threshold_percentage')
    def validate_threshold(cls, value):
        if not 0 <= value <= 100:
            raise ValueError("Must be between 0 and 100")
        return value

# Structured log formatter
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)

# Initialize logging
def setup_logging(config: Config) -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # Stream handler for console
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(JsonFormatter())
    
    # File handler with rotation
    os.makedirs(config.output_dir, exist_ok=True)
    file_handler = logging.FileHandler(
        os.path.join(config.output_dir, "shinobi_monitor.log"),
        encoding="utf-8"
    )
    file_handler.setFormatter(JsonFormatter())
    
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    
    # Configure log retention
    setup_log_retention(config)
    
    return logger

def setup_log_retention(config: Config):
    try:
        now = datetime.now()
        for filename in os.listdir(config.output_dir):
            if filename.endswith(".log"):
                filepath = os.path.join(config.output_dir, filename)
                file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                if (now - file_time) > timedelta(days=config.log_retention_days):
                    os.remove(filepath)
    except Exception as e:
        logging.error(f"Log cleanup failed: {str(e)}")

class ShinobiAPI:
    def __init__(self, config: Config):
        self.config = config
        self.base_url = f"http://{config.shinobi_host}:{config.shinobi_port}/{config.api_key}"
        self.session = self._create_session()
        self.last_successful_fetch = None

    def _create_session(self) -> requests.Session:
        """Create a session with retry configuration."""
        session = requests.Session()
        retries = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retries, pool_maxsize=10, pool_block=True)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get_all_monitors(self) -> Tuple[Optional[List[Dict[str, Any]]], str]:
        """Fetch all monitors with comprehensive error handling."""
        endpoint = f"monitor/{self.config.group_key}"
        url = f"{self.base_url}/{endpoint}"
        status = "success"
        
        try:
            resp = self.session.get(
                url, 
                timeout=(3.05, self.config.request_timeout)
            )
            resp.raise_for_status()
            
            data = resp.json()
            if isinstance(data, dict) and not data.get("ok"):
                msg = data.get("msg", "Unknown Shinobi error")
                return None, f"API error: {msg}"
                
            if not isinstance(data, list):
                return None, "Unexpected API response format"
                
            self.last_successful_fetch = time.time()
            return data, status
        except requests.Timeout:
            status = "timeout"
            return None, "Request timed out"
        except requests.ConnectionError:
            status = "connection_error"
            return None, "Connection failed"
        except requests.HTTPError as e:
            status = f"http_error_{e.response.status_code}"
            return None, f"HTTP error: {str(e)}"
        except (json.JSONDecodeError, ValueError) as e:
            status = "parse_error"
            return None, f"Response parsing failed: {str(e)}"
        except Exception as e:
            status = "unexpected_error"
            return None, f"Unexpected error: {str(e)}"

class GoogleSheetsClient:
    def __init__(self, config: Config):
        self.config = config
        self.client = None
        self.sheet = None
        self.last_successful_auth = None
        self._initialize_client()

    def _initialize_client(self) -> bool:
        """Initialize Google Sheets client with error handling."""
        try:
            if not os.path.exists(self.config.credentials_file):
                raise FileNotFoundError("Credentials file not found")
                
            creds = Credentials.from_service_account_file(
                self.config.credentials_file,
                scopes=self.config.scopes
            )
            self.client = gspread.authorize(creds)
            self.sheet = self.client.open_by_key(self.config.sheet_id).sheet1
            self.last_successful_auth = time.time()
            return True
        except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
            logging.error(f"Configuration error: {str(e)}")
        except (RefreshError, TransportError) as e:
            logging.error(f"Authentication failure: {str(e)}")
        except Exception as e:
            logging.error(f"Client initialization failed: {str(e)}")
        return False

    def append_row(self, row: List[Any]) -> Tuple[bool, str]:
        """Append a row to Google Sheet with robust error handling."""
        if not self.sheet:
            if not self._initialize_client():
                return False, "Client not initialized"
        
        for attempt in range(self.config.max_retries + 1):
            try:
                self.sheet.append_row(row)
                return True, "Success"
            except gspread.exceptions.APIError as e:
                if e.response.status_code in (401, 403):
                    logging.warning("Reinitializing due to auth error")
                    if not self._initialize_client():
                        return False, "Reauth failed"
                else:
                    logging.warning(f"API error: {str(e)}")
            except (gspread.exceptions.GSpreadException, requests.RequestException) as e:
                logging.warning(f"Network error: {str(e)}")
            except Exception as e:
                logging.error(f"Unexpected append error: {str(e)}")
                return False, "Critical failure"
            
            if attempt < self.config.max_retries:
                sleep_time = self.config.retry_backoff_factor * (2 ** attempt)
                time.sleep(sleep_time)
        
        return False, "Max retries exceeded"

def load_config(config_path: str = "config.yaml") -> Config:
    """Load configuration with enhanced validation and fallbacks."""
    config_data = {}
    try:
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                config_data = yaml.safe_load(f) or {}
    except (yaml.YAMLError, OSError) as e:
        logging.error(f"Config load error: {str(e)}")
    
    env_config = {
        "shinobi_host": os.getenv("SHINOBI_HOST", config_data.get("shinobi_host", "localhost")),
        "shinobi_port": int(os.getenv("SHINOBI_PORT", config_data.get("shinobi_port", 8080))),
        "api_key": os.getenv("SHINOBI_API_KEY", config_data.get("api_key", "")),
        "group_key": os.getenv("SHINOBI_GROUP_KEY", config_data.get("group_key", "")),
        "monitor_ids": json.loads(os.getenv("MONITOR_IDS", json.dumps(config_data.get("monitor_ids", [])))),
        "sheet_id": os.getenv("SHEET_ID", config_data.get("sheet_id", "")),
        "credentials_file": os.getenv("CREDENTIALS_FILE", config_data.get("credentials_file", "credentials.json")),
        "scopes": json.loads(os.getenv("SCOPES", json.dumps(config_data.get("scopes", [])))),
        "output_dir": os.getenv("OUTPUT_DIR", config_data.get("output_dir", "monitor_output")),
        "update_interval": float(os.getenv("UPDATE_INTERVAL", config_data.get("update_interval", 60.0))),
        "max_retries": int(os.getenv("MAX_RETRIES", config_data.get("max_retries", 3))),
        "retry_backoff_factor": float(os.getenv("RETRY_BACKOFF_FACTOR", config_data.get("retry_backoff_factor", 1.0))),
        "threshold_percentage": float(os.getenv("THRESHOLD_PERCENTAGE", config_data.get("threshold_percentage", 75.0))),
        "request_timeout": float(os.getenv("REQUEST_TIMEOUT", config_data.get("request_timeout", 10.0))),
        "max_log_files": int(os.getenv("MAX_LOG_FILES", config_data.get("max_log_files", 100))),
        "log_retention_days": int(os.getenv("LOG_RETENTION_DAYS", config_data.get("log_retention_days", 7))),
    }
    
    try:
        return Config(**env_config)
    except ValidationError as e:
        logging.critical(f"Configuration validation failed: {str(e)}")
        sys.exit(1)

def process_monitors(monitors_data: Optional[List[Dict[str, Any]]], config: Config) -> Dict[str, Any]:
    """Process monitor data with comprehensive validation."""
    metrics = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "time": datetime.now().strftime("%H:%M:%S"),
        "total_cameras": len(config.monitor_ids),
        "recording": 0,
        "not_recording": len(config.monitor_ids),
        "percentage_recording": 0.0,
        "threshold_met": "No"
    }
    
    monitor_statuses = []
    missing_monitors = config.monitor_ids[:]
    
    if not isinstance(monitors_data, list):
        return {"monitors": monitor_statuses, "metrics": metrics, "missing_monitors": missing_monitors}
    
    for monitor in monitors_data:
        try:
            monitor_id = monitor["mid"]
            if monitor_id not in config.monitor_ids:
                continue
                
            if monitor_id in missing_monitors:
                missing_monitors.remove(monitor_id)
                
            operational = (
                monitor.get("mode") == "record" and 
                monitor.get("status") == "Recording"
            )
            status = {
                "id": monitor_id,
                "name": monitor.get("name", "Unknown"),
                "recording": monitor.get("mode") == "record",
                "operational": operational,
                "mode": monitor.get("mode", "Unknown"),
                "status": monitor.get("status", "Unknown")
            }
            monitor_statuses.append(status)
            
            if operational:
                metrics["recording"] += 1
        except KeyError:
            logging.warning("Monitor data missing required fields")
    
    metrics["not_recording"] = metrics["total_cameras"] - metrics["recording"]
    if metrics["total_cameras"] > 0:
        metrics["percentage_recording"] = round(
            (metrics["recording"] / metrics["total_cameras"] * 100), 2
        )
        metrics["threshold_met"] = "Yes" if metrics["percentage_recording"] >= config.threshold_percentage else "No"
    
    return {"monitors": monitor_statuses, "metrics": metrics, "missing_monitors": missing_monitors}

def save_metrics(metrics: Dict[str, Any], config: Config) -> str:
    """Save metrics with rotation and error handling."""
    try:
        os.makedirs(config.output_dir, exist_ok=True)
        
        # Clean old files
        files = sorted(
            [f for f in os.listdir(config.output_dir) if f.startswith("monitor_statuses")],
            key=lambda x: os.path.getmtime(os.path.join(config.output_dir, x))
        )
        while len(files) >= config.max_log_files:
            os.remove(os.path.join(config.output_dir, files.pop(0)))
        
        # Save new file
        filename = f"monitor_statuses_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        output_path = os.path.join(config.output_dir, filename)
        with open(output_path, "w") as f:
            json.dump(metrics, f, indent=2)
        return output_path
    except (OSError, IOError) as e:
        logging.error(f"File operation failed: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected save error: {str(e)}")
    return ""

def print_metrics(data: Dict[str, Any]):
    """Print metrics with formatting."""
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
        print(f"  ID: {status['id']}  Name: {status['name']}  "
              f"Recording: {status['recording']}  "
              f"Operational: {status['operational']}")
    
    if data["missing_monitors"]:
        print(f"\nWarning: Missing monitors: {data['missing_monitors']}")

def main():
    """Main processing loop with enhanced resilience."""
    try:
        config = load_config()
        logger = setup_logging(config)
        api = ShinobiAPI(config)
        sheets_client = GoogleSheetsClient(config)
        shutdown = False
        consecutive_errors = 0
        max_consecutive_errors = 5

        def signal_handler(sig, frame):
            nonlocal shutdown
            logger.info("Shutdown signal received")
            shutdown = True

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info("Service started")
        while not shutdown:
            cycle_start = time.time()
            try:
                # Fetch and process data
                monitors_data, api_status = api.get_all_monitors()
                processed_data = process_monitors(monitors_data, config)
                
                # Save metrics
                save_metrics(processed_data, config)
                
                # Update Google Sheet
                if sheets_client.sheet:
                    row = [
                        processed_data["metrics"]["date"],
                        processed_data["metrics"]["time"],
                        processed_data["metrics"]["total_cameras"],
                        processed_data["metrics"]["recording"],
                        processed_data["metrics"]["percentage_recording"],
                        processed_data["metrics"]["threshold_met"]
                    ]
                    success, msg = sheets_client.append_row(row)
                    logger.info(f"Google Sheets update: {msg}")
                
                # Print to console
                print_metrics(processed_data)
                
                consecutive_errors = 0
                
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"Cycle error: {str(e)}", exc_info=True)
                if consecutive_errors >= max_consecutive_errors:
                    logger.critical("Max consecutive errors reached")
                    shutdown = True

            # Calculate sleep time with jitter
            elapsed = time.time() - cycle_start
            sleep_time = max(config.update_interval - elapsed, 0)
            if sleep_time > 0:
                time.sleep(sleep_time + (random.random() * 0.1 * config.update_interval))
                
        logger.info("Service stopped")
    except Exception as e:
        logger.critical(f"Fatal initialization error: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()