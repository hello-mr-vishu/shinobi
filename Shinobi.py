# Import required libraries for the Shinobi Monitoring System
import os  # For file and directory operations (e.g., checking .env file, creating output directory)
import sys  # For system-specific functions (e.g., exiting on import errors)
import json  # For handling JSON data (e.g., parsing API responses, saving metrics)
import time  # For timing operations (e.g., sleep intervals, timestamps)
import signal  # For handling shutdown signals (e.g., Ctrl+C, SIGTERM)
import logging  # For logging script activity to console and file
import requests  # For making HTTP requests to Shinobi API
from datetime import datetime  # For generating timestamps in local timezone
from typing import Optional, List, Dict, Any  # For type hints to improve code clarity
from requests.adapters import HTTPAdapter  # For configuring retry behavior in HTTP requests
from urllib3.util.retry import Retry  # For retry logic on HTTP requests
import gspread  # For interacting with Google Sheets API
from gspread import Client, Worksheet  # For specific Google Sheets client and worksheet types
from oauth2client.service_account import ServiceAccountCredentials  # For Google Sheets authentication
from pydantic import BaseModel, ValidationError  # For structured configuration validation
from dotenv import load_dotenv  # For loading environment variables from .env file
import pytz  # For handling timezone conversions (e.g., Asia/Kolkata)
from logging.handlers import RotatingFileHandler  # For log rotation to manage log file size
from tenacity import retry, stop_after_attempt, wait_exponential  # For retrying failed operations with exponential backoff

# Check for required dependencies to ensure script runs correctly
try:
    import requests
    import gspread
    import oauth2client
    import pydantic
    import dotenv
    import pytz
except ImportError as e:
    print(f"Error: Missing required package: {e.name}. Install with 'pip install requests python-dotenv gspread oauth2client pydantic pytz tenacity'")
    sys.exit(1)  # Exit script if dependencies are missing

# Define configuration model using Pydantic for type safety and validation
class Config(BaseModel):
    shinobi_host: str  # Shinobi server hostname (e.g., localhost)
    shinobi_port: int  # Shinobi server port (e.g., 8080)
    api_key: str  # Shinobi API key for authentication
    group_key: str  # Shinobi group key to filter monitors
    monitor_ids: List[str]  # List of monitor IDs to track
    sheet_id: str  # Google Sheet ID for storing metrics
    credentials_file: str  # Path to Google Sheets service account credentials JSON
    scopes: List[str]  # Google API scopes for authentication
    output_dir: str  # Directory to save JSON metric files
    update_interval: float  # Interval (seconds) between metric updates
    max_retries: int  # Maximum retries for API requests
    retry_backoff_factor: float  # Backoff factor for retry delays
    timezone: str  # Timezone for timestamps (e.g., Asia/Kolkata)
    max_consecutive_failures: int  # Max consecutive API failures before exiting
    log_retention_days: int  # Days to retain JSON metric files
    apps_script_url: str  # URL for Apps Script to send server-down notifications
    notification_cooldown: int  # Cooldown (seconds) between server-down notifications

# Custom log formatter to output logs in JSON format for structured logging
class JsonFormatter(logging.Formatter):
    def __init__(self, timezone: str):
        super().__init__()
        self.tz = pytz.timezone(timezone)  # Set timezone for log timestamps

    def format(self, record):
        # Format log record as JSON with timestamp, level, message, module, and line number
        log_record = {
            "timestamp": datetime.now(self.tz).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno
        }
        return json.dumps(log_record)

# Set up logging to console (INFO and above) and file (DEBUG and above) with rotation
def setup_logging(timezone: str) -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()  # Clear any existing handlers to avoid duplicates
    
    # Console handler: Outputs INFO and higher to stdout
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(JsonFormatter(timezone))
    logger.addHandler(console_handler)
    
    # File handler: Outputs DEBUG and higher to rotating log file (5 MB, 5 backups)
    try:
        file_handler = RotatingFileHandler(
            "shinobi_monitor.log",
            maxBytes=5*1024*1024,  # 5 MB per file
            backupCount=5  # Keep 5 backup files
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(JsonFormatter(timezone))
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"Warning: Failed to set up file logging: {e}")
    
    logger.propagate = False  # Prevent logs from propagating to parent loggers
    return logger

# Load configuration from .env file and validate using Pydantic
def load_config(env_path: str = ".env") -> Config:
    logger = setup_logging("Asia/Kolkata")  # Initialize logger with default timezone
    logger.info("Loading configuration from .env")
    if not os.path.exists(env_path):
        logger.error(f".env file not found at {env_path}")
        raise FileNotFoundError(f".env file not found at {env_path}")

    load_dotenv(env_path)  # Load environment variables from .env
    
    try:
        # Validate required environment variables
        shinobi_port = os.getenv("SHINOBI_PORT")
        if shinobi_port is None:
            raise ValueError("Missing required environment variable: SHINOBI_PORT")
        
        monitor_ids = os.getenv("MONITOR_IDS")
        if monitor_ids is None:
            raise ValueError("Missing required environment variable: MONITOR_IDS")
        
        scopes = os.getenv("SCOPES")
        if scopes is None:
            raise ValueError("Missing required environment variable: SCOPES")
        
        update_interval = os.getenv("UPDATE_INTERVAL")
        if update_interval is None:
            raise ValueError("Missing required environment variable: UPDATE_INTERVAL")
        
        max_retries = os.getenv("MAX_RETRIES")
        if max_retries is None:
            raise ValueError("Missing required environment variable: MAX_RETRIES")
        
        retry_backoff_factor = os.getenv("RETRY_BACKOFF_FACTOR")
        if retry_backoff_factor is None:
            raise ValueError("Missing required environment variable: RETRY_BACKOFF_FACTOR")
        
        # Set default values for optional variables
        max_consecutive_failures = os.getenv("MAX_CONSECUTIVE_FAILURES", "5")
        log_retention_days = os.getenv("LOG_RETENTION_DAYS", "7")
        apps_script_url = os.getenv("APPS_SCRIPT_URL", "")  # Allow empty URL
        notification_cooldown = os.getenv("NOTIFICATION_COOLDOWN", "3600")  # Default 1 hour

        # Create configuration dictionary
        env_config = {
            "shinobi_host": os.getenv("SHINOBI_HOST") or "",
            "shinobi_port": int(shinobi_port),
            "api_key": os.getenv("SHINOBI_API_KEY") or "",
            "group_key": os.getenv("SHINOBI_GROUP_KEY") or "",
            "monitor_ids": json.loads(monitor_ids),
            "sheet_id": os.getenv("SHEET_ID") or "",
            "credentials_file": os.getenv("CREDENTIALS_FILE") or "",
            "scopes": json.loads(scopes),
            "output_dir": os.getenv("OUTPUT_DIR") or "",
            "update_interval": float(update_interval),
            "max_retries": int(max_retries),
            "retry_backoff_factor": float(retry_backoff_factor),
            "timezone": os.getenv("TIMEZONE", "Asia/Kolkata"),
            "max_consecutive_failures": int(max_consecutive_failures),
            "log_retention_days": int(log_retention_days),
            "apps_script_url": apps_script_url,  # URL for server-down notifications
            "notification_cooldown": int(notification_cooldown)  # Cooldown for notifications
        }
        
        # Validate that required fields are not empty (except apps_script_url)
        for key, value in env_config.items():
            if isinstance(value, str) and not value and key not in ["apps_script_url"]:
                logger.error(f"Missing required environment variable: {key.upper()}")
                raise ValueError(f"Missing required environment variable: {key.upper()}")

        # Validate timezone
        try:
            pytz.timezone(env_config["timezone"])
        except pytz.exceptions.UnknownTimeZoneError:
            logger.error(f"Invalid timezone: {env_config['timezone']}")
            raise ValueError(f"Invalid timezone: {env_config['timezone']}")

        logger.info("Configuration loaded successfully")
        return Config(**env_config)  # Return validated Config object
    except (ValidationError, ValueError, json.JSONDecodeError) as e:
        logger.error(f"Configuration error: {str(e)}")
        raise

# Trigger Google Apps Script for server-down notifications
def trigger_apps_script(config: Config, logger: logging.Logger, message: str) -> bool:
    if not config.apps_script_url:
        logger.warning("Apps Script URL not configured; skipping notification")
        return False  # Skip if no URL is provided
    
    try:
        payload = {"message": message}  # Prepare notification payload
        response = requests.post(config.apps_script_url, json=payload, timeout=10)
        response.raise_for_status()  # Raise exception for HTTP errors
        logger.info(f"Successfully triggered Apps Script: {response.text}")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to trigger Apps Script: {str(e)}")
        return False

# Class to interact with Shinobi API
class ShinobiAPI:
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.base_url = f"http://{config.shinobi_host}:{config.shinobi_port}/{config.api_key}"  # Base URL for API requests
        self.session = self._create_session()  # Initialize HTTP session with retries

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retries = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504]  # Retry on specific HTTP errors
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    # Fetch all monitors with retry logic
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
            return data  # Return list of monitor data
        except requests.RequestException as e:
            self.logger.error(f"Request error: {str(e)}")
            return None

    # Check Shinobi server health
    def health_check(self) -> str:
        endpoint = f"monitor/{self.config.group_key}"
        try:
            resp = self.session.get(f"{self.base_url}/{endpoint}", timeout=5)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and not data.get("ok"):
                self.logger.warning(f"Shinobi server responded but with error: {data.get('msg', 'Unknown error')}")
                return "INVALID_RESPONSE"
            self.logger.debug("Shinobi server health check: OK")
            return "OK"
        except requests.ConnectionError:
            self.logger.warning("Shinobi server is unreachable (connection error)")
            return "UNREACHABLE"
        except requests.Timeout:
            self.logger.warning("Shinobi server health check timed out")
            return "TIMEOUT"
        except requests.RequestException as e:
            self.logger.warning(f"Shinobi server health check failed: {str(e)}")
            return "ERROR"

# Class to interact with Google Sheets
class GoogleSheetsClient:
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.client: Optional[Client] = None
        self.sheet: Optional[Worksheet] = None
        self._initialize_client()  # Initialize client on creation

    # Initialize Google Sheets client with retry logic
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _initialize_client(self) -> None:
        try:
            if not os.path.exists(self.config.credentials_file):
                self.logger.error(f"Credentials file not found: {self.config.credentials_file}")
                raise FileNotFoundError(f"Credentials file not found: {self.config.credentials_file}")
            
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                self.config.credentials_file, self.config.scopes
            )
            self.client = gspread.authorize(creds)  # Authenticate with Google Sheets
            self.sheet = self.client.open_by_key(self.config.sheet_id).sheet1  # Open first sheet
        except Exception as e:
            self.logger.error(f"Failed to initialize Google Sheets client: {str(e)}")
            self.client = None
            self.sheet = None
            raise

    # Append a row to Google Sheet with retries
    def append_row(self, row: List[Any]) -> bool:
        if self.sheet is None:
            self.logger.error("Google Sheets client not initialized")
            return False
        for attempt in range(self.config.max_retries):
            try:
                self.sheet.append_row(row)  # Append row to sheet
                return True
            except Exception as e:
                self.logger.warning(f"Failed to append row on attempt {attempt + 1}: {str(e)}")
                time.sleep(self.config.retry_backoff_factor * (2 ** attempt))
        self.logger.error("Max retries reached for appending to Google Sheet")
        return False

# Process monitor data and calculate metrics
def process_monitors(monitors_data: Optional[List[Dict[str, Any]]], config: Config, logger: logging.Logger) -> Dict[str, Any]:
    if not monitors_data or not isinstance(monitors_data, list):
        logger.error("Invalid or no monitor data received")
        return {"monitors": [], "metrics": {}}  # Return empty data on failure

    logger.debug(f"Processing monitors: {monitors_data}")
    logger.debug(f"Configured monitor IDs: {config.monitor_ids}")

    seen_ids = set()  # Track seen monitor IDs to avoid duplicates
    monitor_statuses = []
    for monitor in monitors_data:
        monitor_id = monitor.get("mid")
        logger.debug(f"Checking monitor ID: {monitor_id}")
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
            logger.debug(f"Monitor status: {status}")
            if not operational:
                # Log non-operational monitors to file (DEBUG level)
                logger.debug(json.dumps({
                    "monitor_id": status["id"],
                    "name": status["name"],
                    "recording": status["recording"],
                    "operational": status["operational"],
                    "mode": status["mode"],
                    "status": status["status"],
                    "message": "Monitor not operational"
                }))

    # Identify missing monitors
    missing_monitors = [mid for mid in config.monitor_ids if mid not in seen_ids]
    if missing_monitors:
        logger.warning(f"Missing monitors: {missing_monitors}")

    # Calculate metrics
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

    logger.debug(f"Processed metrics: {metrics}")
    return {"monitors": monitor_statuses, "metrics": metrics, "missing_monitors": missing_monitors}

# Save metrics to JSON file and clean up old files
def save_metrics(metrics: Dict[str, Any], config: Config, logger: logging.Logger) -> str:
    try:
        os.makedirs(config.output_dir, exist_ok=True)  # Create output directory if it doesn't exist
        timestamp = datetime.now(pytz.timezone(config.timezone)).strftime("%Y%m%d_%H%M%S")
        output_path = os.path.normpath(os.path.join(config.output_dir, f"monitor_data_{timestamp}.json"))
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)  # Save metrics as JSON
        
        # Clean up JSON files older than log_retention_days
        cutoff_time = time.time() - (config.log_retention_days * 86400)
        for filename in os.listdir(config.output_dir):
            if filename.startswith("monitor_data_") and filename.endswith(".json"):
                file_path = os.path.join(config.output_dir, filename)
                if os.path.getmtime(file_path) < cutoff_time:
                    try:
                        os.remove(file_path)
                        logger.debug(f"Deleted old log file: {file_path}")
                    except Exception as e:
                        logger.warning(f"Failed to delete old log file {file_path}: {str(e)}")
        
        return output_path
    except Exception as e:
        logger.error(f"Failed to save metrics to {output_path}: {str(e)}")
        return ""

# Print metrics and monitor statuses to console
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

# Main loop to monitor Shinobi server and update Google Sheet
def main() -> None:
    print("Starting Shinobi Monitor Script...")
    try:
        config = load_config()  # Load and validate configuration
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to load configuration: {str(e)}")
        print(f"Error: Failed to load configuration: {e}")
        return

    logger = setup_logging(config.timezone)
    logger.info("Shinobi Monitor Script started")
    logger.info(f"Configuration loaded: {config.dict(exclude={'api_key', 'credentials_file'})}")  # Log config (excluding sensitive fields)

    api = ShinobiAPI(config, logger)  # Initialize Shinobi API client
    sheets_client = GoogleSheetsClient(config, logger)  # Initialize Google Sheets client
    shutdown = False
    consecutive_failures = 0
    last_notification_time = 0.0  # Track time of last server-down notification
    server_was_up = True  # Track if server was previously up

    # Handle shutdown signals (Ctrl+C, SIGTERM)
    def signal_handler(sig: int, frame: Optional[object]) -> None:
        nonlocal shutdown
        logger.info("Shutdown signal received")
        shutdown = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    while not shutdown:
        try:
            start_time = time.time()
            server_status = api.health_check()  # Check Shinobi server status
            print(f"Shinobi Server Status: {server_status}")
            logger.info(f"Shinobi Server Status: {server_status}")

            current_time = time.time()
            if server_status != "OK":
                consecutive_failures += 1
                # Send notification on first failure or after cooldown
                if server_was_up or (current_time - last_notification_time) >= config.notification_cooldown:
                    message = f"Shinobi server at {config.shinobi_host}:{config.shinobi_port} is down (Status: {server_status}). Please check the server."
                    if trigger_apps_script(config, logger, message):
                        last_notification_time = current_time
                    server_was_up = False
                logger.warning(f"Shinobi server check failed (attempt {consecutive_failures}/{config.max_consecutive_failures})")
                if consecutive_failures >= config.max_consecutive_failures:
                    error_msg = f"Shinobi server unreachable after {config.max_consecutive_failures} attempts. Exiting script."
                    logger.error(error_msg)
                    print(f"Error: {error_msg}")
                    trigger_apps_script(config, logger, error_msg)  # Notify on script exit
                    sys.exit(1)
                time.sleep(config.update_interval)
                continue

            consecutive_failures = 0
            server_was_up = True  # Reset server status

            monitors_data = api.get_all_monitors()  # Fetch monitor data
            if monitors_data is None:
                logger.error("Failed to fetch monitor data from Shinobi API")
                consecutive_failures += 1
                if consecutive_failures >= config.max_consecutive_failures:
                    error_msg = f"Shinobi server data fetch failed after {config.max_consecutive_failures} attempts. Exiting script."
                    logger.error(error_msg)
                    print(f"Error: {error_msg}")
                    trigger_apps_script(config, logger, error_msg)
                    sys.exit(1)
                time.sleep(config.update_interval)
                continue

            processed_data = process_monitors(monitors_data, config, logger)  # Process monitor data
            
            if processed_data["metrics"]:
                save_metrics(processed_data, config, logger)  # Save metrics to JSON file
                if not sheets_client.append_row([
                    processed_data["metrics"]["date"],
                    processed_data["metrics"]["time"],
                    processed_data["metrics"]["total_cameras"],
                    processed_data["metrics"]["recording"],
                    processed_data["metrics"]["percentage_recording"],
                    processed_data["metrics"]["threshold_met"]
                ]):
                    logger.error("Failed to append row to Google Sheets")
                print_metrics(processed_data)  # Print metrics to console
            
            elapsed_time = time.time() - start_time
            sleep_time = max(config.update_interval - elapsed_time, 0)  # Adjust sleep to maintain interval
            time.sleep(sleep_time)
        except requests.RequestException as e:
            logger.error(f"Network error while fetching data: {str(e)}")
            consecutive_failures += 1
            current_time = time.time()
            if server_was_up or (current_time - last_notification_time) >= config.notification_cooldown:
                message = f"Shinobi server at {config.shinobi_host}:{config.shinobi_port} is down (Network error: {str(e)}). Please check the server."
                if trigger_apps_script(config, logger, message):
                    last_notification_time = current_time
                server_was_up = False
            if consecutive_failures >= config.max_consecutive_failures:
                error_msg = f"Shinobi server unreachable after {config.max_consecutive_failures} attempts. Exiting script."
                logger.error(error_msg)
                print(f"Error: {error_msg}")
                trigger_apps_script(config, logger, error_msg)
                sys.exit(1)
            time.sleep(config.update_interval)
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {str(e)}")
            time.sleep(config.update_interval)

if __name__ == "__main__":
    main()  # Run the main monitoring loop