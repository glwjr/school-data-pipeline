"""Logging configuration for the school data pipeline

Sets up centralized logging with both file and console output.
Creates timestamped log files in the logs/ directory.
"""

from datetime import datetime
import logging
import os


def setup_logging():
    """Configure logging for the pipeline"""
    # Create logs directory
    os.makedirs("logs", exist_ok=True)

    # Create log filename with timestamp
    log_filename = f"logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(),  # Also print to console
        ],
    )

    return logging.getLogger(__name__)


logger = setup_logging()
