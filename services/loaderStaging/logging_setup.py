"""
Logging Setup Module
====================
Configure application-wide logging with file rotation and console output.
"""

import logging
import logging.handlers
from pathlib import Path
import config


def setup_logging() -> logging.Logger:
    """
    Configure application logging with file and console handlers.
    
    Returns:
        Configured root logger
    """
    # Ensure logs directory exists
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Get root logger
    root_logger = logging.getLogger()
    
    # Avoid adding handlers multiple times
    if root_logger.handlers:
        return root_logger
    
    root_logger.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))
    
    # File handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        config.LOG_FILE,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))
    
    # Format
    formatter = logging.Formatter(
        config.LOG_FORMAT,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to root logger
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return root_logger


# Auto-setup when imported
if __name__ != "__main__":
    setup_logging()
