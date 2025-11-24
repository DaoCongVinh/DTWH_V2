# logging_setup.py
import logging

def setup_logger():
    """Sets up the main application logger."""
    logger = logging.getLogger("crawler")
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(ch)
        
    return logger

# Create a single logger instance to be imported by other modules
logger = setup_logger()