import logging
import os
import sys
from datetime import datetime

class SafeStreamHandler(logging.StreamHandler):
    """A StreamHandler that handles encoding errors gracefully, especially for emojis on Windows."""
    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            try:
                stream.write(msg + self.terminator)
            except UnicodeEncodeError:
                # If encoding fails (common on Windows console with emojis), 
                # encode to ascii with backslashreplace and decode back to get printable string
                safe_msg = msg.encode(sys.stdout.encoding or 'ascii', errors='backslashreplace').decode(sys.stdout.encoding or 'ascii')
                stream.write(safe_msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)

def get_logger(name="agentic_parser"):
    """
    Creates and returns a standardized logger.
    """
    logger = logging.getLogger(name)
    
    # Only configure if not already configured
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Create console handler using safe stream handler
        console_handler = SafeStreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
        console_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(console_handler)
        
        # Create file handler (always use utf-8 for files)
        os.makedirs("logs", exist_ok=True)
        log_file = os.path.join("logs", f"parser_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
    return logger
