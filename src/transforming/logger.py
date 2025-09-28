import logging
import sys


class Logger:
    def __init__(self, name: str = "price_collection"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        # Avoid duplicating handlers if they already exist
        if not self.logger.handlers:
            # Handler for console
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)

            # Message format
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            console_handler.setFormatter(formatter)

            self.logger.addHandler(console_handler)

    def info(self, message: str):
        """Log info"""
        self.logger.info(message)

    def warning(self, message: str):
        """Log warning"""
        self.logger.warning(message)

    def error(self, message: str):
        """Log error"""
        self.logger.error(message)

    def debug(self, message: str):
        """Log debug"""
        self.logger.debug(message)


logger = Logger()
