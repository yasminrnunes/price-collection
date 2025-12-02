"""Logging utilities for the transforming module.

This module provides a Logger class that wraps Python's standard logging
module with a simplified interface. It configures console logging with
a standardized format suitable for data transformation operations.

Features:
    - Simplified logging interface with info, warning, error, and debug methods
    - Automatic handler configuration to avoid duplicates
    - Standardized message format with timestamps, logger name, level, and message
    - Console output to stdout with INFO level by default

The Logger class automatically configures handlers only once per logger name,
preventing duplicate log messages. Each logger instance is identified by a
name for better log organization.

Example:
    Create a logger for a specific component:
        >>> from logger import Logger
        >>> logger = Logger("transforming")
        >>> logger.info("Starting data transformation")
        2025-10-26 18:24:39 - transforming - INFO - Starting data transformation

    Use different log levels:
        >>> logger.debug("Debug information")
        >>> logger.warning("This is a warning")
        >>> logger.error("An error occurred")
"""

import logging
import sys


class Logger:
    """Simple logging wrapper with standardized configuration.

    This class provides a simplified interface to Python's logging module,
    automatically configuring console handlers with a standardized format.
    It prevents duplicate handlers by checking if handlers already exist
    for the given logger name.

    The logger outputs to stdout with the following format:
        {timestamp} - {logger_name} - {level} - {message}

    Attributes:
        logger: The underlying logging.Logger instance configured with
            console handler and INFO level.

    Example:
        Create a logger for a specific component:
            >>> logger = Logger("data_transformer")
            >>> logger.info("Processing product batch")
            >>> logger.error("Failed to process product")

        Use the global logger instance:
            >>> from logger import logger
            >>> logger.info("Global message")
    """

    def __init__(self, name: str = "transforming"):
        """Initialize a Logger instance.

        Args:
            name: The name identifier for this logger. This appears in log
                messages and helps organize logs by component. Common names
                include module identifiers (e.g., "transforming", "cleaning",
                "data_transformer") or component names. Defaults to "transforming".
        """
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
        """Log an informational message.

        This method logs messages at INFO level, which is the default level
        for normal operational messages. INFO messages are displayed by default.

        Args:
            message: The message string to log.

        Example:
            >>> logger.info("Successfully transformed product batch")
        """
        self.logger.info(message)

    def warning(self, message: str):
        """Log a warning message.

        This method logs messages at WARNING level for situations that may
        require attention but are not errors. Warnings are displayed by default.

        Args:
            message: The warning message string to log.

        Example:
            >>> logger.warning("Transformation took longer than expected")
        """
        self.logger.warning(message)

    def error(self, message: str):
        """Log an error message.

        This method logs messages at ERROR level for error conditions that
        may prevent normal operation. Error messages are displayed by default.

        Args:
            message: The error message string to log.

        Example:
            >>> logger.error("Failed to connect to database")
        """
        self.logger.error(message)

    def debug(self, message: str):
        """Log a debug message.

        This method logs messages at DEBUG level for detailed diagnostic
        information. Debug messages are typically used during development
        and troubleshooting. Note that the logger level is set to INFO by
        default, so debug messages will not be displayed unless the level
        is changed to DEBUG.

        Args:
            message: The debug message string to log.

        Example:
            >>> logger.debug("Processing product data: {...}")

        Note:
            To enable debug messages, set the logger level to DEBUG:
            logger.logger.setLevel(logging.DEBUG)
        """
        self.logger.debug(message)


# Global logger instance for convenience
logger = Logger()
