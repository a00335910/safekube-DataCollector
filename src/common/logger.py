"""Structured JSON logging for all SafeKube components."""

import logging
import json
import sys
from datetime import datetime, timezone
from src.common.config import settings


class JSONFormatter(logging.Formatter):
    """Outputs log records as structured JSON."""

    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "component": record.name,
            "message": record.getMessage(),
        }
        # Add incident_id if present
        if hasattr(record, "incident_id"):
            log_entry["incident_id"] = record.incident_id
        # Add exception info if present
        if record.exc_info and record.exc_info[0]:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)


def get_logger(component_name: str) -> logging.Logger:
    """Get a structured logger for a component.

    Usage:
        from src.common.logger import get_logger
        logger = get_logger("telemetry.log_collector")
        logger.info("Started collecting logs")
    """
    logger = logging.getLogger(component_name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)
        logger.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))

    return logger