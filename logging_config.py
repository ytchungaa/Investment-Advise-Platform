import logging
import sys
import os
from logging.handlers import RotatingFileHandler

# logger.debug("Fetching token from API")    # Only shown if LOG_LEVEL=DEBUG
# logger.info("Token refreshed successfully")
# logger.warning("Token expires soon")
# logger.error("Failed to refresh token")
# logger.critical("Unable to connect to Schwab API")

# Base logger
logger = logging.getLogger("schwab_project")
logger.setLevel(logging.DEBUG)  # keep lowest here so handlers decide filtering

# Console handler (debug+ above)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)  # show debug+ in console
ch.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(ch)

# File handler (warning+ above)
os.makedirs("logs", exist_ok=True)
fh = RotatingFileHandler("logs/app.log", maxBytes=512_000, backupCount=3, encoding="utf-8")
fh.setLevel(logging.WARNING)  # only warning, error, critical in file
fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(funcName)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(fh)

