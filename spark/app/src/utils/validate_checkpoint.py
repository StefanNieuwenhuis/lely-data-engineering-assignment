import logging
import os
import shutil

log = logging.getLogger(__name__)

def validate_checkpoint(path: str):
    # if corrupted (e.g., missing VERSION file), reset
    if os.path.exists(path) and not os.path.exists(os.path.join(path, "commits")):
        log.warning(f"Resetting corrupt checkpoint at {path}")
        shutil.rmtree(path, ignore_errors=True)