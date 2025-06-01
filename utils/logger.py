import logging
import os

def get_logger(name=__name__, log_files="scraper.log", level=logging.DEBUG):
    log_dir = "Logs"
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, log_files)
    logger = logging.getLogger(name)

    if not logger.hasHandlers():
        logger.setLevel(level)  
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

        ch = logging.StreamHandler()
        ch.setLevel(level)  
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        fh = logging.FileHandler(log_path)
        fh.setLevel(level)  # Match level
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger