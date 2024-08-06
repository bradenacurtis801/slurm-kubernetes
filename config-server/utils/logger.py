# utils/logger.py

import logging

def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("server.log"), logging.StreamHandler()],
    )

    logger = logging.getLogger(__name__)
    return logger
