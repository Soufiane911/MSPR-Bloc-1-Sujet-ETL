"""
Configuration du logging pour l'ETL ObRail Europe.
"""

import logging
import sys
from pathlib import Path

# Création du répertoire de logs
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Format des logs
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(name: str = "obrail_etl") -> logging.Logger:
    """
    Configure et retourne un logger.
    
    Args:
        name: Nom du logger
        
    Returns:
        Logger configuré
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Éviter les handlers dupliqués
    if logger.handlers:
        return logger
    
    # Handler console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
    console_handler.setFormatter(console_formatter)
    
    # Handler fichier
    file_handler = logging.FileHandler(LOG_DIR / "etl.log")
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
    file_handler.setFormatter(file_formatter)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger
