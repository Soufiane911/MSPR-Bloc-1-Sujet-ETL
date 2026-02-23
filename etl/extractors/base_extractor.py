"""
Classe de base pour les extracteurs de données.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict
from config.logging_config import setup_logging


class BaseExtractor(ABC):
    """Classe abstraite pour les extracteurs de données."""
    
    def __init__(self, source_name: str):
        """
        Initialise l'extracteur.
        
        Args:
            source_name: Nom de la source de données
        """
        self.source_name = source_name
        self.logger = setup_logging(f"extractor.{source_name}")
        self.data = {}
    
    @abstractmethod
    def extract(self) -> Dict[str, Any]:
        """
        Extrait les données de la source.
        
        Returns:
            Dictionnaire des données extraites
        """
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        """
        Valide l'intégrité des données extraites.
        
        Returns:
            True si les données sont valides, False sinon
        """
        pass
    
    def get_data(self) -> Dict[str, Any]:
        """
        Retourne les données extraites.
        
        Returns:
            Dictionnaire des données
        """
        return self.data
