#!/usr/bin/env python3
"""
Script d'orchestration du pipeline ETL ObRail Europe
Permet d'exécuter le pipeline complet ou des étapes individuelles
"""

import argparse
import logging
import sys
from pathlib import Path

# Ajouter le dossier etl au path pour les imports
sys.path.insert(0, str(Path(__file__).parent))

from config.config import DATA_PATHS

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract():
    """Extrait les données sources"""
    logger.info("="*60)
    logger.info("📥 ÉTAPE 1: EXTRACTION")
    logger.info("="*60)
    
    try:
        from extract.extract_all import main as extract_main
        extract_main()
        logger.info("✅ Extraction terminée avec succès")
        return True
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'extraction: {e}")
        return False


def transform():
    """Transforme les données"""
    logger.info("="*60)
    logger.info("🔄 ÉTAPE 2: TRANSFORMATION")
    logger.info("="*60)
    
    try:
        from transform.transform_all import main as transform_main
        transform_main()
        logger.info("✅ Transformation terminée avec succès")
        return True
    except Exception as e:
        logger.error(f"❌ Erreur lors de la transformation: {e}")
        return False


def load():
    """Charge les données en base PostgreSQL"""
    logger.info("="*60)
    logger.info("💾 ÉTAPE 3: CHARGEMENT EN BASE")
    logger.info("="*60)
    
    try:
        from load.load_data import main as load_main
        load_main()
        logger.info("✅ Chargement terminé avec succès")
        return True
    except Exception as e:
        logger.error(f"❌ Erreur lors du chargement: {e}")
        return False


def run_all():
    """Exécute le pipeline complet"""
    logger.info("="*60)
    logger.info("🚀 PIPELINE ETL COMPLET - OBRAIL EUROPE")
    logger.info("="*60)
    logger.info(f"📁 Données Raw: {DATA_PATHS['raw']}")
    logger.info(f"📁 Données Clean: {DATA_PATHS['clean']}")
    logger.info("="*60)
    
    success = True
    
    # Étape 1: Extraction
    logger.info("\n")
    if not extract():
        success = False
        logger.error("❌ Le pipeline s'est arrêté à l'étape d'extraction")
        return False
    
    # Étape 2: Transformation
    logger.info("\n")
    if not transform():
        success = False
        logger.error("❌ Le pipeline s'est arrêté à l'étape de transformation")
        return False
    
    # Étape 3: Chargement
    logger.info("\n")
    if not load():
        success = False
        logger.error("❌ Le pipeline s'est arrêté à l'étape de chargement")
        return False
    
    # Résumé final
    logger.info("\n")
    logger.info("="*60)
    logger.info("🎉 PIPELINE ETL TERMINÉ AVEC SUCCÈS!")
    logger.info("="*60)
    logger.info("📁 Données disponibles dans la base PostgreSQL")
    logger.info("➡️  Prochaine étape: Lancer l'API et le dashboard")
    logger.info("="*60)
    
    return True


def main():
    """Point d'entrée principal"""
    parser = argparse.ArgumentParser(
        description='Pipeline ETL ObRail Europe - Orchestration',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  python run_etl.py --all      # Exécute tout le pipeline (extraction + transformation + chargement)
  python run_etl.py --extract  # Exécute uniquement l'extraction
  python run_etl.py --transform # Exécute uniquement la transformation
  python run_etl.py --load     # Exécute uniquement le chargement
        """
    )
    
    parser.add_argument('--all', action='store_true', help='Exécute tout le pipeline (défaut)')
    parser.add_argument('--extract', action='store_true', help='Exécute uniquement l\'extraction')
    parser.add_argument('--transform', action='store_true', help='Exécute uniquement la transformation')
    parser.add_argument('--load', action='store_true', help='Exécute uniquement le chargement')
    
    args = parser.parse_args()
    
    # Si aucune option n'est spécifiée, exécuter tout
    if not any([args.all, args.extract, args.transform, args.load]):
        args.all = True
    
    if args.all:
        run_all()
    else:
        success = True
        
        if args.extract:
            if not extract():
                success = False
        
        if args.transform:
            if not transform():
                success = False
        
        if args.load:
            if not load():
                success = False
        
        if success:
            logger.info("\n✅ Étape(s) terminée(s) avec succès")
        else:
            logger.error("\n❌ Certaines étapes ont échoué")
            sys.exit(1)


if __name__ == "__main__":
    main()
