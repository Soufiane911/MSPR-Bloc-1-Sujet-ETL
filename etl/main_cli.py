#!/usr/bin/env python3
"""
CLI léger pour ObRail ETL - Démarrage rapide pour les commandes simples.

Ce fichier est volontairement minimaliste pour démarrer instantanément.
Usage:
    python main_cli.py --status        # Affiche le statut des sources (< 2s)
    python main_cli.py --quick-check   # Vérifie rapidement le cache local
    python main_cli.py --run           # Lance l'ETL complet (délègue à main.py)
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime, timezone

# Configuration minimale - pas d'import lourd ici
CACHE_DIR = Path(__file__).parent.parent / "data" / ".cache"
SOURCES_FILE = Path(__file__).parent / "config" / "sources.py"


def load_cache(source_name: str) -> dict:
    """Charge le cache local d'une source."""
    cache_path = CACHE_DIR / f"{source_name}.json"
    if cache_path.exists():
        with open(cache_path, "r") as f:
            return json.load(f)
    return None


def get_sources_list() -> dict:
    """Retourne la liste des sources depuis sources.py sans l'importer."""
    # Lecture directe du fichier pour éviter les imports lourds
    sources = {}
    if SOURCES_FILE.exists():
        content = SOURCES_FILE.read_text()
        # Parsing minimal pour récupérer les noms
        if "SOURCES" in content:
            # Extraction basique des clés
            import re
            matches = re.findall(r'"([^"]+)":\s*\{', content)
            for name in matches:
                sources[name] = {"name": name}
    return sources


def show_status_fast():
    """Affiche le statut rapide basé uniquement sur le cache local."""
    print("=" * 70)
    print("STATUT RAPIDE DES SOURCES (basé sur cache local)")
    print("=" * 70)
    print()
    
    if not CACHE_DIR.exists():
        print("⚠️  Aucun cache trouvé. Lancez 'python main.py --refresh' pour initialiser.")
        return 1
    
    total = 0
    up_to_date = 0
    expired = 0
    never = 0
    
    for cache_file in CACHE_DIR.glob("*.json"):
        source_name = cache_file.stem
        cache = load_cache(source_name)
        total += 1
        
        if cache:
            last_download = cache.get("last_download", "Inconnu")
            if last_download and last_download != "Inconnu":
                try:
                    dt = datetime.fromisoformat(last_download.replace("Z", "+00:00"))
                    age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
                    age_days = age_hours / 24
                    
                    if age_days > 7:
                        status = "⏰ Expiré"
                        expired += 1
                    else:
                        status = "✅ OK"
                        up_to_date += 1
                    
                    date_str = dt.strftime("%d/%m/%Y %H:%M")
                    print(f"  {source_name:<25} {date_str:<20} {status}")
                except:
                    print(f"  {source_name:<25} {last_download:<20} ❓ Erreur date")
            else:
                print(f"  {source_name:<25} {'Jamais':<20} ⚠️  Jamais")
                never += 1
        else:
            print(f"  {source_name:<25} {'Inconnu':<20} ⚠️  Pas de cache")
            never += 1
    
    print()
    print("-" * 70)
    print(f"Total: {total} sources | ✅ {up_to_date} à jour | ⏰ {expired} expirées | ⚠️ {never} jamais")
    print("=" * 70)
    print()
    print("💡 Pour mettre à jour: python main.py --refresh")
    print("💡 Pour lancer l'ETL complet: python main.py --full")
    
    return 0


def run_full_etl(args):
    """Délègue à main.py pour l'ETL complet."""
    print("🚀 Lancement de l'ETL complet...")
    print()
    
    # Import lazy de main.py uniquement quand nécessaire
    import subprocess
    
    cmd = [sys.executable, "-m", "etl.main"]
    
    if args.source:
        cmd.extend(["--source", args.source])
    if args.refresh:
        cmd.append("--refresh")
    if args.force:
        cmd.append("--force")
    if args.skip_download:
        cmd.append("--skip-download")
    
    result = subprocess.run(cmd, cwd=Path(__file__).parent.parent)
    return result.returncode


def main():
    """Point d'entrée CLI rapide."""
    parser = argparse.ArgumentParser(
        description="CLI rapide ObRail ETL",
        prog="main_cli.py"
    )
    
    parser.add_argument("--status", action="store_true", help="Statut rapide depuis cache")
    parser.add_argument("--run", action="store_true", help="Lancer l'ETL complet")
    parser.add_argument("--source", type=str, help="Source spécifique")
    parser.add_argument("--refresh", action="store_true", help="Vérifier et télécharger si obsolète")
    parser.add_argument("--force", action="store_true", help="Forcer le téléchargement")
    parser.add_argument("--skip-download", action="store_true", help="Skip la phase de téléchargement")
    
    args = parser.parse_args()
    
    if args.status:
        return show_status_fast()
    elif args.run or any([args.refresh, args.force, args.skip_download, args.source]):
        return run_full_etl(args)
    else:
        parser.print_help()
        print()
        print("💡 Utilisez --status pour un statut rapide (< 2s)")
        print("💡 Utilisez --run pour lancer l'ETL complet")
        return 0


if __name__ == "__main__":
    sys.exit(main())
