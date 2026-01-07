#!/usr/bin/env python3
"""
Script expérimental pour extraire les datasets depuis la documentation (Markdown)
et trouver les liens de téléchargement dynamiquement via Web Scraping.
"""

import re
import logging
import requests
import sys
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import unicodedata

# Ajouter le chemin parent pour importer extract_all si besoin
sys.path.insert(0, str(Path(__file__).parent.parent))
# Tenter d'importer la fonction de téléchargement existante
try:
    from extract.extract_all import download_file, extract_zip
    from config.config import PATHS
except ImportError:
    # Si l'import échoue (ex: exécution depuis etl/), fallback simple
    def download_file(url, dest_path, logger):
        try:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(dest_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            return True
        except Exception as e:
            logger.error(f"Erreur DL: {e}")
            return False
            
    def extract_zip(zip_path, extract_dir, logger):
        import zipfile
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                z.extractall(extract_dir)
            return True
        except Exception as e:
            logger.error(f"Erreur Unzip: {e}")
            return False

    # Fallback path si config non accessible
    PATHS = {
        "raw_data": Path(__file__).parent.parent / "data" / "raw",
        "raw_archives": Path(__file__).parent.parent / "data" / "raw" / "archives",
        "raw_extracted": Path(__file__).parent.parent / "data" / "raw" / "extracted"
    }

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def slugify(value):
    """
    Normalise une chaîne pour l'utiliser comme nom de fichier.
    """
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value).strip().lower()
    return re.sub(r'[-\s]+', '_', value)

def get_markdown_links(file_path):
    """Extrait les liens (Titres, URL) d'un fichier Markdown"""
    links = []
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        # Regex pour trouver [Titre](URL)
        matches = re.findall(r'\[(.*?)\]\((http[s]?://.*?)\)', content)
        for title, url in matches:
            links.append({'title': title, 'url': url})
    return links

def find_download_link(portal_url):
    """
    Scrape une page (ex: transport.data.gouv.fr) pour trouver le lien de téléchargement (GTFS/ZIP)
    """
    try:
        response = requests.get(portal_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Logique spécifique pour transport.data.gouv.fr
        if "transport.data.gouv.fr" in portal_url:
            candidates = []
            # Chercher les liens qui finissent par .zip ou contiennent 'gtfs'
            for a in soup.find_all('a', href=True):
                href = a['href']
                href_lower = href.lower()
                
                # Exclure les liens de navigation
                if "/explore/" in href_lower or "/datasets" in href_lower or "/login" in href_lower:
                    continue
                
                # Priorité aux ZIP
                if href_lower.endswith('.zip'):
                    candidates.insert(0, href) # Mettre en premier
                elif 'gtfs' in href_lower and not href_lower.startswith('/'):
                     candidates.append(href)
            
            if candidates:
                # Retourner le premier candidat (priorité aux ZIP)
                best_link = candidates[0]
                if best_link.startswith('/'):
                     return f"https://transport.data.gouv.fr{best_link}"
                return best_link
        
        # Logique pour Back-on-Track (Ninja Tables)
        elif "back-on-track.eu" in portal_url:
             # Chercher dans les scripts car c'est une Ninja Table (AJAX)
             import json
             for script in soup.find_all('script'):
                 if script.string and "ninja_tables_public_action" in script.string:
                     # Extraction bourrine mais efficace de l'URL AJAX
                     match = re.search(r'data_request_url":"(.*?)"', script.string)
                     if match:
                         # Décoder les unicode escapes (ex: \/)
                         ajax_url = match.group(1).replace('\\/', '/')
                         return ajax_url

        return None
    except Exception as e:
        logger.error(f"Erreur scraping {portal_url}: {e}")
        return None

def main():
    # 1. Lire le fichier Markdown
    md_path = Path(__file__).parent.parent.parent / "note/02_liens_importants.md"
    
    if not md_path.exists():
        logger.error(f"Fichier non trouvé: {md_path}")
        return

    logger.info(f"📖 Lecture de {md_path.name}...")
    links = get_markdown_links(md_path)
    
    # 2. Filtrer les liens intéressants (Data Portals)
    # On s'intéresse aux liens qui sont probablement des sources de données
    data_portals = [
        l for l in links 
        if "transport.data.gouv.fr" in l['url'] 
        or "back-on-track.eu" in l['url']
    ]
    
    logger.info(f"🔍 {len(data_portals)} sources potentielles trouvées dans la doc.")
    
    # 3. Trouver les liens de téléchargement
    found_resources = []
    
    for item in data_portals:
        logger.info(f"   Analys d'URL: {item['title']} -> {item['url']}")
        direct_link = find_download_link(item['url'])
        
        if direct_link:
            logger.info(f"   ✅ Lien trouvé: {direct_link}")
            found_resources.append({
                'name': item['title'],
                'portal': item['url'],
                'direct_link': direct_link
            })
        else:
            logger.warning(f"   ❌ Pas de lien direct identifié automatiquement.")

    # 4. Afficher le résultat (ou lancer le téléchargement)
    print("\n" + "="*60)
    print("🎯 RÉSULTAT DU SCRAPING DEPUIS LA DOC")
    print("="*60)
    
    # Dossiers de destination
    raw_dir = PATHS.get("raw_data", Path("data/raw"))
    archives_dir = PATHS.get("raw_archives", raw_dir / "archives")
    extracted_dir = PATHS.get("raw_extracted", raw_dir / "extracted")
    
    archives_dir.mkdir(parents=True, exist_ok=True)
    extracted_dir.mkdir(parents=True, exist_ok=True)
    
    downloaded_count = 0
    
    for res in found_resources:
        title = res['name']
        url = res['direct_link']
        
        print(f"📂 {title}")
        print(f"   🔗 {url}")
        
        # Déterminer le nom du fichier
        slug = slugify(title)
        if "csv" in title.lower() or "json" in title.lower() or "database" in title.lower():
            ext = ".json" # Pour Back-on-Track c'est du JSON
        else:
            ext = ".zip"
            
        filename = f"{slug}{ext}"
        dest_path = archives_dir / filename
        
        print(f"   ⬇️  Téléchargement vers {dest_path}...")
        if download_file(url, dest_path, logger):
            print(f"   ✅ Téléchargement OK")
            downloaded_count += 1
            
            # Extraction si ZIP
            if ext == ".zip":
                extract_path = extracted_dir / slug
                print(f"   📦 Extraction vers {extract_path}...")
                if extract_zip(dest_path, extract_path, logger):
                     print(f"   ✅ Extraction OK")
                else:
                     print(f"   ❌ Erreur Extraction")
        else:
            print(f"   ❌ Erreur Téléchargement")
            
        print("-" * 30)

    print(f"\n🎉 Terminé ! {downloaded_count}/{len(found_resources)} fichiers téléchargés.")

if __name__ == "__main__":
    main()
