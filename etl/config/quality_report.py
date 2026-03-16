"""
Module de rapport qualité des données pour l'ETL ObRail Europe.

Collecte les métriques de qualité tout au long du pipeline ETL :
extraction, nettoyage, transformation, chargement et validation.

Usage depuis n'importe quel module du pipeline :
    from config.quality_report import quality_report
    quality_report.record_cleaning("sncf", "stations", duplicates_removed=200, ...)
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from config.logging_config import setup_logging


class QualityReport:
    """
    Collecteur de métriques de qualité pour le pipeline ETL.

    Enregistre les métriques par source et par entité à chaque étape :
    extraction, nettoyage, transformation, chargement, validation.
    """

    def __init__(self):
        """Initialise le rapport qualité."""
        self.logger = setup_logging("quality.report")
        self.metrics: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._start_time = datetime.now(timezone.utc)

    def _ensure_entry(self, source_name: str, entity: str) -> Dict[str, Any]:
        """
        Garantit l'existence d'une entrée pour source/entité.

        Args:
            source_name: Nom de la source
            entity: Nom de l'entité

        Returns:
            Référence vers le dict de métriques
        """
        if source_name not in self.metrics:
            self.metrics[source_name] = {}
        if entity not in self.metrics[source_name]:
            self.metrics[source_name][entity] = {
                "extraction": {},
                "cleaning": {},
                "transformation": {},
                "loading": {},
                "validation": {},
            }
        return self.metrics[source_name][entity]

    def record_extraction(self, source_name: str, entity: str, rows_extracted: int):
        """
        Enregistre le nombre de lignes extraites pour une source/entité.

        Args:
            source_name: Nom de la source
            entity: Nom de l'entité
            rows_extracted: Nombre de lignes extraites
        """
        entry = self._ensure_entry(source_name, entity)
        entry["extraction"] = {
            "rows_extracted": rows_extracted,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.logger.info(
            f"[QUALITE] {source_name}/{entity} - Extraction : {rows_extracted} lignes"
        )

    def record_cleaning(
        self,
        source_name: str,
        entity: str,
        duplicates_removed: int,
        nulls_found: int,
        rows_after: int,
    ):
        """
        Enregistre les métriques de nettoyage.

        Args:
            source_name: Nom de la source
            entity: Nom de l'entité
            duplicates_removed: Nombre de doublons supprimés
            nulls_found: Nombre de valeurs nulles détectées
            rows_after: Nombre de lignes après nettoyage
        """
        entry = self._ensure_entry(source_name, entity)
        entry["cleaning"] = {
            "duplicates_removed": duplicates_removed,
            "nulls_found": nulls_found,
            "rows_after": rows_after,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.logger.info(
            f"[QUALITE] {source_name}/{entity} - Nettoyage : "
            f"{duplicates_removed} doublons, {nulls_found} nulls, "
            f"{rows_after} lignes restantes"
        )

    def record_transformation(
        self,
        source_name: str,
        entity: str,
        rows_before: int,
        rows_after: int,
        nulls_filled: int,
        rows_rejected: int,
        rejection_reasons: Optional[Dict[str, int]] = None,
    ):
        """
        Enregistre les métriques de transformation.

        Args:
            source_name: Nom de la source
            entity: Nom de l'entité
            rows_before: Nombre de lignes avant transformation
            rows_after: Nombre de lignes après transformation
            nulls_filled: Nombre de valeurs nulles remplies
            rows_rejected: Nombre de lignes rejetées
            rejection_reasons: Raisons de rejet avec leur nombre
        """
        entry = self._ensure_entry(source_name, entity)
        entry["transformation"] = {
            "rows_before": rows_before,
            "rows_after": rows_after,
            "nulls_filled": nulls_filled,
            "rows_rejected": rows_rejected,
            "rejection_reasons": rejection_reasons or {},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.logger.info(
            f"[QUALITE] {source_name}/{entity} - Transformation : "
            f"{rows_before} -> {rows_after} lignes, "
            f"{nulls_filled} nulls comblés, {rows_rejected} rejetées"
        )

    def record_loading(
        self,
        source_name: str,
        entity: str,
        rows_attempted: int,
        rows_loaded: int,
        rows_failed: int,
    ):
        """
        Enregistre les métriques de chargement.

        Args:
            source_name: Nom de la source
            entity: Nom de l'entité
            rows_attempted: Nombre de lignes tentées
            rows_loaded: Nombre de lignes chargées avec succès
            rows_failed: Nombre de lignes en échec
        """
        entry = self._ensure_entry(source_name, entity)
        entry["loading"] = {
            "rows_attempted": rows_attempted,
            "rows_loaded": rows_loaded,
            "rows_failed": rows_failed,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.logger.info(
            f"[QUALITE] {source_name}/{entity} - Chargement : "
            f"{rows_loaded}/{rows_attempted} lignes chargées"
        )

    def record_validation(
        self,
        source_name: str,
        entity: str,
        null_violations: int,
        type_mismatches: int,
    ):
        """
        Enregistre les résultats de validation.

        Args:
            source_name: Nom de la source
            entity: Nom de l'entité
            null_violations: Nombre de violations de contrainte NOT NULL
            type_mismatches: Nombre d'erreurs de type
        """
        entry = self._ensure_entry(source_name, entity)
        entry["validation"] = {
            "null_violations": null_violations,
            "type_mismatches": type_mismatches,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if null_violations > 0 or type_mismatches > 0:
            self.logger.warning(
                f"[QUALITE] {source_name}/{entity} - Validation : "
                f"{null_violations} violations null, {type_mismatches} erreurs de type"
            )

    def get_source_report(self, source_name: str) -> dict:
        """
        Retourne le rapport qualité pour une source spécifique.

        Args:
            source_name: Nom de la source

        Returns:
            Dict avec les métriques par entité pour cette source
        """
        return self.metrics.get(source_name, {})

    def get_summary(self) -> dict:
        """
        Retourne le résumé global de qualité.

        Returns:
            Dict avec les totaux agrégés sur toutes les sources
        """
        summary = {
            "total_sources": len(self.metrics),
            "total_entities": 0,
            "total_extracted": 0,
            "total_duplicates_removed": 0,
            "total_nulls_found": 0,
            "total_nulls_filled": 0,
            "total_rows_rejected": 0,
            "total_rows_loaded": 0,
            "total_rows_failed": 0,
            "total_null_violations": 0,
            "total_type_mismatches": 0,
            "duration_seconds": (
                datetime.now(timezone.utc) - self._start_time
            ).total_seconds(),
        }

        for source_name, entities in self.metrics.items():
            for entity, stages in entities.items():
                summary["total_entities"] += 1

                extraction = stages.get("extraction", {})
                summary["total_extracted"] += extraction.get("rows_extracted", 0)

                cleaning = stages.get("cleaning", {})
                summary["total_duplicates_removed"] += cleaning.get(
                    "duplicates_removed", 0
                )
                summary["total_nulls_found"] += cleaning.get("nulls_found", 0)

                transformation = stages.get("transformation", {})
                summary["total_nulls_filled"] += transformation.get("nulls_filled", 0)
                summary["total_rows_rejected"] += transformation.get(
                    "rows_rejected", 0
                )

                loading = stages.get("loading", {})
                summary["total_rows_loaded"] += loading.get("rows_loaded", 0)
                summary["total_rows_failed"] += loading.get("rows_failed", 0)

                validation = stages.get("validation", {})
                summary["total_null_violations"] += validation.get(
                    "null_violations", 0
                )
                summary["total_type_mismatches"] += validation.get(
                    "type_mismatches", 0
                )

        return summary

    def format_report(self) -> str:
        """
        Formate un rapport qualité complet en texte lisible.

        Returns:
            Chaîne formatée du rapport qualité
        """
        lines: List[str] = []
        summary = self.get_summary()

        lines.append("=" * 60)
        lines.append("RAPPORT QUALITE DES DONNEES")
        lines.append("=" * 60)

        # Tableau détaillé par source/entité
        lines.append(self.format_report_table())

        # Résumé global
        lines.append("")
        lines.append("RESUME GLOBAL :")
        lines.append(f"  Sources traitées      : {summary['total_sources']}")
        lines.append(f"  Entités traitées      : {summary['total_entities']}")
        lines.append(f"  Total extraites       : {summary['total_extracted']}")
        lines.append(
            f"  Doublons supprimés    : {summary['total_duplicates_removed']}"
        )
        lines.append(f"  Nulls détectés        : {summary['total_nulls_found']}")
        lines.append(f"  Nulls comblés         : {summary['total_nulls_filled']}")
        lines.append(f"  Lignes rejetées       : {summary['total_rows_rejected']}")
        lines.append(f"  Lignes chargées       : {summary['total_rows_loaded']}")
        lines.append(f"  Échecs chargement     : {summary['total_rows_failed']}")
        lines.append(f"  Violations NULL       : {summary['total_null_violations']}")
        lines.append(f"  Erreurs de type       : {summary['total_type_mismatches']}")
        lines.append(f"  Durée                 : {summary['duration_seconds']:.1f}s")
        lines.append("=" * 60)

        return "\n".join(lines)

    def format_report_table(self) -> str:
        """
        Formate un tableau des métriques de qualité par source.

        Returns:
            Chaîne formatée du tableau qualité
        """
        header = (
            f"{'Source':<20} | {'Entité':<12} | {'Extrait':>7} | "
            f"{'Doublons':>8} | {'Nulls':>5} | {'Rejetés':>7} | "
            f"{'Chargé':>6} | {'Taux':>5}"
        )
        separator = "-" * len(header)

        lines: List[str] = [header, separator]

        for source_name in sorted(self.metrics.keys()):
            entities = self.metrics[source_name]
            for entity in sorted(entities.keys()):
                stages = entities[entity]

                extracted = stages.get("extraction", {}).get("rows_extracted", 0)
                duplicates = stages.get("cleaning", {}).get("duplicates_removed", 0)
                nulls = stages.get("cleaning", {}).get("nulls_found", 0)
                rejected = stages.get("transformation", {}).get("rows_rejected", 0)
                loaded = stages.get("loading", {}).get("rows_loaded", 0)

                if extracted > 0:
                    rate = f"{(loaded / extracted * 100):.0f}%"
                else:
                    rate = "N/A"

                lines.append(
                    f"{source_name:<20} | {entity:<12} | {extracted:>7} | "
                    f"{duplicates:>8} | {nulls:>5} | {rejected:>7} | "
                    f"{loaded:>6} | {rate:>5}"
                )

        return "\n".join(lines)

    def reset(self):
        """Réinitialise toutes les métriques."""
        self.metrics.clear()
        self._start_time = datetime.now(timezone.utc)
        self.logger.info("[QUALITE] Rapport qualité réinitialisé")


# Instance singleton accessible depuis tout le pipeline
quality_report = QualityReport()
