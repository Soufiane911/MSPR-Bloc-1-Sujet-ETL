"""
Router pour les endpoints liés aux trains.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from app.models.train import Train, TrainDetail, ClassificationMetadata
from app.services.train_service import TrainService

router = APIRouter()
train_service = TrainService()


def _build_classification_metadata(
    train_data: dict,
) -> Optional[ClassificationMetadata]:
    """Construit l'objet ClassificationMetadata si les donnees sont presentes."""
    if train_data.get("classification_method") is None:
        return None
    return ClassificationMetadata(
        classification_method=train_data.get("classification_method"),
        classification_reason=train_data.get("classification_reason"),
        classification_confidence=train_data.get("classification_confidence"),
        ml_night_probability=train_data.get("ml_night_probability"),
        night_percentage=train_data.get("night_percentage"),
        needs_manual_review=train_data.get("needs_manual_review", False),
    )


@router.get("/", response_model=List[Train])
def get_trains(
    train_type: Optional[str] = Query(
        None, description="Type de train: 'day' ou 'night'"
    ),
    operator: Optional[str] = Query(
        None, description="Nom de l'opérateur (recherche partielle)"
    ),
    category: Optional[str] = Query(
        None, description="Catégorie (TGV, ICE, Eurostar, etc.)"
    ),
    country: Optional[str] = Query(None, description="Code pays (FR, DE, etc.)"),
    limit: int = Query(100, ge=1, le=1000, description="Nombre maximum de résultats"),
    offset: int = Query(0, ge=0, description="Décalage pour la pagination"),
):
    """
    Récupère la liste des trains avec filtres optionnels.

    - **train_type**: Filtre par type (day/night)
    - **operator**: Filtre par nom d'opérateur (recherche partielle, insensible à la casse)
    - **category**: Filtre par catégorie de train
    - **country**: Filtre par code pays
    - **limit**: Nombre maximum de résultats (1-1000)
    - **offset**: Décalage pour la pagination

    Returns:
        List[Train]: Liste des trains correspondants
    """
    trains = train_service.get_trains(
        train_type=train_type,
        operator=operator,
        category=category,
        country=country,
        limit=limit,
        offset=offset,
    )
    return trains


@router.get("/{train_id}", response_model=TrainDetail)
def get_train(train_id: int):
    """
    Récupère les détails d'un train spécifique.

    - **train_id**: Identifiant unique du train

    Returns:
        TrainDetail: Détails du train avec métadonnées de classification

    Raises:
        HTTPException: 404 si le train n'est pas trouvé
    """
    train = train_service.get_train_by_id(train_id)
    if not train:
        raise HTTPException(status_code=404, detail="Train non trouvé")

    classification = _build_classification_metadata(train)

    return TrainDetail(
        train_id=train["train_id"],
        train_number=train["train_number"],
        operator_name=train["operator_name"],
        train_type=train["train_type"],
        category=train.get("category"),
        route_name=train.get("route_name"),
        operator_id=train["operator_id"],
        country=train["country"],
        classification=classification,
        created_at=train["created_at"],
        updated_at=train["updated_at"],
    )


@router.get("/{train_id}/schedules")
def get_train_schedules(
    train_id: int,
    limit: int = Query(100, ge=1, le=1000, description="Nombre maximum de résultats"),
):
    """
    Récupère les dessertes d'un train spécifique.

    - **train_id**: Identifiant unique du train
    - **limit**: Nombre maximum de résultats

    Returns:
        list: Liste des dessertes du train

    Raises:
        HTTPException: 404 si le train n'est pas trouvé
    """
    # Vérifier que le train existe
    train = train_service.get_train_by_id(train_id)
    if not train:
        raise HTTPException(status_code=404, detail="Train non trouvé")

    schedules = train_service.get_train_schedules(train_id, limit=limit)
    return schedules


@router.get("/count/by-type")
def count_trains_by_type():
    """
    Compte les trains par type (jour/nuit).

    Returns:
        dict: Comptage par type
    """
    return train_service.count_by_type()


@router.get("/count/by-country")
def count_trains_by_country():
    """
    Compte les trains par pays.

    Returns:
        dict: Comptage par pays
    """
    return train_service.count_by_country()
