"""
Router pour les endpoints liés aux opérateurs.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from app.services.operator_service import OperatorService

router = APIRouter()
operator_service = OperatorService()


@router.get("/")
def get_operators(
    country: Optional[str] = Query(None, description="Code pays (FR, DE, etc.)"),
    name: Optional[str] = Query(None, description="Nom de l'opérateur (recherche partielle)"),
    limit: int = Query(100, ge=1, le=1000, description="Nombre maximum de résultats"),
    offset: int = Query(0, ge=0, description="Décalage pour la pagination")
):
    """
    Récupère la liste des opérateurs avec filtres.
    
    - **country**: Filtre par code pays
    - **name**: Filtre par nom (recherche partielle)
    - **limit**: Nombre maximum de résultats
    - **offset**: Décalage pour la pagination
    
    Returns:
        list: Liste des opérateurs
    """
    operators = operator_service.get_operators(
        country=country,
        name=name,
        limit=limit,
        offset=offset
    )
    return operators


@router.get("/{operator_id}")
def get_operator(operator_id: int):
    """
    Récupère les détails d'un opérateur spécifique.
    
    - **operator_id**: Identifiant unique de l'opérateur
    
    Returns:
        dict: Détails de l'opérateur
        
    Raises:
        HTTPException: 404 si l'opérateur n'est pas trouvé
    """
    operator = operator_service.get_operator_by_id(operator_id)
    if not operator:
        raise HTTPException(status_code=404, detail="Opérateur non trouvé")
    return operator


@router.get("/{operator_id}/trains")
def get_operator_trains(
    operator_id: int,
    train_type: Optional[str] = Query(None, description="Type: 'day' ou 'night'"),
    limit: int = Query(100, ge=1, le=1000, description="Nombre maximum de résultats")
):
    """
    Récupère les trains d'un opérateur.
    
    - **operator_id**: Identifiant unique de l'opérateur
    - **train_type**: Filtre par type (day/night)
    - **limit**: Nombre maximum de résultats
    
    Returns:
        list: Liste des trains
        
    Raises:
        HTTPException: 404 si l'opérateur n'est pas trouvé
    """
    # Vérifier que l'opérateur existe
    operator = operator_service.get_operator_by_id(operator_id)
    if not operator:
        raise HTTPException(status_code=404, detail="Opérateur non trouvé")
    
    trains = operator_service.get_trains(
        operator_id=operator_id,
        train_type=train_type,
        limit=limit
    )
    return trains


@router.get("/count/by-country")
def count_operators_by_country():
    """
    Compte les opérateurs par pays.
    
    Returns:
        dict: Comptage par pays
    """
    return operator_service.count_by_country()
