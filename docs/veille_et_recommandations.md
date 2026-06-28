# Veille et recommandations (Risques, RGPD, biais)

## Veille techno

Les tendances observees en 2025-2026 sur les APIs de prediction en production:

- Standardisation des pipelines MLOps (feature store, registre de modeles, monitoring drift).
- Exigence croissante d'explicabilite et de tracabilite (audit de prediction, versionning modele).
- Approche "human-in-the-loop" sur les classes incertaines.
- Surveillance des performances de modeles en continu (latence, taux d'erreur, derive).

## Risques principaux

1. Derive de donnees: distribution des trajets evolue selon saisons, greves, travaux.
2. Derive conceptuelle: ce qui definit "jour" et "nuit" peut changer selon regles metier.
3. Risque de qualite source: donnees incompletes ou heterogenes entre operateurs.
4. Risque operationnel: endpoint indisponible ou latence excessive pendant demo/production.

## RGPD et conformite

- Minimisation: ne pas collecter de donnees personnelles non necessaires pour `/predict`.
- Finalite: la prediction doit rester liee a l'analyse transport (pas de profilage individuel).
- Retention: conserver uniquement les journaux techniques utiles (latence, classe predite, version modele).
- Transparence: documenter le role du modele, ses limites et ses criteres d'utilisation.
- Droit a l'explication: fournir une explication simplifiee de la logique de prediction.

## Biais potentiels

- Biais geographique: sur-representation de certains pays/reseaux.
- Biais temporel: sur-apprentissage de periodes specifiques (vacances, weekends).
- Biais de source: qualite differente selon fournisseurs de donnees.

## Mesures de mitigation

1. Echantillonnage equilibre par pays/operateur/plage horaire.
2. Seuil de confiance + revue humaine pour les predictions ambiguës.
3. Monitoring continu: classes predites, latence, erreurs, drift.
4. Reevaluation periodique des jeux de test et du contrat API.

## Recommandations de mise en production

- Conserver un mode fallback (modele mock/regles) si modele indisponible.
- Versionner strictement le modele et les features (`model_version`, schema request/response).
- Introduire un test de non-regression sur `/predict` en CI.
- Ajouter une alerte Prometheus sur hausse d'erreurs de prediction.
