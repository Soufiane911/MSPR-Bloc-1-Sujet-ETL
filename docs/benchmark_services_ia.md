# Benchmark Services IA (MLOps)

## Comparatif rapide

| Critere | Azure ML | AWS SageMaker | Google Vertex AI | Hugging Face AutoTrain |
|---|---|---|---|---|
| Positionnement | Suite MLOps enterprise integree Azure | Plateforme MLOps complete AWS | Plateforme ML unifiee GCP | AutoML oriente prototypage rapide |
| Time-to-market | Bon avec templates et AutoML | Bon mais plus d'assemblage initial | Bon, forte integration BigQuery | Tres rapide pour baseline |
| Deploiement API | Managed endpoints, autoscaling | Endpoints temps reel, autoscaling | Endpoints prediction managée | Spaces/Inference endpoints selon offre |
| Monitoring modele | Azure Monitor + drift (ML) | Model Monitor mature | Model Monitoring + Explainable AI | Limite, souvent a completer manuellement |
| Couts | Moyen a eleve selon compute | Flexible mais peut grimper vite | Competitif pour stack GCP | Faible au debut, moins robuste enterprise |
| Gouvernance / IAM | Excellente (Entra, RBAC) | Excellente (IAM granulaire) | Excellente (IAM GCP) | Plus limitee pour gouvernance forte |
| Lock-in cloud | Azure fort | AWS fort | GCP fort | Plus faible (selon architecture) |
| Cas ideal MSPR | Projet deja sur Azure / besoin demo pro | Equipe AWS + pipeline data riche | Equipe data GCP/BigQuery | Prototype rapide avant industrialisation |

## Recommandation pratique pour ObRail

1. Court terme (soutenance): conserver le modele mock local + endpoint `/predict` stable.
2. Moyen terme: industrialiser sur Azure ML ou Vertex AI selon cloud cible ecole/entreprise.
3. Long terme: ajouter suivi drift + re-entrainement planifie + validation humaine des cas limites.

## Criteres de decision proposes

- Cout mensuel cible
- Besoin de monitoring drift / explainability
- Niveau d'integration avec la stack existante (Docker, Prometheus, FastAPI)
- Exigences RGPD (localisation des donnees, retention, droit a l'explication)
