FROM python:3.11-slim

WORKDIR /app

# Installation des dépendances système
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copie des requirements
COPY requirements.txt .

# Installation des dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code
COPY . .

# Création des répertoires nécessaires
RUN mkdir -p data/raw data/processed logs

# Commande par défaut
CMD ["python", "main.py"]
