# Utiliser l'image officielle PyTorch avec CUDA (support GPU NVIDIA)
# C'est la base indispensable pour la vitesse x100
FROM pytorch/pytorch:2.1.0-cuda12.1-cudnn8-runtime

# Éviter les fichiers .pyc
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Installer ffmpeg (nécessaire pour l'audio) et git
RUN apt-get update && apt-get install -y \
    ffmpeg \
    git \
    && rm -rf /var/lib/apt/lists/*

# Dossier de travail dans le conteneur
WORKDIR /app

# Copier les fichiers de dépendances
COPY requirements.txt .

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code de l'application
COPY . .

# Créer les dossiers nécessaires
RUN mkdir -p uploads output processed

# Exposer le port 5001
EXPOSE 5001

# Commande de démarrage
# On utilise gunicorn pour un serveur de prod plus robuste que le serveur Flask dev
# Mais pour l'instant on reste sur python app.py pour garder la compatibilité avec le code actuel
CMD ["python", "app.py"]
