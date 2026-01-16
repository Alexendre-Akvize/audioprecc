# ID By Rivoli - Audio Separator

Application web de séparation audio et création d'éditions DJ.

## Fonctionnalités

- **Séparation vocale/instrumentale** via Demucs (IA)
- **Génération d'éditions DJ** : Clap In, Acap In, Extended, Short, Slam...
- **Export MP3 & WAV** avec métadonnées complètes
- **Téléchargement ZIP** de tous les fichiers traités
- **Envoi automatique** des métadonnées vers l'API ID By Rivoli

## Installation sur RunPod

### 1. Cloner le repository

```bash
git clone https://github.com/EytanTeachr/IDByRivoli-separate-audio.git
cd IDByRivoli-separate-audio
```

### 2. Installer les dépendances système

```bash
apt-get update && apt-get install -y ffmpeg
```

### 3. Installer les dépendances Python

```bash
pip install --ignore-installed -r requirements.txt
```

### 4. Lancer l'application

```bash
# Port par défaut (8888)
python app.py

# Ou avec un port personnalisé
python app.py -p 8889
python app.py --port 9000
```

L'application sera accessible sur le port choisi :
`https://[votre-pod-id]-[PORT].proxy.runpod.net/`

**Exemples pour plusieurs pods simultanés :**
- Pod 1 : `python app.py -p 8888` → `https://xxx-8888.proxy.runpod.net/`
- Pod 2 : `python app.py -p 8889` → `https://xxx-8889.proxy.runpod.net/`
- Pod 3 : `python app.py -p 8890` → `https://xxx-8890.proxy.runpod.net/`

## Commandes utiles

### Mettre à jour vers la dernière version

```bash
cd IDByRivoli-separate-audio
git pull
python app.py
```

### Lancer en arrière-plan

```bash
nohup python app.py > app.log 2>&1 &
```

## Structure des fichiers

```
IDByRivoli-separate-audio/
├── app.py                 # Application Flask principale
├── audio_processor.py     # Logique de traitement audio
├── templates/
│   └── index.html         # Interface web
├── static/
│   ├── covers/            # Pochettes
│   └── fonts/             # Police ClashGrotesk
├── uploads/               # Fichiers uploadés
├── output/                # Fichiers Demucs (vocals/instrumental)
├── processed/             # Fichiers finaux (éditions)
└── assets/                # Ressources (clap sample, covers)
```

## Genres et éditions

- **House, Electro House, Dance** : Export uniquement en version "Main" (MP3 + WAV)
- **Autres genres** : Suite complète d'éditions DJ (Clap In, Acap In, Extended, etc.)

## Configuration

Les variables d'environnement suivantes peuvent être configurées :

- `PUBLIC_URL` : URL publique du pod pour les liens de téléchargement
- `API_KEY` : Clé d'authentification pour l'API ID By Rivoli

## Requirements

- Python 3.8+
- FFmpeg
- ~4GB RAM minimum (pour Demucs)
- GPU recommandé pour un traitement plus rapide
