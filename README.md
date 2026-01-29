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

### General
- `PUBLIC_URL` : URL publique du pod pour les liens de téléchargement
- `API_KEY` : Clé d'authentification pour l'API ID By Rivoli
- `MAX_PENDING_TRACKS` : Nombre maximum de tracks en attente avant blocage (défaut: 1500)
- `PENDING_WARNING_THRESHOLD` : Seuil d'avertissement (défaut: 1000)

### Database Mode (Direct Track Creation with Prisma)

Au lieu d'envoyer les tracks vers une API externe, l'application peut créer les tracks directement dans la base de données PostgreSQL en utilisant Prisma Python (comme l'app NestJS).

**1. Configuration de l'environnement :**
```bash
# URL de connexion à la base de données
export DATABASE_URL="postgresql://user:password@host:5432/database"

# Activer le mode base de données
export USE_DATABASE_MODE=true
```

**2. Installation et génération du client Prisma :**
```bash
# Installer Prisma Python
pip install prisma

# Récupérer le schéma depuis la base de données existante
prisma db pull

# Générer le client Python
prisma generate
```

**3. Vérifier la configuration :**
```bash
curl "https://your-pod-url/database_status"
```

**Avantages du mode Prisma :**
- Utilise le même ORM que l'application NestJS
- Gestion automatique des relations (Artist, Album, ReferenceArtist)
- Validation des types et schéma
- Pas de dépendance au service track.idbyrivoli.com
- Meilleure performance et fiabilité

## API Endpoints

### Confirmation de téléchargement

Les fichiers traités restent disponibles jusqu'à ce que `track.idbyrivoli.com` confirme le téléchargement réussi.

#### POST `/confirm_download`

Confirme le téléchargement réussi d'un track et supprime les fichiers associés.

```bash
curl -X POST "https://your-pod-url/confirm_download" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -d '{"track_name": "Track Name"}'
```

**Paramètres :**
- `track_name` : Nom du track (nom du dossier dans `processed/`)
- `api_key` : Clé d'authentification (via body, query param ou header Authorization)

**Réponse :**
```json
{
  "success": true,
  "message": "Track 'Track Name' confirmed and cleaned up",
  "pending_count": 5
}
```

#### GET `/pending_downloads`

Liste tous les tracks en attente de confirmation de téléchargement.

```bash
curl "https://your-pod-url/pending_downloads?api_key=YOUR_API_KEY"
```

**Réponse :**
```json
{
  "pending_count": 5,
  "max_pending": 20,
  "warning_threshold": 10,
  "warning": {"warning": false, "count": 5},
  "tracks": [...]
}
```

### Gestion des avertissements

- **Seuil d'avertissement** (1000 tracks par défaut) : Un avertissement est affiché
- **Limite critique** (1500 tracks par défaut) : Les nouveaux uploads sont bloqués

Les utilisateurs sont avertis via l'interface web et les réponses API incluent le statut des tracks en attente.

### Database Status

#### GET `/database_status`

Vérifie le statut du mode base de données et la connexion.

```bash
curl "https://your-pod-url/database_status"
```

**Réponse (mode database activé) :**
```json
{
  "database_mode_enabled": true,
  "api_endpoint": null,
  "database_connected": true,
  "database_host": "localhost",
  "database_name": "idbyrivoli"
}
```

**Réponse (mode API) :**
```json
{
  "database_mode_enabled": false,
  "api_endpoint": "https://track.idbyrivoli.com/upload",
  "database_connected": false,
  "note": "Database mode disabled - using external API"
}
```

## Requirements

- Python 3.8+
- FFmpeg
- ~4GB RAM minimum (pour Demucs)
- GPU recommandé pour un traitement plus rapide
