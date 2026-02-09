"""
Database Service for ID By Rivoli
Uses Prisma Python client to create/update tracks directly in the database.
Uploads files to S3 before storing in database (like the NestJS app).

Setup:
1. pip install prisma boto3
2. prisma db pull  (to get schema from existing database)
3. prisma generate (to generate Python client)
4. Set S3 environment variables (S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, etc.)
"""

import os
import re
import json
from datetime import datetime
from typing import Optional, Dict, Any, List
import threading
import traceback

# Try to import Prisma client
PRISMA_AVAILABLE = False
Prisma = None
PrismaJson = None
try:
    from prisma import Prisma
    from prisma import Json as PrismaJson
    PRISMA_AVAILABLE = True
    print("✅ Prisma client available")
except ImportError as e:
    print(f"⚠️ Prisma client not available: {e}")
    print("   Run: pip install prisma && prisma db pull && prisma generate")

# Try to import S3 service
S3_AVAILABLE = False
try:
    from s3_service import get_s3_service, S3Service
    _s3 = get_s3_service()
    S3_AVAILABLE = _s3.is_configured
    if S3_AVAILABLE:
        print("✅ S3 service available")
    else:
        print("⚠️ S3 service not configured - files won't be uploaded to S3")
except ImportError as e:
    print(f"⚠️ S3 service not available: {e}")

# Try to import waveform generator
WAVEFORM_AVAILABLE = False
try:
    from waveform_generator import generate_waveform_from_url
    WAVEFORM_AVAILABLE = True
    print("✅ Waveform generator available")
except ImportError as e:
    print(f"⚠️ Waveform generator not available: {e}")

# Type to file field mapping (matching NestJS create-track.dto.ts)
TYPE_TO_FILE_FIELD_MAP = {
    # Main versions
    'Main': 'trackFile',
    'main': 'trackFile',
    
    # Extended versions
    'Extended': 'extendedTrackMp3',
    'extended': 'extendedTrackMp3',
    
    # Original track versions
    'Original': 'originalTrackMp3',
    'Original Main': 'originalTrackMp3Main',
    'Original Clean': 'originalTrackMp3Clean',
    'Original Dirty': 'originalTrackMp3Dirty',
    'Original Wave': 'originalTrackWave',
    'Original Wave Clean': 'originalTrackWaveClean',
    'Original Wave Dirty': 'originalTrackWaveDirty',
    
    # Extended versions (detailed)
    'Extended Clean': 'extendedTrackMp3Clean',
    'Extended Dirty': 'extendedTrackMp3Dirty',
    'Extended Wave': 'extendedTrackWave',
    'Extended Wave Clean': 'extendedTrackWaveClean',
    'Extended Wave Dirty': 'extendedTrackWaveDirty',
    
    # Acap versions
    'Acap In': 'acapIn',
    'Acap Out': 'acapOut',
    'Acapella': 'acapella',
    'acap in': 'acapIn',
    'acap out': 'acapOut',
    'acapella': 'acapella',
    
    # Intro/Outro
    'Intro': 'intro',
    'intro': 'intro',
    
    # Short versions
    'Short': 'short',
    'Short Main': 'shortMain',
    'Short Acap In': 'shortAcapIn',
    'Short Acap Out': 'shortAcapOut',
    'Short Clap In': 'shortClapIn',
    'short': 'short',
    'short main': 'shortMain',
    'short acap in': 'shortAcapIn',
    'short acap out': 'shortAcapOut',
    'short clap in': 'shortClapIn',
    
    # Clap versions
    'Clap In': 'clapInMain',
    'Clap In Main': 'clapInMain',
    'Clap In Short Acap Out': 'clapInShortAcapOut',
    'clap in': 'clapInMain',
    'clap in main': 'clapInMain',
    
    # Acap In/Out combinations
    'Acap In Acap Out': 'acapInAcapOutMain',
    'Acap In Acap Out Main': 'acapInAcapOutMain',
    'acap in acap out': 'acapInAcapOutMain',
    
    # Slam versions
    'Slam': 'slamDirtyMain',
    'Slam Dirty Main': 'slamDirtyMain',
    'Slam Intro Short Acap Out': 'slamIntroShortAcapOut',
    'slam': 'slamDirtyMain',
    
    # Other
    'Super Short': 'superShort',
    'super short': 'superShort',
    'Instrumental': 'instru',
    'Instru': 'instru',
    'instrumental': 'instru',
    'instru': 'instru',
    
    # Preview
    'Preview': 'trackPreview',
    'preview': 'trackPreview',
}

# Fields that have WAV variants (only Demucs-generated edits — Main/Original/Extended handled separately)
# Acapella, Intro, Instrumental, Short, Super Short are MP3-only (no WAV needed)
FIELDS_WITH_WAV_VARIANTS = [
    'clapInMain', 'shortMain', 'shortAcapIn', 'shortClapIn',
    'acapInAcapOutMain', 'slamDirtyMain', 'shortAcapOut',
    'clapInShortAcapOut', 'slamIntroShortAcapOut',
]

# Types that are MP3-only — if a WAV is uploaded for these, skip it
MP3_ONLY_FIELDS = [
    'acapIn', 'acapOut', 'intro', 'short', 'acapella', 'instru', 'superShort',
]

# Known styles for parsing
KNOWN_STYLES = [
    'drum and bass', 'hip hop', 'r&b', 'r & b',
    'dance', 'rap', 'pop', 'soul', 'rock', 'jazz',
    'electronic', 'house', 'techno', 'trance', 'dubstep',
    'reggae', 'reggaeton', 'latin', 'country', 'folk', 'blues', 'metal',
    'punk', 'indie', 'alternative', 'ambient', 'classical', 'funk',
    'disco', 'edm', 'trap', 'drill', 'afrobeat', 'dancehall'
]


class PrismaDatabaseService:
    """Service for direct database operations using Prisma."""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._initialized = True
        self._db: Optional[Prisma] = None
        self._connected = False
        
        print(f"🔌 Prisma Database Service initialized")
        database_url = os.environ.get('DATABASE_URL', '')
        if database_url:
            masked_url = re.sub(r':([^:@]+)@', ':****@', database_url)
            print(f"   DATABASE_URL: {masked_url}")
        else:
            print("   ⚠️ DATABASE_URL not set!")
    
    def connect(self) -> bool:
        """Connect to the database."""
        if not PRISMA_AVAILABLE:
            print("❌ Prisma client not available. Run setup commands first.")
            return False
        
        if self._connected and self._db:
            return True
        
        try:
            self._db = Prisma()
            self._db.connect()
            self._connected = True
            print("✅ Prisma connected to database")
            return True
        except Exception as e:
            print(f"❌ Prisma connection failed: {e}")
            traceback.print_exc()
            self._connected = False
            return False
    
    def disconnect(self):
        """Disconnect from the database."""
        if self._db and self._connected:
            try:
                self._db.disconnect()
            except:
                pass
        self._connected = False
    
    @property
    def db(self) -> Prisma:
        """Get the Prisma client, connecting if needed."""
        if not self._connected:
            self.connect()
        return self._db
    
    def sanitize_string(self, value: Any) -> Any:
        """Remove null bytes from strings."""
        if isinstance(value, str):
            return value.replace('\x00', '').strip()
        return value
    
    def extract_base_track_id(self, track_id: str, track_type: Optional[str] = None) -> str:
        """Extract base track ID by removing variant type suffix."""
        if not track_type:
            return track_id
        
        type_suffix = '_' + track_type.replace(' ', '_')
        if track_id.lower().endswith(type_suffix.lower()):
            return track_id[:-len(type_suffix)]
        
        return track_id
    
    def extract_base_title(self, title: str, track_type: Optional[str] = None) -> str:
        """Extract base title by removing variant type suffix."""
        result = title
        
        if track_type:
            dash_pattern = re.compile(rf'\s*-\s*{re.escape(track_type)}\s*$', re.IGNORECASE)
            if dash_pattern.search(result):
                result = dash_pattern.sub('', result).strip()
            else:
                type_suffix = ' ' + track_type
                if result.lower().endswith(type_suffix.lower()):
                    result = result[:-len(type_suffix)].strip()
        
        result = re.sub(r'[\s\-]+$', '', result).strip()
        return result
    
    def parse_multi_value_field(self, value: Optional[str]) -> List[str]:
        """Parse style/mood/univers string."""
        if not value:
            return []
        
        normalized_known = [s.replace(' & ', '&').replace(' ', '_').lower() for s in KNOWN_STYLES]
        result = []
        remaining = value.lower().strip()
        
        if re.search(r'[\/,\|;]+', remaining):
            parts = [p.strip() for p in re.split(r'[\/,\|;]+', remaining) if p.strip()]
        else:
            parts = [remaining]
        
        sorted_styles = sorted(KNOWN_STYLES, key=len, reverse=True)
        
        for part in parts:
            for style in sorted_styles:
                regex = re.compile(style.replace(' ', r'\s*').replace('&', r'\s*&\s*'), re.IGNORECASE)
                match = regex.search(part)
                
                if match:
                    normalized = style.replace(' & ', '&').replace(' ', '_').lower()
                    if normalized not in result:
                        result.append(normalized)
                    part = regex.sub(' ', part).strip()
        
        return [v for v in result if v in normalized_known]
    
    def get_file_field_from_type(self, track_type: Optional[str], format: Optional[str] = None) -> Optional[str]:
        """Map track type to database field name."""
        if not track_type:
            return None
        
        is_wav = format and format.upper() in ('WAV', 'WAVE')
        print(f"   🔍 get_file_field_from_type: type='{track_type}', format='{format}', is_wav={is_wav}")
        
        if track_type.lower() == 'main':
            field = 'trackWav' if is_wav else 'trackFile'
            print(f"   🔍 Main type → {field}")
            return field
        
        if track_type.lower() == 'extended':
            field = 'extendedTrackWave' if is_wav else 'extendedTrackMp3'
            print(f"   🔍 Extended type → {field}")
            return field
        
        base_field = TYPE_TO_FILE_FIELD_MAP.get(track_type)
        if not base_field:
            for key, value in TYPE_TO_FILE_FIELD_MAP.items():
                if key.lower() == track_type.lower():
                    base_field = value
                    break
        
        if not base_field:
            print(f"   🔍 No base_field found for type '{track_type}'")
            return None
        
        # MP3-only types: Acapella, Intro, Instrumental, Short, Super Short — skip WAV
        if is_wav and base_field in MP3_ONLY_FIELDS:
            print(f"   ⏭️ {base_field} is MP3-only — WAV skipped")
            return None
        
        if is_wav and base_field in FIELDS_WITH_WAV_VARIANTS:
            field = f"{base_field}Wav"
            print(f"   🔍 WAV variant: {base_field} → {field}")
            return field
        
        print(f"   🔍 Using base field: {base_field}")
        return base_field
    
    def find_artist_by_name(self, artist_name: str):
        """Find artist by name."""
        if not artist_name:
            return None
        
        try:
            # Exact match first
            artist = self.db.artist.find_first(
                where={'name': {'equals': artist_name, 'mode': 'insensitive'}}
            )
            if artist:
                return artist
            
            # Contains match
            artist = self.db.artist.find_first(
                where={'name': {'contains': artist_name, 'mode': 'insensitive'}}
            )
            return artist
        except Exception as e:
            print(f"   ⚠️ Artist lookup failed: {e}")
            return None
    
    def _split_artist_string(self, artist_name: str) -> List[str]:
        """Split compound artist string into individual names.
        
        'Akon Ft. John Mamann & Dawty Music' → ['Akon', 'John Mamann', 'Dawty Music']
        'A Boogie Wit Da Hoodie & Pnb Rock' → ['A Boogie Wit Da Hoodie', 'Pnb Rock']
        """
        if not artist_name:
            return []
        
        # Normalize separators to |
        s = artist_name
        # "Ft.", "Feat.", "feat.", "ft." "featuring" → separator
        s = re.sub(r'\s+(?:feat\.?|ft\.?|featuring)\s+', '|', s, flags=re.IGNORECASE)
        # " & " → separator (but not inside a name like "R&B")
        s = re.sub(r'\s+&\s+', '|', s)
        # " x " (collab marker) → separator
        s = re.sub(r'\s+x\s+', '|', s, flags=re.IGNORECASE)
        # ", " between artists
        s = re.sub(r'\s*,\s+', '|', s)
        
        parts = [p.strip() for p in s.split('|') if p.strip()]
        return parts
    
    def find_or_create_reference_artist(self, name: str):
        """Find a reference artist by exact name (case-insensitive), or create one."""
        if not name or len(name.strip()) < 2:
            return None
        
        name = name.strip()
        
        try:
            # Exact match (case-insensitive)
            existing = self.db.referenceartist.find_first(
                where={'name': {'equals': name, 'mode': 'insensitive'}}
            )
            if existing:
                return existing
            
            # Create new reference artist with CUID v1 ID (matching Keystone format)
            try:
                import cuid as _cuid
                new_id = _cuid.cuid()
            except ImportError:
                import uuid
                new_id = 'c' + str(uuid.uuid4()).replace('-', '')[:24]
            
            new_artist = self.db.referenceartist.create(data={
                'id': new_id,
                'name': name,
            })
            print(f"   ➕ Created new ReferenceArtist: '{name}' (id={new_id})")
            return new_artist
        except Exception as e:
            print(f"   ⚠️ ReferenceArtist upsert failed for '{name}': {e}")
            return None
    
    def find_reference_artists(self, artist_name: str) -> List:
        """Find or create reference artists from a compound artist string.
        
        Splits 'Akon Ft. John Mamann & Dawty Music' into individual names,
        then finds or creates each one as a ReferenceArtist.
        """
        if not artist_name:
            return []
        
        try:
            individual_names = self._split_artist_string(artist_name)
            if not individual_names:
                return []
            
            print(f"   🔍 Parsed artists: {individual_names}")
            
            results = []
            for name in individual_names:
                artist = self.find_or_create_reference_artist(name)
                if artist:
                    results.append(artist)
            
            return results
        except Exception as e:
            print(f"   ⚠️ Reference artists upsert failed: {e}")
            return []
    
    def find_or_create_album(self, album_name: str, release_date: str, 
                             reference_artists: List, label: Optional[str] = None,
                             sous_label: Optional[str] = None):
        """Find or create an album by name."""
        if not album_name or not album_name.strip():
            return None
        
        try:
            # Find existing album
            album = self.db.album.find_first(
                where={'nomAlbum': {'equals': album_name, 'mode': 'insensitive'}}
            )
            
            if album:
                # Update with missing info
                update_data = {}
                if not album.major and label:
                    update_data['major'] = label
                if not album.sousLabel and sous_label:
                    update_data['sousLabel'] = sous_label
                if not album.releaseDate and release_date:
                    update_data['releaseDate'] = release_date
                
                if update_data:
                    album = self.db.album.update(
                        where={'id': album.id},
                        data=update_data
                    )
                
                # Connect reference artists
                if reference_artists:
                    for artist in reference_artists:
                        try:
                            self.db.album.update(
                                where={'id': album.id},
                                data={'ReferenceArtist': {'connect': [{'id': artist.id}]}}
                            )
                        except:
                            pass
                
                return album
            
            # Create new album
            create_data = {
                'nomAlbum': album_name,
                'releaseDate': release_date or '',
                'sousLabel': sous_label or '',
                'major': label,
            }
            
            if reference_artists:
                create_data['ReferenceArtist'] = {
                    'connect': [{'id': a.id} for a in reference_artists]
                }
            
            album = self.db.album.create(data=create_data)
            return album
            
        except Exception as e:
            print(f"   ⚠️ Album lookup/create failed: {e}")
            return None
    
    def create_or_update_track(self, track_data: Dict[str, Any], skip_waveform: bool = False) -> Dict[str, Any]:
        """Create or update a track using Prisma.
        
        Args:
            track_data: Dictionary containing track metadata
            skip_waveform: If True, skip waveform generation for faster uploads
        """
        try:
            if not self.connect():
                return {'error': 'Database connection failed'}
            
            # Sanitize all string values
            sanitized_data = {k: self.sanitize_string(v) for k, v in track_data.items()}
            
            track_type = sanitized_data.get('Type', '')
            format_type = sanitized_data.get('Format', 'MP3')
            
            print(f"\n{'='*60}")
            print(f"📀 PRISMA: Processing track")
            print(f"   Type: {track_type}")
            print(f"   Format: {format_type}")
            print(f"   Titre: {sanitized_data.get('Titre', 'N/A')}")
            print(f"{'='*60}")
            
            # Get file field from type
            file_field = self.get_file_field_from_type(track_type, format_type)
            if not file_field:
                file_field = 'trackFile'
                print(f"   Using default field: {file_field}")
            
            # Extract base track ID and title
            raw_track_id = sanitized_data.get('TRACK_ID', '')
            if not raw_track_id:
                raw_track_id = re.sub(r'[^\w\s-]', '', sanitized_data.get('Titre', 'unknown'))
                raw_track_id = raw_track_id.replace(' ', '_')
            
            base_track_id = self.extract_base_track_id(raw_track_id, track_type)
            base_title = self.extract_base_title(sanitized_data.get('Titre', ''), track_type)
            
            print(f"   Base Track ID: {base_track_id}")
            print(f"   Base Title: {base_title}")
            print(f"   File Field: {file_field}")
            
            # Parse release date
            date_sortie = sanitized_data.get('Date de sortie', 0)
            if isinstance(date_sortie, (int, float)) and date_sortie > 0:
                release_date = datetime.fromtimestamp(date_sortie).strftime('%Y-%m-%d')
            else:
                release_date = ''
            
            # Parse style/mood/univers
            style = self.parse_multi_value_field(sanitized_data.get('Style', ''))
            mood = self.parse_multi_value_field(sanitized_data.get('Mood', ''))
            univers = self.parse_multi_value_field(sanitized_data.get('Univers', ''))
            
            # Find artist and reference artists
            artist_name = sanitized_data.get('Artiste', '')
            matched_artist = self.find_artist_by_name(artist_name)
            matched_ref_artists = self.find_reference_artists(artist_name)
            
            if matched_artist:
                print(f"   Matched Artist: {matched_artist.name}")
            if matched_ref_artists:
                print(f"   Matched Ref Artists: {[a.name for a in matched_ref_artists]}")
            
            # Find or create album
            album_name = sanitized_data.get('Album', '')
            matched_album = self.find_or_create_album(
                album_name, release_date, matched_ref_artists,
                sanitized_data.get('Label'), sanitized_data.get('Sous-label')
            )
            if matched_album:
                print(f"   Matched Album: {matched_album.nomAlbum}")
            
            # File URL and S3 upload
            file_url = sanitized_data.get('Fichiers', '')
            file_filename = ''
            file_filesize = 0
            
            # Upload file to S3 if configured (matching Keystone's storage pattern)
            if S3_AVAILABLE and file_url:
                try:
                    s3 = get_s3_service()
                    
                    # Check if this is a WAV field
                    wav_fields = [
                        'trackWav', 'originalTrackWave', 'originalTrackWaveClean',
                        'originalTrackWaveDirty', 'extendedTrackWave', 'extendedTrackWaveClean',
                        'extendedTrackWaveDirty', 'clapInMainWav', 'shortMainWav',
                        'shortAcapInWav', 'shortClapInWav', 'acapInAcapOutMainWav',
                        'slamDirtyMainWav', 'shortAcapOutWav', 'clapInShortAcapOutWav',
                        'slamIntroShortAcapOutWav', 'acapInWav', 'acapOutWav',
                        'introWav', 'shortWav', 'acapellaWav', 'instruWav', 'superShortWav',
                    ]
                    is_wav = file_field in wav_fields
                    
                    # Generate filename from title (Keystone stores just the filename)
                    # The Titre field already contains the variant (e.g., "Track Name - Main")
                    full_title = sanitized_data.get('Titre', '')
                    print(f"   📝 Title for filename: '{full_title}'")
                    
                    audio_filename = s3.generate_audio_filename(
                        full_title,
                        None,  # Type is already in title
                        format_type,
                        file_url
                    )
                    
                    # Upload to S3 (stored in tracks/mp3/ or tracks/wav/)
                    print(f"   📤 Uploading to S3 ({'WAV' if is_wav else 'MP3'} folder)...")
                    result = s3.upload_audio_file(
                        source_url=file_url,
                        filename=audio_filename,
                        is_wav=is_wav,
                    )
                    
                    # Store just the filename (like Keystone does)
                    file_filename = result.filename
                    file_filesize = result.filesize
                    print(f"   ✅ S3 Upload complete: {file_filename} ({file_filesize} bytes)")
                    
                except Exception as e:
                    print(f"   ❌ S3 upload failed: {e}")
                    import traceback
                    traceback.print_exc()
                    return {'error': f'S3 upload failed: {e}'}
            else:
                if not S3_AVAILABLE:
                    print(f"   ❌ S3 not configured - cannot upload file")
                    return {'error': 'S3 not configured'}
                if not file_url:
                    print(f"   ❌ No file URL provided")
                    return {'error': 'No file URL provided'}
            
            # Upload cover image to S3 if provided (matching Keystone's image storage)
            cover_image_data = {}
            cover_url = sanitized_data.get('Url', '')
            if S3_AVAILABLE and cover_url and 'idbyrivoli' not in cover_url.lower():
                try:
                    s3 = get_s3_service()
                    print(f"   📤 Uploading cover image...")
                    img_result = s3.upload_image(cover_url)
                    # Keystone stores: {field}_id, {field}_filesize, {field}_width, {field}_height, {field}_extension
                    cover_image_data = {
                        'coverImage_id': img_result.id,
                        'coverImage_filesize': img_result.filesize,
                        'coverImage_width': img_result.width,
                        'coverImage_height': img_result.height,
                        'coverImage_extension': img_result.extension,
                    }
                    print(f"   ✅ Cover uploaded: {img_result.id}.{img_result.extension}")
                except Exception as e:
                    print(f"   ⚠️ Cover upload failed: {e}")
            
            # Check if track exists
            existing_track = self.db.track.find_first(
                where={'trackId': base_track_id},
                include={'Artist': True, 'ReferenceArtist': True, 'Album': True}
            )
            
            if existing_track:
                print(f"   📝 Updating existing track: {existing_track.id}")
                print(f"   📁 Adding file field: {file_field}_filename = '{file_filename}'")
                print(f"   📁 Adding file size: {file_field}_filesize = {file_filesize}")
                
                # Build update data
                update_data = {
                    f'{file_field}_filename': file_filename,
                    f'{file_field}_filesize': file_filesize,
                    'isOriginal': True,
                }
                
                # Add cover image: force replace if _force_cover_replace is set, otherwise only fill if empty
                force_cover = track_data.get('_force_cover_replace', False)
                if cover_image_data and (force_cover or not existing_track.coverImage_id):
                    update_data.update(cover_image_data)
                    if force_cover and existing_track.coverImage_id:
                        print(f"   🖼️ Replacing existing cover (mandatory Deezer cover replacement)")
                
                # Generate waveform for audio fields (Main, Acapella, Intro, Instru, etc.)
                # Map file_field → JSON waveform field name
                waveform_json_field = 'jsonData' if file_field == 'trackFile' else f'{file_field}Json' if file_field else None
                needs_waveform = (
                    not skip_waveform and WAVEFORM_AVAILABLE and file_url and waveform_json_field
                )
                if file_field == 'trackFile':
                    needs_waveform = needs_waveform and (not existing_track.jsonData or not existing_track.duration)
                # For non-main fields, always generate (check if json field is empty via getattr)
                elif waveform_json_field:
                    existing_json = getattr(existing_track, waveform_json_field, None) if hasattr(existing_track, waveform_json_field) else None
                    needs_waveform = needs_waveform and not existing_json
                
                if needs_waveform:
                    try:
                        print(f"   📊 Generating waveform for {file_field}...")
                        waveform_data = generate_waveform_from_url(file_url)
                        if waveform_data:
                            update_data[waveform_json_field] = PrismaJson(waveform_data['waveform'])
                            # Only set duration on trackFile (main track duration)
                            if file_field == 'trackFile':
                                update_data['duration'] = waveform_data['duration']
                            print(f"   ✅ Waveform added to {waveform_json_field}: {len(waveform_data['waveform'])} peaks, {waveform_data['duration']:.2f}s")
                    except Exception as e:
                        print(f"   ⚠️ Waveform generation failed for {file_field}: {e}")
                elif skip_waveform:
                    print(f"   ⏭️ Waveform generation skipped (fast mode)")
                
                # Only update empty fields
                if not existing_track.title:
                    update_data['title'] = base_title
                if not existing_track.originalArtist:
                    update_data['originalArtist'] = sanitized_data.get('Artiste original', '')
                if not existing_track.album:
                    update_data['album'] = album_name
                if not existing_track.format:
                    update_data['format'] = format_type
                if not existing_track.bpm:
                    update_data['bpm'] = sanitized_data.get('BPM', 0) or 0
                if not existing_track.label:
                    update_data['label'] = sanitized_data.get('Label')
                if not existing_track.SousLabel:
                    update_data['SousLabel'] = sanitized_data.get('Sous-label', '')
                if not existing_track.releaseDate:
                    update_data['releaseDate'] = release_date
                if not existing_track.ISRC:
                    update_data['ISRC'] = sanitized_data.get('ISRC', '')
                
                # Connect artist
                if matched_artist:
                    existing_ids = [a.id for a in existing_track.Artist] if existing_track.Artist else []
                    if matched_artist.id not in existing_ids:
                        update_data['Artist'] = {'connect': [{'id': matched_artist.id}]}
                
                # Connect reference artists
                if matched_ref_artists:
                    existing_ids = [a.id for a in existing_track.ReferenceArtist] if existing_track.ReferenceArtist else []
                    to_connect = [a for a in matched_ref_artists if a.id not in existing_ids]
                    if to_connect:
                        update_data['ReferenceArtist'] = {'connect': [{'id': a.id} for a in to_connect]}
                
                # Connect album
                if matched_album:
                    existing_ids = [a.id for a in existing_track.Album] if existing_track.Album else []
                    if matched_album.id not in existing_ids:
                        update_data['Album'] = {'connect': [{'id': matched_album.id}]}
                
                updated_track = self.db.track.update(
                    where={'id': existing_track.id},
                    data=update_data
                )
                
                print(f"   ✅ Track updated: {updated_track.id}")
                return {'trackId': base_track_id, 'id': updated_track.id, 'action': 'updated'}
            
            else:
                print(f"   ➕ Creating new track with trackId: {base_track_id}")
                print(f"   📁 File field: {file_field}_filename = '{file_filename}'")
                print(f"   📁 File size: {file_field}_filesize = {file_filesize}")
                
                # Build create data
                create_data = {
                    'trackId': base_track_id,
                    'title': base_title,
                    'editTitle': sanitized_data.get('Artiste original', ''),
                    'originalArtist': sanitized_data.get('Artiste original', ''),
                    'album': album_name,
                    'format': format_type,
                    'bpm': sanitized_data.get('BPM', 0) or 0,
                    'label': sanitized_data.get('Label'),
                    'SousLabel': sanitized_data.get('Sous-label', ''),
                    'ISRC': sanitized_data.get('ISRC', ''),
                    'releaseDate': release_date,
                    'style': PrismaJson(style) if style else PrismaJson([]),
                    'mood': PrismaJson(mood) if mood else PrismaJson([]),
                    'univers': PrismaJson(univers) if univers else PrismaJson([]),
                    'isOriginal': True,
                    f'{file_field}_filename': file_filename,
                    f'{file_field}_filesize': file_filesize,
                }
                
                # Add cover image data if we uploaded one
                if cover_image_data:
                    create_data.update(cover_image_data)
                
                # Generate waveform for audio fields (Main, Acapella, Intro, Instru, etc.)
                waveform_json_field = 'jsonData' if file_field == 'trackFile' else f'{file_field}Json' if file_field else None
                if not skip_waveform and WAVEFORM_AVAILABLE and file_url and waveform_json_field:
                    try:
                        print(f"   📊 Generating waveform for {file_field}...")
                        waveform_data = generate_waveform_from_url(file_url)
                        if waveform_data:
                            create_data[waveform_json_field] = PrismaJson(waveform_data['waveform'])
                            if file_field == 'trackFile':
                                create_data['duration'] = waveform_data['duration']
                            print(f"   ✅ Waveform added to {waveform_json_field}: {len(waveform_data['waveform'])} peaks, {waveform_data['duration']:.2f}s")
                    except Exception as e:
                        print(f"   ⚠️ Waveform generation failed for {file_field}: {e}")
                elif skip_waveform:
                    print(f"   ⏭️ Waveform generation skipped (fast mode)")
                
                # Connect artist
                if matched_artist:
                    create_data['Artist'] = {'connect': [{'id': matched_artist.id}]}
                
                # Connect reference artists
                if matched_ref_artists:
                    create_data['ReferenceArtist'] = {'connect': [{'id': a.id} for a in matched_ref_artists]}
                
                # Connect album
                if matched_album:
                    create_data['Album'] = {'connect': [{'id': matched_album.id}]}
                
                created_track = self.db.track.create(data=create_data)
                
                print(f"   ✅ Track created: {created_track.id}")
                return {'trackId': base_track_id, 'id': created_track.id, 'action': 'created'}
        
        except Exception as e:
            print(f"❌ Prisma error: {e}")
            traceback.print_exc()
            return {'error': str(e)}
    
    def check_connection(self) -> bool:
        """Test database connectivity."""
        try:
            if not self.connect():
                return False
            # Simple query to test connection
            self.db.track.count()
            return True
        except Exception as e:
            print(f"❌ Connection check failed: {e}")
            return False


# Global instance
_db_service: Optional[PrismaDatabaseService] = None


def get_database_service() -> PrismaDatabaseService:
    """Get the singleton database service instance."""
    global _db_service
    if _db_service is None:
        _db_service = PrismaDatabaseService()
    return _db_service


def save_track_to_database(track_data: Dict[str, Any], skip_waveform: bool = False) -> Dict[str, Any]:
    """Save track data directly to database using Prisma.
    
    Args:
        track_data: Dictionary containing track metadata
        skip_waveform: If True, skip waveform generation for faster uploads
    """
    db = get_database_service()
    return db.create_or_update_track(track_data, skip_waveform=skip_waveform)


def check_database_connection() -> bool:
    """Check if database is accessible."""
    db = get_database_service()
    return db.check_connection()


def get_schema_info() -> Dict[str, Any]:
    """Get database schema information."""
    db = get_database_service()
    try:
        if not db.connect():
            return {'error': 'Connection failed'}
        
        track_count = db.db.track.count()
        artist_count = db.db.artist.count()
        album_count = db.db.album.count()
        
        return {
            'connected': True,
            'track_count': track_count,
            'artist_count': artist_count,
            'album_count': album_count,
        }
    except Exception as e:
        return {'error': str(e)}


def test_database_insert() -> Dict[str, Any]:
    """Test database insert capability."""
    return {'note': 'Use Prisma generate first'}
