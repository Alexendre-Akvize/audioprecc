"""
S3 Service for ID By Rivoli
Uploads audio files and images to S3 storage matching Keystone's storage pattern.

Keystone stores files as:
- Audio files: tracks/mp3/{filename} or tracks/wav/{filename}
  Database stores: {field}_filename = just the filename, {field}_filesize = size in bytes
- Images: tracks/cover/{id}.{extension}
  Database stores: {field}_id, {field}_filesize, {field}_width, {field}_height, {field}_extension
"""

import os
import re
import uuid
import requests
from typing import Optional, Dict, Any
from dataclasses import dataclass
from io import BytesIO

# Try to import boto3 for S3
try:
    import boto3
    from botocore.config import Config
    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False
    print("⚠️ boto3 not installed. Run: pip install boto3")

# S3 Configuration from environment
S3_BUCKET = os.environ.get('S3_BUCKET', '')
S3_REGION = os.environ.get('S3_REGION', os.environ.get('AWS_REGION', 'eu-north-1'))
S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY', os.environ.get('AWS_ACCESS_KEY_ID', ''))
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY', os.environ.get('AWS_SECRET_ACCESS_KEY', ''))
# Optional: Only needed for S3-compatible services like DigitalOcean Spaces
S3_ENDPOINT = os.environ.get('S3_ENDPOINT', '')

# S3 base paths (matching Keystone configuration)
AUDIO_MP3_PATH = 'tracks/mp3'
AUDIO_WAV_PATH = 'tracks/wav'
IMAGE_PATH = 'tracks/cover'


@dataclass
class AudioUploadResult:
    """Result of audio file upload - matches Keystone's file storage pattern."""
    filename: str  # Just the filename, stored in {field}_filename
    filesize: int  # Size in bytes, stored in {field}_filesize


@dataclass
class ImageUploadResult:
    """Result of image upload - matches Keystone's image storage pattern."""
    id: str         # UUID, stored in {field}_id
    filesize: int   # Size in bytes, stored in {field}_filesize
    width: int      # Image width, stored in {field}_width
    height: int     # Image height, stored in {field}_height
    extension: str  # File extension (jpg, png, etc.), stored in {field}_extension


class S3Service:
    """Service for S3 file operations matching Keystone's storage pattern."""
    
    def __init__(self):
        self._client = None
        self._configured = False
        
        if not S3_AVAILABLE:
            print("❌ S3 Service: boto3 not available")
            return
        
        if not all([S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY]):
            print("⚠️ S3 Service: Missing configuration")
            print(f"   S3_BUCKET: {'✓' if S3_BUCKET else '✗ NOT SET'}")
            print(f"   S3_ACCESS_KEY: {'✓' if S3_ACCESS_KEY else '✗ NOT SET'}")
            print(f"   S3_SECRET_KEY: {'✓' if S3_SECRET_KEY else '✗ NOT SET'}")
            return
        
        try:
            # Configure S3 client for AWS S3
            client_kwargs = {
                'aws_access_key_id': S3_ACCESS_KEY,
                'aws_secret_access_key': S3_SECRET_KEY,
                'region_name': S3_REGION,
            }
            
            # Only add endpoint_url for S3-compatible services
            if S3_ENDPOINT:
                client_kwargs['endpoint_url'] = S3_ENDPOINT
            
            self._client = boto3.client('s3', **client_kwargs)
            
            self._configured = True
            print(f"✅ S3 Service configured (AWS S3)")
            print(f"   Bucket: {S3_BUCKET}")
            print(f"   Region: {S3_REGION}")
        except Exception as e:
            print(f"❌ S3 Service initialization failed: {e}")
    
    @property
    def is_configured(self) -> bool:
        return self._configured
    
    def sanitize_filename(self, name: str) -> str:
        """Sanitize a string to be safe for use as a filename."""
        result = name
        # Replace characters not allowed in filenames
        result = re.sub(r'[<>:"/\\|?*]', '', result)
        # Replace multiple spaces with single space
        result = re.sub(r'\s+', ' ', result)
        # Remove trailing dashes and separators
        result = re.sub(r'[\s\-]+$', '', result)
        return result.strip()
    
    def get_file_extension(self, format_type: Optional[str], url: str) -> str:
        """Determine file extension from format or URL."""
        if format_type:
            fmt = format_type.lower()
            if fmt in ('mp3', 'wav', 'wave', 'flac', 'aiff', 'aif', 'm4a', 'ogg'):
                return 'wav' if fmt == 'wave' else fmt
        
        # Try to extract from URL
        try:
            from urllib.parse import urlparse, unquote, parse_qs
            parsed = urlparse(url)
            
            # Check for download_file endpoint
            if 'download_file' in parsed.path:
                query = parse_qs(parsed.query)
                if 'path' in query:
                    path_value = unquote(query['path'][0])
                    if '.' in path_value:
                        ext = path_value.rsplit('.', 1)[-1].lower()
                        if ext in ('mp3', 'wav', 'wave', 'flac'):
                            return 'wav' if ext == 'wave' else ext
            
            # Regular URL path
            path = unquote(parsed.path)
            if '.' in path:
                ext = path.rsplit('.', 1)[-1].lower()
                if ext in ('mp3', 'wav', 'wave', 'flac', 'aiff', 'aif', 'm4a', 'ogg'):
                    return 'wav' if ext == 'wave' else ext
        except:
            pass
        
        return 'mp3'  # Default
    
    def extract_filename_from_url(self, url: str) -> str:
        """Extract the actual filename from a URL (handles download_file endpoints)."""
        try:
            from urllib.parse import urlparse, unquote, parse_qs
            parsed = urlparse(url)
            
            # Check if it's a download_file endpoint with path parameter
            if 'download_file' in parsed.path:
                query = parse_qs(parsed.query)
                if 'path' in query:
                    path_value = unquote(query['path'][0])
                    # Extract filename from path (e.g., "Track Name/Track Name - Main.mp3")
                    if '/' in path_value:
                        return path_value.split('/')[-1]
                    return path_value
            
            # Regular URL - get last path segment
            path = unquote(parsed.path)
            if '/' in path:
                filename = path.split('/')[-1]
                if filename:
                    return filename
        except Exception as e:
            print(f"   ⚠️ Could not extract filename from URL: {e}")
        
        return ''
    
    def generate_audio_filename(self, title: str, track_type: Optional[str], format_type: Optional[str], url: str) -> str:
        """
        Generate filename for audio file matching Keystone's pattern.
        Format: {title}.{extension} or {title} ({type}).{extension}
        """
        extension = self.get_file_extension(format_type, url)
        
        # Try to get filename from title first
        base_name = ''
        
        if title and title.strip():
            base_name = title.strip()
        else:
            # Try to extract from URL
            extracted = self.extract_filename_from_url(url)
            if extracted:
                # Remove extension from extracted filename
                if '.' in extracted:
                    base_name = extracted.rsplit('.', 1)[0]
                else:
                    base_name = extracted
        
        # Fallback to UUID if still no name
        if not base_name:
            base_name = f"track_{uuid.uuid4().hex[:12]}"
        
        # Sanitize the filename
        sanitized = self.sanitize_filename(base_name)
        
        if not sanitized:
            sanitized = f"track_{uuid.uuid4().hex[:12]}"
        
        filename = f"{sanitized}.{extension}"
        print(f"   📝 Generated filename: {filename}")
        return filename
    
    def download_file(self, url: str) -> tuple[bytes, int]:
        """Download a file from URL and return bytes and size."""
        print(f"   📥 Downloading: {url[:100]}...")
        
        response = requests.get(url, timeout=300, stream=True)
        response.raise_for_status()
        
        content = response.content
        size = len(content)
        
        print(f"   📥 Downloaded: {size / 1024 / 1024:.2f} MB")
        return content, size
    
    def upload_audio_file(
        self,
        source_url: str,
        filename: str,
        is_wav: bool = False,
    ) -> AudioUploadResult:
        """
        Download audio file from URL and upload to S3.
        
        Args:
            source_url: URL to download the file from
            filename: Target filename (e.g., "Track Name - Main.mp3")
            is_wav: True for WAV files (tracks/wav/), False for MP3 (tracks/mp3/)
        
        Returns:
            AudioUploadResult with filename and filesize (matching Keystone's pattern)
        """
        if not self._configured:
            raise Exception("S3 not configured")
        
        # Download the file
        content, filesize = self.download_file(source_url)
        
        # Determine S3 path based on file type
        base_path = AUDIO_WAV_PATH if is_wav else AUDIO_MP3_PATH
        s3_key = f"{base_path}/{filename}"
        
        # Determine content type
        extension = filename.rsplit('.', 1)[-1].lower() if '.' in filename else 'mp3'
        content_types = {
            'mp3': 'audio/mpeg',
            'wav': 'audio/wav',
            'flac': 'audio/flac',
            'aiff': 'audio/aiff',
            'm4a': 'audio/mp4',
        }
        content_type = content_types.get(extension, 'audio/mpeg')
        
        print(f"   📤 Uploading to S3: {s3_key}")
        
        # Upload to S3
        self._client.upload_fileobj(
            BytesIO(content),
            S3_BUCKET,
            s3_key,
            ExtraArgs={
                'ContentType': content_type,
                'ACL': 'public-read',
            },
        )
        
        print(f"   ✅ Uploaded: {filename} ({filesize / 1024:.1f} KB)")
        
        # Return just the filename (not the full path) - this is what Keystone stores
        return AudioUploadResult(
            filename=filename,
            filesize=filesize,
        )
    
    def upload_image(self, source_url: str) -> ImageUploadResult:
        """
        Download image from URL and upload to S3.
        
        Args:
            source_url: URL to download the image from
        
        Returns:
            ImageUploadResult with id, filesize, dimensions, extension (matching Keystone's pattern)
        """
        if not self._configured:
            raise Exception("S3 not configured")
        
        # Download the file
        content, filesize = self.download_file(source_url)
        
        # Generate unique ID for the image (Keystone uses UUID)
        image_id = uuid.uuid4().hex
        
        # Determine extension from URL or content
        extension = 'jpg'
        try:
            from urllib.parse import urlparse, unquote
            path = unquote(urlparse(source_url).path)
            if '.' in path:
                ext = path.rsplit('.', 1)[-1].lower()
                if ext in ('jpg', 'jpeg', 'png', 'gif', 'webp'):
                    extension = 'jpg' if ext == 'jpeg' else ext
        except:
            pass
        
        # Try to get image dimensions
        width, height = 0, 0
        try:
            from PIL import Image
            img = Image.open(BytesIO(content))
            width, height = img.size
        except Exception as e:
            print(f"   ⚠️ Could not get image dimensions: {e}")
        
        # S3 key: tracks/cover/{id}.{extension}
        filename = f"{image_id}.{extension}"
        s3_key = f"{IMAGE_PATH}/{filename}"
        
        # Determine content type
        content_types = {
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'png': 'image/png',
            'gif': 'image/gif',
            'webp': 'image/webp',
        }
        
        print(f"   📤 Uploading image to S3: {s3_key}")
        
        self._client.upload_fileobj(
            BytesIO(content),
            S3_BUCKET,
            s3_key,
            ExtraArgs={
                'ContentType': content_types.get(extension, 'image/jpeg'),
                'ACL': 'public-read',
            },
        )
        
        print(f"   ✅ Uploaded image: {filename} ({width}x{height})")
        
        # Return data matching Keystone's image storage pattern
        return ImageUploadResult(
            id=image_id,
            filesize=filesize,
            width=width,
            height=height,
            extension=extension,
        )


# Global instance
_s3_service: Optional[S3Service] = None


def get_s3_service() -> S3Service:
    """Get the singleton S3 service instance."""
    global _s3_service
    if _s3_service is None:
        _s3_service = S3Service()
    return _s3_service
