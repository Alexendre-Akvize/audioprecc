#!/usr/bin/env python3
"""
Fetch missing metadata for ReferenceArtist records.

Fills the following fields when empty:
  - image          (from Deezer artist picture → S3)
  - description    (from Wikipedia biography)
  - city           (from MusicBrainz)
  - country        (from MusicBrainz)
  - flag           (emoji flag derived from country code)
  - topSongs       (from Deezer top tracks, format: "Song 1, Song 2, Song 3")
  - popularity     (from Deezer nb_fan)
  - styles         (from Deezer genres + MusicBrainz tags, JSON array)

APIs used (all free, no auth required):
  - Deezer     → image, popularity, top songs, genres
  - Wikipedia  → biography / description
  - MusicBrainz → country, city, genre tags

Usage:
    python fetch_artist_metadata.py                  # Process all artists missing data
    python fetch_artist_metadata.py --dry-run        # Preview what would be updated
    python fetch_artist_metadata.py --limit 50       # Process only 50 artists
    python fetch_artist_metadata.py --name "Drake"   # Process a specific artist by name
    python fetch_artist_metadata.py --force          # Re-fetch even if fields already filled
"""

import os
import re
import sys
import time
import json
import argparse
import unicodedata
from typing import Optional, Dict, Any, List
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Prisma & S3 imports
# ---------------------------------------------------------------------------
try:
    from prisma import Prisma
    from prisma import Json as PrismaJson
    PRISMA_AVAILABLE = True
except ImportError:
    PRISMA_AVAILABLE = False
    print("ERROR: Prisma not available. Run: pip install prisma && prisma generate")
    sys.exit(1)

try:
    from s3_service import get_s3_service
    _s3 = get_s3_service()
    S3_AVAILABLE = _s3.is_configured
except ImportError:
    S3_AVAILABLE = False

# ---------------------------------------------------------------------------
# Constants & rate limits
# ---------------------------------------------------------------------------
DEEZER_DELAY = 0.15        # ~6-7 req/s (well within 50 req/5s)
WIKIPEDIA_DELAY = 0.1
MUSICBRAINZ_DELAY = 1.1    # MusicBrainz enforces 1 req/s

USER_AGENT = "IDByRivoli/1.0 (artist-metadata-fetcher; contact@idbyrivoli.com)"

# Country code → emoji flag
def country_code_to_flag(code: str) -> str:
    """Convert a 2-letter ISO country code to its emoji flag."""
    if not code or len(code) != 2:
        return ""
    code = code.upper()
    return "".join(chr(0x1F1E6 + ord(c) - ord("A")) for c in code)

# Country code → full country name (common ones)
COUNTRY_NAMES = {
    "US": "United States", "GB": "United Kingdom", "FR": "France", "DE": "Germany",
    "CA": "Canada", "AU": "Australia", "JP": "Japan", "KR": "South Korea",
    "BR": "Brazil", "MX": "Mexico", "ES": "Spain", "IT": "Italy", "NL": "Netherlands",
    "SE": "Sweden", "NO": "Norway", "DK": "Denmark", "BE": "Belgium", "CH": "Switzerland",
    "AT": "Austria", "PT": "Portugal", "IE": "Ireland", "NZ": "New Zealand",
    "JM": "Jamaica", "TT": "Trinidad and Tobago", "CO": "Colombia", "AR": "Argentina",
    "CL": "Chile", "PE": "Peru", "VE": "Venezuela", "CU": "Cuba", "PR": "Puerto Rico",
    "DO": "Dominican Republic", "HT": "Haiti", "NG": "Nigeria", "GH": "Ghana",
    "ZA": "South Africa", "KE": "Kenya", "SN": "Senegal", "CI": "Ivory Coast",
    "CM": "Cameroon", "CD": "DR Congo", "MA": "Morocco", "DZ": "Algeria",
    "TN": "Tunisia", "EG": "Egypt", "IL": "Israel", "TR": "Turkey", "IN": "India",
    "PK": "Pakistan", "BD": "Bangladesh", "CN": "China", "TW": "Taiwan",
    "PH": "Philippines", "TH": "Thailand", "ID": "Indonesia", "MY": "Malaysia",
    "SG": "Singapore", "RU": "Russia", "UA": "Ukraine", "PL": "Poland",
    "RO": "Romania", "GR": "Greece", "HU": "Hungary", "CZ": "Czech Republic",
    "FI": "Finland", "HR": "Croatia", "RS": "Serbia", "BG": "Bulgaria",
    "SK": "Slovakia", "LT": "Lithuania", "LV": "Latvia", "EE": "Estonia",
    "IS": "Iceland", "LU": "Luxembourg", "MT": "Malta", "CY": "Cyprus",
    "GE": "Georgia", "AM": "Armenia", "AZ": "Azerbaijan", "KZ": "Kazakhstan",
    "UZ": "Uzbekistan", "AE": "United Arab Emirates", "SA": "Saudi Arabia",
    "QA": "Qatar", "KW": "Kuwait", "BH": "Bahrain", "OM": "Oman",
    "LB": "Lebanon", "JO": "Jordan", "IQ": "Iraq", "IR": "Iran",
    "AF": "Afghanistan", "MM": "Myanmar", "VN": "Vietnam", "LA": "Laos",
    "KH": "Cambodia", "NP": "Nepal", "LK": "Sri Lanka", "MN": "Mongolia",
    "XK": "Kosovo", "BA": "Bosnia and Herzegovina", "ME": "Montenegro",
    "MK": "North Macedonia", "AL": "Albania", "SI": "Slovenia",
}


# =========================================================================
#  DEEZER API
# =========================================================================
def _normalize(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s).lower())
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9\s]", "", s)
    return " ".join(s.split())


def search_deezer_artist(artist_name: str, timeout: int = 10) -> Optional[Dict]:
    """Search Deezer for an artist. Returns best-matching artist dict or None."""
    if not artist_name:
        return None
    try:
        clean_name = re.sub(r'\s*(feat\.?|ft\.?|featuring)\s+.*$', '', artist_name, flags=re.IGNORECASE).strip()
        resp = requests.get("https://api.deezer.com/search/artist",
                            params={"q": clean_name, "limit": 5}, timeout=timeout)
        if resp.status_code != 200:
            return None

        data = resp.json().get("data", [])
        if not data:
            return None

        query_norm = _normalize(clean_name)
        best, best_score = None, -1.0

        for artist in data:
            candidate_norm = _normalize(artist.get("name", ""))
            if candidate_norm == query_norm:
                return artist
            words_q = set(query_norm.split())
            words_c = set(candidate_norm.split())
            score = len(words_q & words_c) / len(words_q | words_c) if (words_q and words_c) else 0.0
            if query_norm in candidate_norm or candidate_norm in query_norm:
                score = max(score, 0.8)
            if score > best_score:
                best_score = score
                best = artist

        return best if best_score >= 0.5 else None
    except Exception as e:
        print(f"    [!] Deezer search error: {e}")
        return None


def get_deezer_artist_image(deezer_artist: Dict) -> Optional[str]:
    """Get highest-res image URL (skip Deezer default placeholder)."""
    url = deezer_artist.get("picture_xl") or deezer_artist.get("picture_big") or deezer_artist.get("picture_medium")
    if url and "user" not in url.split("/")[-1]:
        return url
    return None


def get_deezer_top_tracks(deezer_artist_id: int, limit: int = 3, timeout: int = 10) -> List[str]:
    """Fetch top tracks for a Deezer artist. Returns list of track titles."""
    try:
        resp = requests.get(f"https://api.deezer.com/artist/{deezer_artist_id}/top",
                            params={"limit": limit}, timeout=timeout)
        if resp.status_code != 200:
            return []
        tracks = resp.json().get("data", [])
        return [t.get("title", "") for t in tracks if t.get("title")]
    except Exception:
        return []


def get_deezer_artist_genres(deezer_artist: Dict, timeout: int = 10) -> List[str]:
    """Fetch genres for a Deezer artist via their top track's album."""
    try:
        # Get top track to find genre
        resp = requests.get(f"https://api.deezer.com/artist/{deezer_artist['id']}/top",
                            params={"limit": 1}, timeout=timeout)
        if resp.status_code != 200:
            return []
        tracks = resp.json().get("data", [])
        if not tracks:
            return []

        # Get album details for genre info
        album_id = tracks[0].get("album", {}).get("id")
        if not album_id:
            return []

        time.sleep(DEEZER_DELAY)
        album_resp = requests.get(f"https://api.deezer.com/album/{album_id}", timeout=timeout)
        if album_resp.status_code != 200:
            return []

        genres = album_resp.json().get("genres", {}).get("data", [])
        return [g.get("name", "") for g in genres if g.get("name")]
    except Exception:
        return []


# =========================================================================
#  MUSICBRAINZ API
# =========================================================================
def search_musicbrainz_artist(artist_name: str, timeout: int = 10) -> Optional[Dict]:
    """
    Search MusicBrainz for an artist.
    Returns dict with: country (code), city, tags (genres list).
    """
    if not artist_name:
        return None

    try:
        clean_name = re.sub(r'\s*(feat\.?|ft\.?|featuring)\s+.*$', '', artist_name, flags=re.IGNORECASE).strip()
        resp = requests.get(
            "https://musicbrainz.org/ws/2/artist/",
            params={"query": f'artist:"{clean_name}"', "fmt": "json", "limit": 5},
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
        )
        if resp.status_code != 200:
            return None

        artists = resp.json().get("artists", [])
        if not artists:
            return None

        # Find best match
        query_norm = _normalize(clean_name)
        best_artist = None
        best_score = -1.0

        for mb_artist in artists:
            candidate_norm = _normalize(mb_artist.get("name", ""))
            if candidate_norm == query_norm:
                best_artist = mb_artist
                break
            words_q = set(query_norm.split())
            words_c = set(candidate_norm.split())
            score = len(words_q & words_c) / len(words_q | words_c) if (words_q and words_c) else 0.0
            if query_norm in candidate_norm or candidate_norm in query_norm:
                score = max(score, 0.8)
            if score > best_score:
                best_score = score
                best_artist = mb_artist

        if not best_artist or (best_score < 0.5 and _normalize(best_artist.get("name", "")) != query_norm):
            return None

        # Extract data
        result = {"country_code": "", "country": "", "city": "", "tags": []}

        # Country
        country_code = best_artist.get("country", "")
        if country_code:
            result["country_code"] = country_code
            result["country"] = COUNTRY_NAMES.get(country_code, country_code)

        # City: from begin-area or area
        begin_area = best_artist.get("begin-area", {})
        area = best_artist.get("area", {})
        if begin_area and begin_area.get("name"):
            result["city"] = begin_area["name"]
        elif area and area.get("name") and area.get("type") == "City":
            result["city"] = area["name"]

        # If no country from top-level, try the area
        if not result["country_code"] and area:
            # Walk up area to find country
            area_type = area.get("type", "")
            if area_type == "Country":
                result["country"] = area.get("name", "")
            # Could also check area ISO codes if available

        # Tags/genres
        tags = best_artist.get("tags", [])
        if tags:
            # Sort by count (most popular first), take top tags
            sorted_tags = sorted(tags, key=lambda t: t.get("count", 0), reverse=True)
            result["tags"] = [t["name"].lower() for t in sorted_tags[:8] if t.get("name")]

        return result

    except Exception as e:
        print(f"    [!] MusicBrainz error: {e}")
        return None


# =========================================================================
#  WIKIPEDIA API
# =========================================================================
def _normalize_for_compare(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s).lower())
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9\s]", "", s)
    return " ".join(s.split())


def _fuzzy_match(a: str, b: str) -> bool:
    def _norm(s):
        s = unicodedata.normalize("NFKD", s.lower())
        s = "".join(c for c in s if not unicodedata.combining(c))
        return re.sub(r"[^a-z0-9]", "", s)
    return _norm(a) == _norm(b)


def _is_article_about_artist(extract: str, page_title: str, artist_name: str) -> bool:
    """Validate that a Wikipedia article is actually about the searched artist."""
    extract_lower = extract.lower()
    title_lower = page_title.lower()

    # Reject known bad patterns
    reject_patterns = [
        r"^this is a list of", r"^the following is a",
        r"notable events in .* that took place", r"^this article",
        r"may refer to:", r"can refer to:", r"is a disambiguation",
        r"^\d{4} in music", r"^\d{4} was a", r"^in music,? \d{4}",
    ]
    for p in reject_patterns:
        if re.search(p, extract_lower):
            return False

    title_reject = [r"^\d{4} in ", r"^list of ", r"^glossary of ", r"^index of ", r"^timeline of "]
    for p in title_reject:
        if re.search(p, title_lower):
            return False

    # Artist name must appear in title or extract
    artist_norm = _normalize_for_compare(artist_name)
    extract_norm = _normalize_for_compare(extract)
    title_norm = _normalize_for_compare(page_title)

    title_matches = _fuzzy_match(artist_name.lower(), title_lower) or artist_norm in title_norm or title_norm in artist_norm
    name_in_extract = artist_norm in extract_norm
    significant_part = False
    for part in artist_norm.split():
        if len(part) >= 3 and part in extract_norm:
            significant_part = True
            break

    if not title_matches and not name_in_extract and not significant_part:
        return False

    # Must have person/music indicators
    person_indicators = [
        "singer", "rapper", "musician", "songwriter", "producer", "composer",
        "dj", "disc jockey", "vocalist", "frontman", "frontwoman",
        "born ", "known professionally as", "known by", "stage name",
        "recording artist", "record label", "solo artist",
        "chanteur", "chanteuse", "rappeur", "rappeuse", "musicien", "musicienne",
        "artiste", "compositeur", "compositrice", "producteur", "productrice",
        "né le ", "née le ", "de son vrai nom",
    ]
    if any(ind in extract_lower for ind in person_indicators):
        return True

    band_indicators = ["band", "duo", "trio", "quartet", "group", "ensemble", "groupe"]
    if title_matches and any(ind in extract_lower for ind in band_indicators):
        return True

    if title_matches and len(extract) > 200:
        return True

    return False


def fetch_wikipedia_bio(artist_name: str, timeout: int = 10) -> Optional[str]:
    """Fetch artist biography from Wikipedia (EN + FR fallback)."""
    if not artist_name:
        return None

    search_variants = [
        artist_name,
        artist_name.replace(" ", "_"),
        f"{artist_name} (musician)",
        f"{artist_name} (rapper)",
        f"{artist_name} (singer)",
        f"{artist_name} (DJ)",
    ]

    for lang in ["en", "fr"]:
        for variant in search_variants:
            try:
                url = f"https://{lang}.wikipedia.org/api/rest_v1/page/summary/{requests.utils.quote(variant)}"
                resp = requests.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
                if resp.status_code != 200:
                    continue

                data = resp.json()
                if data.get("type") == "disambiguation":
                    continue
                extract = data.get("extract", "").strip()
                page_title = data.get("title", "")
                if not extract or len(extract) < 50:
                    continue

                if _is_article_about_artist(extract, page_title, artist_name):
                    return extract
            except Exception:
                continue
            time.sleep(WIKIPEDIA_DELAY)

    # Fallback: Wikipedia search API
    for lang in ["en", "fr"]:
        try:
            resp = requests.get(
                f"https://{lang}.wikipedia.org/w/api.php",
                params={
                    "action": "query", "list": "search", "format": "json", "srlimit": 5,
                    "srsearch": f'"{artist_name}" musician OR rapper OR singer OR DJ OR producer',
                },
                timeout=timeout, headers={"User-Agent": USER_AGENT},
            )
            if resp.status_code != 200:
                continue
            for result in resp.json().get("query", {}).get("search", []):
                title = result.get("title", "")
                if not title:
                    continue
                summary_resp = requests.get(
                    f"https://{lang}.wikipedia.org/api/rest_v1/page/summary/{requests.utils.quote(title)}",
                    timeout=timeout, headers={"User-Agent": USER_AGENT},
                )
                if summary_resp.status_code != 200:
                    continue
                data = summary_resp.json()
                extract = data.get("extract", "").strip()
                page_title = data.get("title", "")
                if extract and len(extract) >= 50 and _is_article_about_artist(extract, page_title, artist_name):
                    return extract
                time.sleep(WIKIPEDIA_DELAY)
        except Exception:
            continue

    return None


# =========================================================================
#  S3 UPLOAD
# =========================================================================
def upload_artist_image_to_s3(image_url: str) -> Optional[Dict]:
    """Download artist image and upload to S3. Returns Keystone-compatible image fields."""
    if not S3_AVAILABLE:
        print("    [!] S3 not configured — cannot upload image")
        return None
    try:
        s3 = get_s3_service()
        result = s3.upload_image(image_url)
        return {
            "id": result.id,
            "filesize": result.filesize,
            "width": result.width,
            "height": result.height,
            "extension": result.extension,
        }
    except Exception as e:
        print(f"    [!] S3 upload failed: {e}")
        return None


# =========================================================================
#  STYLE NORMALIZATION
# =========================================================================
# Normalize genre strings to match database convention (lowercase, underscores)
STYLE_NORMALIZE_MAP = {
    "hip hop": "hip_hop", "hip-hop": "hip_hop", "hip hop/rap": "hip_hop",
    "r&b": "r&b", "rnb": "r&b", "rhythm and blues": "r&b",
    "drum and bass": "drum_and_bass", "drum & bass": "drum_and_bass", "dnb": "drum_and_bass",
    "electronic": "electronic", "electro": "electronic",
    "dance": "dance", "edm": "edm",
    "rap": "rap", "rap/hip hop": "rap",
    "pop": "pop", "rock": "rock", "jazz": "jazz", "soul": "soul",
    "house": "house", "techno": "techno", "trance": "trance", "dubstep": "dubstep",
    "reggae": "reggae", "reggaeton": "reggaeton", "latin": "latin",
    "country": "country", "folk": "folk", "blues": "blues", "metal": "metal",
    "punk": "punk", "indie": "indie", "alternative": "alternative",
    "ambient": "ambient", "classical": "classical", "funk": "funk",
    "disco": "disco", "trap": "trap", "drill": "drill",
    "afrobeat": "afrobeat", "afrobeats": "afrobeat",
    "dancehall": "dancehall", "afro": "afrobeat",
    "k-pop": "k-pop", "j-pop": "j-pop",
}


def normalize_styles(raw_genres: List[str]) -> List[str]:
    """Normalize genre strings to the database style format."""
    seen = set()
    result = []
    for genre in raw_genres:
        key = genre.strip().lower()
        normalized = STYLE_NORMALIZE_MAP.get(key, key.replace(" ", "_").replace("-", "_"))
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result[:6]  # Cap at 6 styles


# =========================================================================
#  MAIN PROCESSING
# =========================================================================
def fetch_and_update_artists(
    dry_run: bool = False,
    limit: Optional[int] = None,
    name_filter: Optional[str] = None,
    force: bool = False,
):
    """Main function: fetch and update missing artist metadata."""

    print("=" * 70)
    print("  FETCH REFERENCE ARTIST METADATA")
    print("  Fields: image, description, city, country, flag,")
    print("          topSongs, popularity, styles")
    print("=" * 70)
    print(f"  Mode:       {'DRY RUN' if dry_run else 'LIVE (will update database)'}")
    print(f"  Limit:      {limit or 'all'}")
    print(f"  Force:      {'yes (re-fetch all)' if force else 'no (skip filled fields)'}")
    print(f"  S3:         {'available' if S3_AVAILABLE else 'NOT configured (images skipped)'}")
    if name_filter:
        print(f"  Filter:     {name_filter}")
    print("=" * 70)

    db = Prisma()
    db.connect()
    print("\n[+] Connected to database")

    # Build query: find artists that are missing ANY metadata field
    if name_filter:
        where_filter = {"name": {"contains": name_filter, "mode": "insensitive"}}
    elif force:
        where_filter = {}
    else:
        where_filter = {
            "OR": [
                {"image_id": None},
                {"description": {"equals": ""}},
                {"country": {"equals": ""}},
                {"city": {"equals": ""}},
                {"topSongs": {"equals": ""}},
                {"popularity": None},
                {"flag": {"equals": ""}},
            ]
        }

    total = db.referenceartist.count(where=where_filter)
    print(f"[+] Found {total} ReferenceArtist(s) to process")

    if total == 0:
        print("[+] All artists are complete!")
        db.disconnect()
        return

    take = min(limit, total) if limit else total
    artists = db.referenceartist.find_many(where=where_filter, order={"name": "asc"}, take=take)
    print(f"[+] Processing {len(artists)} artist(s)...\n")

    stats = {
        "processed": 0, "updated": 0, "skipped": 0, "errors": 0,
        "image": 0, "description": 0, "city": 0, "country": 0,
        "flag": 0, "topSongs": 0, "popularity": 0, "styles": 0,
    }

    for i, artist in enumerate(artists):
        stats["processed"] += 1

        # Determine which fields need filling
        needs = {}
        needs["image"] = force or not artist.image_id
        needs["description"] = force or not artist.description or artist.description.strip() == ""
        needs["city"] = force or not artist.city or artist.city.strip() == ""
        needs["country"] = force or not artist.country or artist.country.strip() == ""
        needs["flag"] = force or not artist.flag or artist.flag.strip() == ""
        needs["topSongs"] = force or not artist.topSongs or artist.topSongs.strip() == ""
        needs["popularity"] = force or artist.popularity is None
        needs["styles"] = force or artist.styles is None or artist.styles == [] or artist.styles == "[]"

        needed_fields = [k for k, v in needs.items() if v]
        if not needed_fields:
            stats["skipped"] += 1
            continue

        print(f"[{i+1}/{len(artists)}] {artist.name}")
        print(f"    needs: {', '.join(needed_fields)}")

        update_data = {}
        deezer_artist = None

        # ── DEEZER: image, popularity, topSongs, genres ──────────────
        deezer_genres = []
        if any(needs[f] for f in ["image", "popularity", "topSongs", "styles"]):
            print(f"    Searching Deezer...")
            deezer_artist = search_deezer_artist(artist.name)
            time.sleep(DEEZER_DELAY)

            if deezer_artist:
                dz_name = deezer_artist.get("name", "")
                dz_fans = deezer_artist.get("nb_fan", 0)
                print(f"    -> Deezer match: {dz_name} ({dz_fans:,} fans)")

                # Image
                if needs["image"]:
                    img_url = get_deezer_artist_image(deezer_artist)
                    if img_url:
                        if not dry_run and S3_AVAILABLE:
                            img_data = upload_artist_image_to_s3(img_url)
                            if img_data:
                                update_data["image_id"] = img_data["id"]
                                update_data["image_filesize"] = img_data["filesize"]
                                update_data["image_width"] = img_data["width"]
                                update_data["image_height"] = img_data["height"]
                                update_data["image_extension"] = img_data["extension"]
                                print(f"    -> Image uploaded: {img_data['id']}.{img_data['extension']} ({img_data['width']}x{img_data['height']})")
                        elif dry_run:
                            print(f"    -> [DRY] Would upload image: {img_url[:70]}...")
                    else:
                        print(f"    -> No usable image (Deezer placeholder)")

                # Popularity
                if needs["popularity"] and dz_fans:
                    update_data["popularity"] = dz_fans
                    print(f"    -> Popularity: {dz_fans:,}")

                # Top songs
                if needs["topSongs"]:
                    time.sleep(DEEZER_DELAY)
                    top_tracks = get_deezer_top_tracks(deezer_artist["id"], limit=3)
                    if top_tracks:
                        update_data["topSongs"] = ", ".join(top_tracks)
                        print(f"    -> Top songs: {update_data['topSongs']}")
                    else:
                        print(f"    -> No top songs found")

                # Genres from Deezer
                if needs["styles"]:
                    time.sleep(DEEZER_DELAY)
                    deezer_genres = get_deezer_artist_genres(deezer_artist)
                    if deezer_genres:
                        print(f"    -> Deezer genres: {deezer_genres}")
            else:
                print(f"    -> No Deezer match")

        # ── MUSICBRAINZ: country, city, tags ─────────────────────────
        mb_data = None
        if any(needs[f] for f in ["country", "city", "flag", "styles"]):
            print(f"    Searching MusicBrainz...")
            time.sleep(MUSICBRAINZ_DELAY)
            mb_data = search_musicbrainz_artist(artist.name)

            if mb_data:
                print(f"    -> MusicBrainz match: country={mb_data.get('country_code', '?')}, city={mb_data.get('city', '?')}, tags={mb_data.get('tags', [])[:5]}")

                if needs["country"] and mb_data.get("country"):
                    update_data["country"] = mb_data["country"]
                    print(f"    -> Country: {mb_data['country']}")

                if needs["city"] and mb_data.get("city"):
                    update_data["city"] = mb_data["city"]
                    print(f"    -> City: {mb_data['city']}")

                if needs["flag"] and mb_data.get("country_code"):
                    flag = country_code_to_flag(mb_data["country_code"])
                    if flag:
                        update_data["flag"] = flag
                        print(f"    -> Flag: {flag}")
            else:
                print(f"    -> No MusicBrainz match")

        # Combine styles from Deezer genres + MusicBrainz tags
        if needs["styles"]:
            raw_genres = list(deezer_genres)  # from Deezer (may be empty)
            if mb_data and mb_data.get("tags"):
                raw_genres.extend(mb_data["tags"])

            if raw_genres:
                styles = normalize_styles(raw_genres)
                if styles:
                    update_data["styles"] = PrismaJson(styles)
                    print(f"    -> Styles: {styles}")

        # ── WIKIPEDIA: description ───────────────────────────────────
        if needs["description"]:
            print(f"    Searching Wikipedia...")
            bio = fetch_wikipedia_bio(artist.name)
            if bio:
                if len(bio) > 2000:
                    bio = bio[:1997] + "..."
                update_data["description"] = bio
                print(f"    -> Bio found ({len(bio)} chars)")
            else:
                print(f"    -> No bio found")

        # ── UPDATE DATABASE ──────────────────────────────────────────
        if update_data and not dry_run:
            try:
                db.referenceartist.update(where={"id": artist.id}, data=update_data)
                stats["updated"] += 1
                for key in update_data:
                    field = key.split("_")[0] if key.startswith("image_") else key
                    if field in stats:
                        stats[field] = stats.get(field, 0) + 1
                    elif key == "image_id":
                        stats["image"] += 1
                print(f"    -> DB updated: {[k for k in update_data.keys() if not k.startswith('image_') or k == 'image_id']}")
            except Exception as e:
                stats["errors"] += 1
                print(f"    [!] DB update failed: {e}")
        elif update_data and dry_run:
            stats["updated"] += 1
            print(f"    -> [DRY] Would update: {[k for k in update_data.keys() if not k.startswith('image_') or k == 'image_id']}")
        else:
            print(f"    -> Nothing found to update")

        print()

    # ── SUMMARY ──────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    print(f"  Processed:    {stats['processed']}")
    print(f"  Updated:      {stats['updated']}")
    print(f"  Skipped:      {stats['skipped']}")
    print(f"  Errors:       {stats['errors']}")
    print(f"  ---")
    print(f"  Images:       {stats['image']}")
    print(f"  Descriptions: {stats['description']}")
    print(f"  Countries:    {stats['country']}")
    print(f"  Cities:       {stats['city']}")
    print(f"  Flags:        {stats['flag']}")
    print(f"  Top Songs:    {stats['topSongs']}")
    print(f"  Popularity:   {stats['popularity']}")
    print(f"  Styles:       {stats['styles']}")
    print("=" * 70)

    db.disconnect()
    print("\n[+] Done!")


# =========================================================================
#  CLI
# =========================================================================
def main():
    parser = argparse.ArgumentParser(description="Fetch missing ReferenceArtist metadata from Deezer, Wikipedia & MusicBrainz")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing to DB or S3")
    parser.add_argument("--limit", type=int, default=None, help="Max number of artists to process")
    parser.add_argument("--name", type=str, default=None, help="Process a specific artist by name (partial match)")
    parser.add_argument("--force", action="store_true", help="Re-fetch all fields even if already filled")
    args = parser.parse_args()

    fetch_and_update_artists(
        dry_run=args.dry_run,
        limit=args.limit,
        name_filter=args.name,
        force=args.force,
    )


if __name__ == "__main__":
    main()
