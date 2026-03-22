import argparse
import csv
from pathlib import Path
from typing import List, Dict

# Google API imports
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Audio & Matching imports
import mutagen
from rapidfuzz import process, fuzz

# Scopes for YouTube API (Read-only access)
SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]


def get_youtube_client(credentials_file: str):
    """Phase 1: Authenticate and return the YouTube API client."""
    print("Authenticating with Google...")
    flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
    # This will open a browser window for authentication
    creds = flow.run_local_server(port=0)
    return build("youtube", "v3", credentials=creds)


def parse_playlist_csv(csv_path: str) -> List[str]:
    """Extract Video IDs from the Takeout CSV."""
    video_ids = []
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if "Video ID" in row:
                video_ids.append(row["Video ID"])
    print(f"Found {len(video_ids)} Video IDs in CSV.")
    return video_ids


def fetch_youtube_titles(youtube, video_ids: List[str]) -> List[Dict[str, str]]:
    """Phase 2: Resolve Video IDs to Titles via YouTube API in batches of 50."""
    print("Fetching titles from YouTube API...")
    playlist_items = []

    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i : i + 50]
        request = youtube.videos().list(part="snippet", id=",".join(chunk))
        response = request.execute()

        # Map returned IDs to their titles
        id_to_title = {
            item["id"]: item["snippet"]["title"] for item in response.get("items", [])
        }

        # Maintain the original playlist order
        for vid in chunk:
            if vid in id_to_title:
                playlist_items.append({"id": vid, "title": id_to_title[vid]})
            else:
                print(
                    f"Warning: Could not fetch title for Video ID {vid} (Might be deleted/unavailable)"
                )

    return playlist_items


def index_local_music(music_dir: str) -> List[Dict[str, str]]:
    """Phase 3: Scan local directory and extract ID3 titles."""
    print(f"Scanning local music directory: {music_dir}...")
    local_files = []
    supported_exts = ["*.mp3", "*.m4a", "*.flac", "*.ogg", "*.wav"]

    base_path = Path(music_dir)

    for ext in supported_exts:
        for filepath in base_path.rglob(ext):
            try:
                # Use easy=True to standardize standard tags across formats
                audio = mutagen.File(filepath, easy=True)
                title = None

                if audio and "title" in audio:
                    title = audio["title"][0]
                else:
                    # Fallback to filename without extension if no ID3 title exists
                    title = filepath.stem

                album = audio["album"][0] if audio and "album" in audio else None

                tracknumber = None
                if audio and "tracknumber" in audio:
                    try:
                        tracknumber = int(audio["tracknumber"][0].split("/")[0])
                    except (ValueError, IndexError):
                        pass

                local_files.append({"path": str(filepath.absolute()), "title": title, "album": album, "tracknumber": tracknumber})
            except Exception as e:
                print(f"Could not read metadata for {filepath.name}: {e}")

    print(f"Indexed {len(local_files)} local audio files.")
    return local_files


def generate_m3u(
    playlist_items: List[Dict[str, str]],
    local_files: List[Dict[str, str]],
    output_path: str,
    threshold: float,
    sort_playlist: bool = False,
):
    """Phases 4 & 5: Fuzzy match titles and write the M3U file."""
    print("Matching tracks and generating M3U...")

    # Create a dictionary of local titles mapped to their list index for RapidFuzz
    local_titles = {i: f["title"] for i, f in enumerate(local_files)}

    matched = []
    unmatched = []

    for item in playlist_items:
        yt_title = item["title"]

        # Using token_sort_ratio which handles reordered words well (e.g., "Artist - Song" vs "Song")
        match = process.extractOne(
            yt_title, local_titles, scorer=fuzz.token_sort_ratio
        )

        if match and match[1] >= threshold:
            matched.append((yt_title, local_files[match[2]]))
        else:
            unmatched.append(item)

    if sort_playlist:
        def sort_key(item):
            _, local_file = item
            album = local_file.get("album")
            tracknumber = local_file.get("tracknumber")
            album_sort = (0, album.lower()) if album else (1, "")
            track_sort = (0, tracknumber) if tracknumber is not None else (1, 0)
            return album_sort + track_sort

        matched.sort(key=sort_key)

    print("\n--- Summary ---")
    if not matched:
        print(f"No tracks matched — skipping {output_path}")
        return

    with open(output_path, "w", encoding="utf-8") as m3u:
        m3u.write("#EXTM3U\n")
        for yt_title, local_file in matched:
            m3u.write(f"#EXTINF:-1,{yt_title}\n")
            m3u.write(f"{local_file['path']}\n")

    print(
        f"Successfully matched and wrote {len(matched)}/{len(playlist_items)} tracks to {output_path}"
    )

    if unmatched:
        print(f"\nCould not confidently match {len(unmatched)} tracks:")
        for u in unmatched:
            print(f" - {u['title']} (ID: {u['id']})")


def csv_stem_to_m3u_name(csv_path: Path) -> str:
    """Derive the M3U filename from a CSV path, stripping the ' videos' suffix."""
    stem = csv_path.stem
    if stem.endswith(" videos"):
        stem = stem[: -len(" videos")]
    return stem + ".m3u"


def process_csv(youtube_client, csv_path: Path, output_path: Path, music_dir: str, threshold: float, sort_playlist: bool = False):
    """Process a single CSV playlist and write its M3U counterpart."""
    print(f"\nProcessing: {csv_path} -> {output_path}")

    video_ids = parse_playlist_csv(str(csv_path))
    resolved_playlist = fetch_youtube_titles(youtube_client, video_ids)
    local_music_index = index_local_music(music_dir)

    if not local_music_index:
        print("No local files found. Check your --music-dir path.")
    elif not resolved_playlist:
        print("No titles resolved from YouTube API.")
    else:
        generate_m3u(resolved_playlist, local_music_index, str(output_path), threshold, sort_playlist)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert YouTube Music Takeout CSV playlists to M3U files."
    )
    parser.add_argument(
        "--credentials",
        default="api_config.json",
        help="Path to Google API credentials JSON (default: api_config.json)",
    )
    parser.add_argument(
        "--music-dir",
        required=True,
        help="Directory containing local audio files",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=85.0,
        help="Fuzzy match confidence threshold 0-100 (default: 85.0)",
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--input",
        metavar="CSV",
        help="Path to a single input CSV playlist",
    )
    mode.add_argument(
        "--input-folder",
        metavar="FOLDER",
        help="Folder containing multiple CSV playlists to process in batch",
    )

    parser.add_argument(
        "--output",
        metavar="M3U",
        help="Output M3U path for single-file mode (defaults to <csv-stem>.m3u next to the CSV)",
    )
    parser.add_argument(
        "--output-folder",
        metavar="FOLDER",
        help="Folder to write M3U files into for batch mode (defaults to the input folder)",
    )
    parser.add_argument(
        "--sort",
        action="store_true",
        help="Sort playlist by album name then track number (missing fields sort last)",
    )

    args = parser.parse_args()

    youtube_client = get_youtube_client(args.credentials)

    if args.input:
        csv_path = Path(args.input)
        if not csv_path.is_file():
            parser.error(f"Input file not found: {csv_path}")
        out_path = Path(args.output) if args.output else csv_path.parent / csv_stem_to_m3u_name(csv_path)
        process_csv(youtube_client, csv_path, out_path, args.music_dir, args.threshold, args.sort)
    else:
        in_folder = Path(args.input_folder)
        if not in_folder.is_dir():
            parser.error(f"Input folder not found: {in_folder}")
        out_folder = Path(args.output_folder) if args.output_folder else in_folder
        out_folder.mkdir(parents=True, exist_ok=True)

        csv_files = list(in_folder.glob("*.csv"))
        if not csv_files:
            parser.error(f"No CSV files found in: {in_folder}")

        for csv_path in csv_files:
            out_path = out_folder / csv_stem_to_m3u_name(csv_path)
            process_csv(youtube_client, csv_path, out_path, args.music_dir, args.threshold, args.sort)
