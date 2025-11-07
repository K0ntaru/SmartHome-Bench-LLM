#!/usr/bin/env python3
"""
Script to download videos from YouTube URLs listed in Video_url.csv
Only downloads videos that are not marked as "privacy url"
Supports concurrent downloads for faster processing
"""

import os
import csv
import subprocess
import sys
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Configuration
CSV_FILE = "Videos/Video_url.csv"
OUTPUT_DIR = "Videos/Trim_Videos/raw_video"
MAX_RETRIES = 3
DEFAULT_MAX_WORKERS = 8  # Default number of concurrent downloads

# Thread-safe counters
progress_lock = Lock()
success_count = 0
skip_count = 0
fail_count = 0
completed_count = 0

def check_ytdlp():
    """Check if yt-dlp is installed"""
    try:
        subprocess.run(["yt-dlp", "--version"], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

def install_ytdlp():
    """Install yt-dlp if not available"""
    print("yt-dlp not found. Installing...")
    try:
        # Try pip3 first, then pip
        for pip_cmd in ["pip3", "pip"]:
            try:
                subprocess.run([pip_cmd, "install", "yt-dlp"], check=True, capture_output=True)
                print("yt-dlp installed successfully!")
                return True
            except (subprocess.CalledProcessError, FileNotFoundError):
                continue
        
        # Fallback to python -m pip
        subprocess.run([sys.executable, "-m", "pip", "install", "yt-dlp"], check=True)
        print("yt-dlp installed successfully!")
        return True
    except subprocess.CalledProcessError:
        print("Failed to install yt-dlp. Please install it manually:")
        print("  pip3 install yt-dlp")
        print("  or")
        print("  pip install yt-dlp")
        return False

def create_output_dir():
    """Create output directory if it doesn't exist"""
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {os.path.abspath(OUTPUT_DIR)}")

def read_video_urls():
    """Read video URLs from CSV file"""
    videos = []
    try:
        # Use utf-8-sig to handle BOM (Byte Order Mark)
        with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Handle both 'Title' and possible variations
                title_key = 'Title' if 'Title' in row else 'title'
                url_key = 'URL' if 'URL' in row else 'url'
                
                if title_key not in row or url_key not in row:
                    print(f"Warning: Skipping row with missing columns: {row}")
                    continue
                
                title = row[title_key].strip()
                url = row[url_key].strip()
                
                # Skip empty rows
                if not title or not url:
                    continue
                
                # Skip privacy URLs
                if url.lower() == "privacy url" or not url.startswith("http"):
                    continue
                
                videos.append({
                    'title': title,
                    'url': url
                })
        return videos
    except FileNotFoundError:
        print(f"Error: {CSV_FILE} not found!")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def video_exists(title):
    """Check if video file already exists"""
    # Check for common video extensions
    extensions = ['.mp4', '.mkv', '.webm', '.flv']
    for ext in extensions:
        video_path = os.path.join(OUTPUT_DIR, f"{title}{ext}")
        if os.path.exists(video_path):
            return True
    return False

def download_video(title, url, total_videos, retry_count=0):
    """Download a single video using yt-dlp"""
    global success_count, fail_count, completed_count
    
    output_path = os.path.join(OUTPUT_DIR, f"{title}.%(ext)s")
    
    # yt-dlp command with options
    cmd = [
        "yt-dlp",
        "-o", output_path,
        "--no-playlist",
        "--format", "best[ext=mp4]/best",  # Prefer mp4, fallback to best available
        "--no-warnings",
        "--quiet",  # Reduce output noise in concurrent mode
        url
    ]
    
    try:
        with progress_lock:
            current = completed_count + 1
        print(f"[{current}/{total_videos}] Downloading: {title}", flush=True)
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=1800  # 30 minutes timeout per video
        )
        
        # Find the actual downloaded file
        downloaded_file = None
        for ext in ['.mp4', '.mkv', '.webm', '.flv']:
            potential_file = os.path.join(OUTPUT_DIR, f"{title}{ext}")
            if os.path.exists(potential_file):
                downloaded_file = potential_file
                break
        
        if downloaded_file:
            with progress_lock:
                success_count += 1
                completed_count += 1
            print(f"[{current}/{total_videos}] ✓ Successfully downloaded: {title}", flush=True)
            return True
        else:
            with progress_lock:
                fail_count += 1
                completed_count += 1
            print(f"[{current}/{total_videos}] ✗ File not found after download: {title}", flush=True)
            return False
            
    except subprocess.TimeoutExpired:
        with progress_lock:
            current = completed_count + 1
        if retry_count < MAX_RETRIES:
            print(f"[{current}/{total_videos}] ⚠ Timeout, retry {retry_count + 1}/{MAX_RETRIES} for {title}", flush=True)
            return download_video(title, url, total_videos, retry_count + 1)
        else:
            with progress_lock:
                fail_count += 1
                completed_count += 1
            print(f"[{current}/{total_videos}] ✗ Timeout after {MAX_RETRIES} retries: {title}", flush=True)
            return False
    except subprocess.CalledProcessError as e:
        with progress_lock:
            current = completed_count + 1
        if retry_count < MAX_RETRIES:
            print(f"[{current}/{total_videos}] ⚠ Retry {retry_count + 1}/{MAX_RETRIES} for {title}", flush=True)
            return download_video(title, url, total_videos, retry_count + 1)
        else:
            with progress_lock:
                fail_count += 1
                completed_count += 1
            error_msg = e.stderr[:100] if e.stderr else str(e)
            print(f"[{current}/{total_videos}] ✗ Failed to download {title}: {error_msg}", flush=True)
            return False
    except Exception as e:
        with progress_lock:
            current = completed_count + 1
            fail_count += 1
            completed_count += 1
        print(f"[{current}/{total_videos}] ✗ Error downloading {title}: {e}", flush=True)
        return False

def download_video_wrapper(video_info, total_videos):
    """Wrapper function for concurrent downloads"""
    title = video_info['title']
    url = video_info['url']
    
    # Check if video already exists
    if video_exists(title):
        global skip_count, completed_count
        with progress_lock:
            skip_count += 1
            completed_count += 1
            current = completed_count
        print(f"[{current}/{total_videos}] ⏭ Skipping {title} (already exists)", flush=True)
        return {'title': title, 'status': 'skipped'}
    
    # Download video
    success = download_video(title, url, total_videos)
    return {'title': title, 'status': 'success' if success else 'failed'}

def main():
    """Main function"""
    global success_count, skip_count, fail_count, completed_count
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Download videos from YouTube URLs listed in Video_url.csv',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  python3 download_videos.py              # Use default {DEFAULT_MAX_WORKERS} concurrent downloads
  python3 download_videos.py -j 8       # Use 8 concurrent downloads
  python3 download_videos.py --workers 2  # Use 2 concurrent downloads
        """
    )
    parser.add_argument(
        '-j', '--workers', '--max-workers',
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f'Number of concurrent downloads (default: {DEFAULT_MAX_WORKERS})'
    )
    args = parser.parse_args()
    
    max_workers = args.workers
    if max_workers < 1:
        print("Error: Number of workers must be at least 1")
        sys.exit(1)
    
    print("=" * 60)
    print("SmartHome-Bench Video Downloader")
    print("=" * 60)
    print(f"Concurrent downloads: {max_workers}")
    
    # Check and install yt-dlp
    if not check_ytdlp():
        if not install_ytdlp():
            sys.exit(1)
    
    # Create output directory
    create_output_dir()
    
    # Read video URLs
    print(f"\nReading video URLs from {CSV_FILE}...")
    videos = read_video_urls()
    print(f"Found {len(videos)} videos to download\n")
    
    # Reset counters
    success_count = 0
    skip_count = 0
    fail_count = 0
    completed_count = 0
    
    # Download videos concurrently
    if max_workers == 1:
        # Sequential download (original behavior)
        print("Downloading videos sequentially...\n")
        for video in videos:
            result = download_video_wrapper(video, len(videos))
    else:
        # Concurrent download
        print(f"Downloading videos with {max_workers} concurrent workers...\n")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all download tasks
            future_to_video = {
                executor.submit(download_video_wrapper, video, len(videos)): video
                for video in videos
            }
            
            # Process completed downloads
            for future in as_completed(future_to_video):
                try:
                    result = future.result()
                except Exception as e:
                    video = future_to_video[future]
                    with progress_lock:
                        fail_count += 1
                        completed_count += 1
                    print(f"Exception downloading {video['title']}: {e}")
    
    # Summary
    print("\n" + "=" * 60)
    print("Download Summary:")
    print(f"  Successfully downloaded: {success_count}")
    print(f"  Skipped (already exists): {skip_count}")
    print(f"  Failed: {fail_count}")
    print(f"  Total: {len(videos)}")
    print("=" * 60)

if __name__ == "__main__":
    main()

