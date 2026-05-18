#!/usr/bin/env python3
"""
ShadowDownloader v2.0
WordPress Uploads Mass Downloader
"""

import argparse
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

import requests
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def format_size(size_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


# ---------------------------------------------------------------------------
# Core downloader
# ---------------------------------------------------------------------------

class ShadowDownloader:
    # File extensions we care about
    FILE_EXTENSIONS = (
        "jpg", "jpeg", "png", "gif", "bmp", "webp",
        "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx",
        "zip", "rar", "7z", "tar", "gz",
        "mp4", "mp3", "avi", "mov", "wmv", "flv",
        "txt", "log", "csv", "json", "xml",
        "sql", "backup", "bak",
    )

    # Single compiled pattern (case-insensitive, no redundancy)
    _EXT_PATTERN = re.compile(
        r'href=["\']([^"\']+\.(?:' +
        "|".join(FILE_EXTENSIONS) +
        r'))["\']',
        re.IGNORECASE,
    )

    def __init__(
        self,
        base_url: str,
        output_dir: str | None = None,
        threads: int = 10,
        timeout: int = 30,
        delay: float = 0.0,
        user_agent: str | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.threads = threads
        self.timeout = timeout
        self.delay = delay          # seconds between requests (rate-limit courtesy)

        # Thread-safe result tracking
        self._lock = threading.Lock()
        self.downloaded_files: list[dict] = []
        self.failed_downloads: list[dict] = []

        # Output directory (resolved lazily for single-file mode)
        self._output_dir = output_dir

        # Session
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": user_agent or (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": self.base_url,
        })

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def output_dir(self) -> str:
        if self._output_dir is None:
            domain = urlparse(self.base_url).netloc
            self._output_dir = f"downloads_{domain}_{int(time.time())}"
        return self._output_dir

    # ------------------------------------------------------------------
    # Internal network helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, stream: bool = False) -> requests.Response | None:
        """GET with error handling and optional delay."""
        try:
            if self.delay:
                time.sleep(self.delay)
            return self.session.get(url, stream=stream, timeout=self.timeout)
        except requests.RequestException as exc:
            print(f"❌ GET failed [{url}]: {exc}")
            return None

    def _head(self, url: str) -> int:
        """Return status code from HEAD request, or 0 on error."""
        try:
            if self.delay:
                time.sleep(self.delay)
            return self.session.head(url, timeout=self.timeout).status_code
        except requests.RequestException:
            return 0

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def _parse_links(self, url: str) -> list[str]:
        """Return absolute file URLs found in a directory listing page."""
        resp = self._get(url)
        if resp is None or resp.status_code != 200:
            return []

        found = []
        for match in self._EXT_PATTERN.finditer(resp.text):
            href = match.group(1)
            if href in ("../", "./") or href.startswith("?") or href.endswith("/"):
                continue
            full_url = urljoin(url, href)
            found.append(full_url)

        return list(dict.fromkeys(found))   # deduplicate, preserve order

    def _discover_directories(self, base_url: str) -> list[str]:
        """Probe common year/month sub-directories under *base_url*."""
        print("🔍 Scanning year/month sub-directories...")

        years  = [str(y) for y in range(2018, 2026)]
        months = [f"{m:02d}" for m in range(1, 13)]

        dirs: list[str] = []

        for year in years:
            year_url = f"{base_url}/{year}/"
            if self._head(year_url) == 200:
                dirs.append(year_url)
                print(f"  ├─ 📁 {year}/")

                for month in months:
                    month_url = f"{year_url}{month}/"
                    if self._head(month_url) == 200:
                        dirs.append(month_url)
                        print(f"  │  └─ 📂 {year}/{month}/")

        print(f"  └─ Found {len(dirs)} sub-directories")
        return dirs

    def discover_files(self) -> list[str]:
        """Return every unique file URL reachable under self.base_url."""
        print("\n🔍 Starting file discovery...")

        all_files: list[str] = []

        # Base directory
        base_files = self._parse_links(self.base_url)
        all_files.extend(base_files)
        print(f"  ├─ Base directory : {len(base_files)} file(s)")

        # Year/month sub-directories
        sub_dirs = self._discover_directories(self.base_url)
        sub_total = 0
        for d in sub_dirs:
            files = self._parse_links(d)
            all_files.extend(files)
            sub_total += len(files)

        if sub_total:
            print(f"  ├─ Sub-directories: {sub_total} file(s)")

        unique = list(dict.fromkeys(all_files))
        print(f"  └─ Total unique   : {len(unique)} file(s)\n")
        return unique

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def _download_one(self, file_url: str, local_path: str) -> bool:
        """Download a single file. Thread-safe."""
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            resp = self._get(file_url, stream=True)
            if resp is None:
                raise RuntimeError("No response")
            resp.raise_for_status()

            with open(local_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        fh.write(chunk)

            size = os.path.getsize(local_path)
            info = {"url": file_url, "local_path": local_path,
                    "size": size, "timestamp": time.time()}

            with self._lock:
                self.downloaded_files.append(info)

            print(f"  ✅ {os.path.basename(local_path):<50} {format_size(size):>10}")
            return True

        except Exception as exc:
            with self._lock:
                self.failed_downloads.append({"url": file_url, "error": str(exc)})
            print(f"  ❌ {os.path.basename(local_path):<50} {str(exc)[:30]}")
            return False

    # ------------------------------------------------------------------
    # Public entry-points
    # ------------------------------------------------------------------

    def download_single(self, file_url: str, custom_filename: str | None = None) -> bool:
        """Download one file to the current directory (or self._output_dir if set)."""
        base_dir = self._output_dir or os.getcwd()
        os.makedirs(base_dir, exist_ok=True)

        if custom_filename:
            local_path = os.path.join(base_dir, custom_filename)
        else:
            filename = os.path.basename(urlparse(file_url).path) or \
                       f"file_{int(time.time())}"
            local_path = os.path.join(base_dir, filename)

        print(f"\n🎯 Single-file download")
        print(f"   URL  : {file_url}")
        print(f"   Save : {local_path}\n")

        success = self._download_one(file_url, local_path)
        print(f"\n{'✅ Done' if success else '❌ Failed'}: {local_path}")
        return success

    def download_all(self) -> None:
        """Discover and mass-download every file under self.base_url."""
        files = self.discover_files()

        if not files:
            print("❌ No files found.")
            return

        os.makedirs(self.output_dir, exist_ok=True)

        # Build (url, local_path) pairs
        tasks: list[tuple[str, str]] = []
        for url in files:
            rel = urlparse(url).path.split("/wp-content/uploads/", 1)[-1].lstrip("/")
            local = os.path.join(self.output_dir, rel)
            tasks.append((url, local))

        print(f"📦 {len(tasks)} file(s) queued  →  {self.output_dir}\n")

        ok_count = 0
        try:
            with ThreadPoolExecutor(max_workers=self.threads) as pool:
                futures = {pool.submit(self._download_one, u, p): (u, p)
                           for u, p in tasks}

                with tqdm(total=len(futures), desc="📥 Downloading", unit="file") as bar:
                    for future in as_completed(futures):
                        try:
                            if future.result():
                                ok_count += 1
                        except Exception as exc:
                            url, _ = futures[future]
                            print(f"💥 Unexpected error [{url}]: {exc}")
                        finally:
                            bar.update(1)

        except KeyboardInterrupt:
            print("\n⏹️  Interrupted by user")
            return

        self._print_summary(len(tasks), ok_count)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def _print_summary(self, total: int, ok: int) -> None:
        total_bytes = sum(f["size"] for f in self.downloaded_files)
        print("\n" + "─" * 60)
        print("📊 SUMMARY")
        print("─" * 60)
        print(f"  Target     : {self.base_url}")
        print(f"  Output dir : {self.output_dir}")
        print(f"  Total      : {total}")
        print(f"  ✅ OK      : {ok}")
        print(f"  ❌ Failed  : {len(self.failed_downloads)}")
        print(f"  💾 Data    : {format_size(total_bytes)}")

        if self.failed_downloads:
            print(f"\n⚠️  Failed downloads:")
            for item in self.failed_downloads:
                print(f"    └─ {os.path.basename(item['url'])}  →  {item['url']}")
        print("─" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _banner(target: str, output: str | None, threads: int) -> None:
    print("┌─────────────────────────────────────────────────────────┐")
    print("│             WP Content Upload Downloader v2.0           │")
    print("│                        by Rbayl                         │")
    print("└─────────────────────────────────────────────────────────┘")
    print(f"  🔗 Target  : {target}")
    if output:
        print(f"  📁 Output  : {output}")
    print(f"  🔢 Threads : {threads}")
    print("─" * 60)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="ShadowDownloader v2.0 — WordPress Uploads Downloader"
    )
    p.add_argument("url", help="URL of wp-content/uploads/ directory OR a single file URL")
    p.add_argument("-o", "--output", help="Output directory (default: auto-generated)")
    p.add_argument("-t", "--threads", type=int, default=10,
                   help="Concurrent downloads (default: 10)")
    p.add_argument("--timeout", type=int, default=30,
                   help="Request timeout in seconds (default: 30)")
    p.add_argument("--delay", type=float, default=0.0,
                   help="Delay between requests in seconds (default: 0)")
    p.add_argument("-st", "--single-target", action="store_true",
                   help="Download only the specified URL (single file)")
    p.add_argument("-name", "--filename",
                   help="Custom filename for single-target mode")
    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    _banner(args.url, args.output, args.threads)

    try:
        dl = ShadowDownloader(
            base_url=args.url,
            output_dir=args.output,
            threads=args.threads,
            timeout=args.timeout,
            delay=args.delay,
        )

        if args.single_target:
            dl.download_single(args.url, args.filename)
        else:
            dl.download_all()

    except KeyboardInterrupt:
        print("\n⏹️  Interrupted by user")
    except Exception as exc:
        print(f"💥 Fatal error: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
