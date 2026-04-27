import os
import re
import json
import time
import hashlib
import sqlite3
import threading
import datetime as dt
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote_plus, urlparse
import queue

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

import tkinter as tk
from tkinter import ttk, messagebox, filedialog


APP_NAME = "GreyNOC PDF Scraper"
BRAND = "GreyNOC"
DEFAULT_DOWNLOAD_DIR = r"C:\Users\bsoul\OneDrive\Desktop\AiFace\Manual_pdfs"

APP_DIR = Path(__file__).resolve().parent
STOP_FLAG = APP_DIR / "STOP_GREYNOC_SCRAPER.flag"
DB_FILE = APP_DIR / "greynoc_pdf_scraper.sqlite3"
LOG_FILE = APP_DIR / "greynoc_pdf_scraper.log"
PROFILE_FILE = APP_DIR / "keyword_profiles.json"

USER_AGENT = (
    "GreyNOCPDFCollector/1.0 "
    "(open-access PDF collector; contact: local-user)"
)

LOW_VALUE_TERMS = [
    "call for papers", "conference program", "poster", "slides",
    "newsletter", "advertisement", "brochure", "syllabus",
    "course schedule", "table of contents", "index", "errata",
    "correction", "presentation",
]

HIGH_VALUE_TERMS = [
    "survey", "review", "systematic review", "benchmark", "dataset",
    "architecture", "framework", "method", "empirical", "experimental",
    "evaluation", "analysis", "security", "model", "algorithm", "theory",
]

DEFAULT_PROFILES = {
    "Core AI": [
        "artificial intelligence", "machine learning", "deep learning",
        "neural networks", "transformers", "large language models",
    ],
    "Advanced AI": [
        "self supervised learning", "multimodal models", "generative ai",
        "diffusion models", "ai alignment", "cognitive architectures",
    ],
    "Cybersecurity": [
        "penetration testing", "network security", "exploit development",
        "malware analysis", "reverse engineering", "vulnerability research",
    ],
    "Cloud DevOps": [
        "cloud computing", "kubernetes architecture", "distributed systems",
        "microservices", "infrastructure as code", "site reliability engineering",
    ],
    "GreyNOC Physics": [
        "quantum field theory", "unified field theory",
        "wave resonance physics", "harmonic oscillations",
        "spacetime curvature", "quantum gravity",
    ],
    "Complex Systems": [
        "complex systems theory", "nonlinear dynamics", "chaos theory",
        "information theory", "emergent systems", "systems biology",
    ],
    "Robotics": [
        "robotics control systems", "autonomous navigation", "sensor fusion",
        "computer vision robotics", "human robot interaction",
    ],
    "Cognitive Science": [
        "cognitive science", "human learning models", "decision making theory",
        "neuroscience computation", "perception modeling",
    ],
    "Finance Strategy": [
        "algorithmic trading", "financial modeling", "economic forecasting",
        "game theory", "market dynamics",
    ],
    "Biomedical": [
        "genomics", "proteomics", "computational biology",
        "medical imaging", "clinical trials", "epidemiology",
    ],
}

load_dotenv(APP_DIR / ".env")


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}


def clean_keyword_list(raw_keywords):
    cleaned = []
    seen = set()
    for item in raw_keywords:
        keyword = str(item).strip()
        if not keyword:
            continue
        key = keyword.lower()
        if key not in seen:
            cleaned.append(keyword)
            seen.add(key)
    return cleaned


def load_profiles():
    if PROFILE_FILE.exists():
        try:
            with open(PROFILE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and data:
                cleaned = {}
                for name, keywords in data.items():
                    name = str(name).strip()
                    if not name:
                        continue
                    if isinstance(keywords, list):
                        clean_keywords = clean_keyword_list(keywords)
                    else:
                        clean_keywords = clean_keyword_list(str(keywords).split(","))
                    if clean_keywords:
                        cleaned[name] = clean_keywords
                if cleaned:
                    return cleaned
        except Exception:
            pass
    return DEFAULT_PROFILES.copy()


def save_profiles(profiles):
    cleaned = {}
    for name, keywords in profiles.items():
        name = str(name).strip()
        clean_keywords = clean_keyword_list(keywords)
        if name and clean_keywords:
            cleaned[name] = clean_keywords

    with open(PROFILE_FILE, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, indent=2, ensure_ascii=False)

    return cleaned


def is_valid_pdf(path: Path) -> bool:
    try:
        if not path.exists() or path.stat().st_size < 1024:
            return False
        with open(path, "rb") as f:
            return f.read(5) == b"%PDF-"
    except Exception:
        return False


def hash_file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 256), b""):
            h.update(chunk)
    return h.hexdigest()


class DailyLimiter:
    def __init__(self, max_downloads: int, max_bytes: int):
        self.max_downloads = max_downloads
        self.max_bytes = max_bytes

    def get_usage(self, conn: sqlite3.Connection):
        today = dt.date.today().isoformat()
        row = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(size_bytes), 0) FROM downloads WHERE date(downloaded_at)=?",
            (today,),
        ).fetchone()
        return int(row[0]), int(row[1] or 0)

    def can_download(self, conn: sqlite3.Connection, next_size: int = 0):
        count, bytes_used = self.get_usage(conn)
        if count >= self.max_downloads:
            return False, f"daily download limit reached: {count}/{self.max_downloads}"
        if bytes_used + next_size > self.max_bytes:
            mb_used = bytes_used / 1024 / 1024
            mb_limit = self.max_bytes / 1024 / 1024
            return False, f"daily size limit reached: {mb_used:.2f}/{mb_limit:.2f} MB"
        return True, "ok"


class OutputFolderScanner:
    def __init__(self, download_dir: Path):
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.baseline = self.snapshot()

    def snapshot(self):
        snap = {}
        self.download_dir.mkdir(parents=True, exist_ok=True)
        for path in self.download_dir.rglob("*.pdf"):
            try:
                stat = path.stat()
                snap[str(path.resolve())] = {
                    "size": stat.st_size,
                    "mtime": stat.st_mtime,
                    "valid": is_valid_pdf(path),
                }
            except Exception:
                pass
        return snap

    def scan(self):
        current = self.snapshot()
        new_or_changed = []
        invalid = []

        for file_path, meta in current.items():
            old = self.baseline.get(file_path)
            if old is None or old.get("size") != meta.get("size") or old.get("mtime") != meta.get("mtime"):
                new_or_changed.append((file_path, meta))
            if not meta.get("valid"):
                invalid.append((file_path, meta))

        total_size = sum(meta["size"] for meta in current.values())

        return {
            "folder": str(self.download_dir),
            "total_pdfs": len(current),
            "total_size_bytes": total_size,
            "new_or_changed": new_or_changed,
            "invalid": invalid,
            "all": current,
        }

    def confirm_file(self, path: Path):
        path = Path(path)
        try:
            correct_folder = (
                self.download_dir.resolve() in path.resolve().parents
                or path.resolve().parent == self.download_dir.resolve()
            )
        except Exception:
            correct_folder = False

        return {
            "path": str(path),
            "exists": path.exists(),
            "correct_folder": correct_folder,
            "valid_pdf": is_valid_pdf(path),
            "size_bytes": path.stat().st_size if path.exists() else 0,
        }


class QualityFilter:
    def __init__(self, keywords, min_score=35):
        self.keywords = [k.lower().strip() for k in keywords if k.strip()]
        self.min_score = int(min_score)

    def score(self, item):
        score = 0
        reasons = []

        title = (item.get("title") or "").lower()
        abstract = (item.get("abstract") or "").lower()
        url = (item.get("url") or "").lower()
        source = (item.get("source") or "").lower()
        doi = item.get("doi")
        cited_by = int(item.get("cited_by_count") or 0)
        year = item.get("year")

        if "openalex" in source:
            score += 12; reasons.append("+12 OpenAlex metadata")
        if "arxiv" in source:
            score += 10; reasons.append("+10 arXiv source")
        if "pubmed" in source or "pmc" in source:
            score += 11; reasons.append("+11 PubMed Central source")
        if "pdf" in url:
            score += 8; reasons.append("+8 direct PDF URL")
        if doi:
            score += 8; reasons.append("+8 DOI")
        if abstract:
            score += 8; reasons.append("+8 abstract/metadata")

        if year:
            try:
                y = int(year)
                current = dt.date.today().year
                if y >= current - 3:
                    score += 12; reasons.append("+12 recent")
                elif y >= current - 8:
                    score += 7; reasons.append("+7 fairly recent")
                elif y < 1995:
                    score -= 10; reasons.append("-10 very old")
            except Exception:
                pass

        if cited_by >= 500:
            score += 20; reasons.append("+20 highly cited")
        elif cited_by >= 100:
            score += 14; reasons.append("+14 well cited")
        elif cited_by >= 25:
            score += 8; reasons.append("+8 cited")
        elif cited_by > 0:
            score += 3; reasons.append("+3 some citations")

        text_blob = f"{title} {abstract}"

        keyword_hits = sum(1 for k in self.keywords if k and k in text_blob)
        if keyword_hits:
            add = min(20, keyword_hits * 8)
            score += add
            reasons.append(f"+{add} keyword relevance")

        for term in HIGH_VALUE_TERMS:
            if term in text_blob:
                score += 4
                reasons.append(f"+4 high-value term: {term}")
                break

        low_hits = [term for term in LOW_VALUE_TERMS if term in text_blob]
        if low_hits:
            penalty = min(30, 10 * len(low_hits))
            score -= penalty
            reasons.append(f"-{penalty} low-value terms")

        if len(title) < 12:
            score -= 15
            reasons.append("-15 weak title")

        if any(bad in url for bad in ["slides", "poster", "program", "brochure"]):
            score -= 20
            reasons.append("-20 weak URL")

        return score, reasons

    def accept(self, item):
        score, reasons = self.score(item)
        return score >= self.min_score, score, reasons


class PDFScraper:
    def __init__(self, download_dir: Path, log_queue: queue.Queue,
                 max_downloads: int, max_mb: int, min_quality_score: int,
                 sources_enabled: dict):
        self.download_dir = Path(download_dir)
        self.log_queue = log_queue
        self.headers = {"User-Agent": USER_AGENT}
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.limiter = DailyLimiter(max_downloads=max_downloads, max_bytes=max_mb * 1024 * 1024)
        self.min_quality_score = min_quality_score
        self.sources_enabled = sources_enabled or {"openalex": True, "arxiv": True, "pmc": True}
        self.conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        self._init_db()
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.scanner = OutputFolderScanner(self.download_dir)

    def _init_db(self):
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE,
                title TEXT,
                keyword TEXT,
                path TEXT,
                size_bytes INTEGER,
                sha256 TEXT,
                downloaded_at TEXT
            )
            """
        )
        for ddl in [
            "ALTER TABLE downloads ADD COLUMN quality_score INTEGER",
            "ALTER TABLE downloads ADD COLUMN quality_reasons TEXT",
            "ALTER TABLE downloads ADD COLUMN source TEXT",
        ]:
            try:
                self.conn.execute(ddl)
            except sqlite3.OperationalError:
                pass
        self.conn.commit()

    def log(self, msg: str):
        timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] {msg}"
        self.log_queue.put(line)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    def internet_check(self) -> bool:
        try:
            r = self.session.get("https://api.openalex.org/works?search=ai&per-page=1", timeout=15)
            ok = r.status_code == 200
            self.log(f"Internet check: {'OK' if ok else 'failed'} HTTP {r.status_code}")
            return ok
        except Exception as e:
            self.log(f"Internet check failed: {e}")
            return False

    def already_downloaded(self, url: str) -> bool:
        row = self.conn.execute("SELECT 1 FROM downloads WHERE url=?", (url,)).fetchone()
        return row is not None

    def safe_filename(self, title: str, url: str) -> str:
        title = title or "document"
        title = re.sub(r"[^\w\s.-]", "", title, flags=re.UNICODE).strip()
        title = re.sub(r"\s+", "_", title)
        if len(title) > 120:
            title = title[:120]
        digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]
        return f"{title}_{digest}.pdf"

    def file_exists_in_output_folder(self, filename: str) -> bool:
        return (self.download_dir / filename).exists()

    def normalize_pdf_url(self, url: str) -> str:
        if not url:
            return ""
        url = url.strip()
        if url.startswith("http://arxiv.org/abs/"):
            return url.replace("/abs/", "/pdf/") + ".pdf"
        if url.startswith("https://arxiv.org/abs/"):
            return url.replace("/abs/", "/pdf/") + ".pdf"
        return url

    def invert_abstract(self, inverted_index):
        if not inverted_index:
            return ""
        positions = []
        for word, indexes in inverted_index.items():
            for index in indexes:
                positions.append((index, word))
        positions.sort()
        return " ".join(word for _, word in positions)

    def search_openalex(self, keyword: str, per_page: int = 50):
        url = f"https://api.openalex.org/works?search={quote_plus(keyword)}&filter=is_oa:true&per-page={per_page}"
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            for work in data.get("results", []):
                title = work.get("title") or keyword
                open_access = work.get("open_access") or {}
                primary = work.get("primary_location") or {}
                abstract = self.invert_abstract(work.get("abstract_inverted_index"))
                year = work.get("publication_year")
                cited_by = work.get("cited_by_count") or 0
                doi = work.get("doi")

                pdf_url = None
                landing = None
                if primary:
                    pdf_url = primary.get("pdf_url")
                    landing = primary.get("landing_page_url")
                if not pdf_url:
                    pdf_url = open_access.get("oa_url")

                pdf_url = self.normalize_pdf_url(pdf_url or "")
                if pdf_url and pdf_url.lower().endswith(".pdf"):
                    yield {
                        "title": title, "url": pdf_url, "source": "OpenAlex",
                        "abstract": abstract, "year": year,
                        "cited_by_count": cited_by, "doi": doi,
                    }
                elif landing:
                    found = self.find_pdf_on_page(landing)
                    if found:
                        yield {
                            "title": title, "url": found,
                            "source": "OpenAlex landing page",
                            "abstract": abstract, "year": year,
                            "cited_by_count": cited_by, "doi": doi,
                        }
        except Exception as e:
            self.log(f"OpenAlex search error for '{keyword}': {e}")

    def search_arxiv(self, keyword: str, max_results: int = 50):
        url = (
            "https://export.arxiv.org/api/query?"
            f"search_query=all:{quote_plus(keyword)}&start=0&max_results={max_results}"
        )
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "xml")
            for entry in soup.find_all("entry"):
                title_tag = entry.find("title")
                summary_tag = entry.find("summary")
                published_tag = entry.find("published")
                title = title_tag.get_text(" ", strip=True) if title_tag else keyword
                abstract = summary_tag.get_text(" ", strip=True) if summary_tag else ""
                year = None
                if published_tag:
                    try:
                        year = int(published_tag.get_text(strip=True)[:4])
                    except Exception:
                        year = None

                pdf_url = None
                for link in entry.find_all("link"):
                    if link.get("title") == "pdf" or link.get("type") == "application/pdf":
                        pdf_url = link.get("href")
                        break

                if pdf_url:
                    yield {
                        "title": title, "url": self.normalize_pdf_url(pdf_url),
                        "source": "arXiv", "abstract": abstract, "year": year,
                        "cited_by_count": 0, "doi": None,
                    }
        except Exception as e:
            self.log(f"arXiv search error for '{keyword}': {e}")

    def search_pmc(self, keyword: str, max_results: int = 30):
        """Search PubMed Central open-access subset via NCBI E-utilities."""
        try:
            term = f'({keyword}) AND "open access"[filter]'
            esearch_url = (
                "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
                f"?db=pmc&term={quote_plus(term)}&retmode=json&retmax={max_results}"
            )
            r = self.session.get(esearch_url, timeout=30)
            r.raise_for_status()
            ids = (r.json().get("esearchresult") or {}).get("idlist") or []
            if not ids:
                return

            esum_url = (
                "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
                f"?db=pmc&id={','.join(ids)}&retmode=json"
            )
            r = self.session.get(esum_url, timeout=30)
            r.raise_for_status()
            result = (r.json() or {}).get("result", {})

            for pmc_id in ids:
                meta = result.get(pmc_id) or {}
                title = meta.get("title") or keyword
                year = None
                pubdate = meta.get("pubdate") or meta.get("epubdate") or ""
                if pubdate:
                    try:
                        year = int(pubdate[:4])
                    except Exception:
                        year = None
                doi = None
                for art_id in meta.get("articleids", []) or []:
                    if (art_id.get("idtype") or "").lower() == "doi":
                        doi = art_id.get("value")
                        break

                pdf_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmc_id}/pdf/"
                yield {
                    "title": title, "url": pdf_url, "source": "PubMed Central",
                    "abstract": "", "year": year, "cited_by_count": 0, "doi": doi,
                }
        except Exception as e:
            self.log(f"PubMed Central search error for '{keyword}': {e}")

    def find_pdf_on_page(self, landing_url: str):
        try:
            r = self.session.get(landing_url, timeout=20, allow_redirects=True)
            if r.status_code >= 400:
                return None
            content_type = r.headers.get("Content-Type", "").lower()
            if "application/pdf" in content_type or r.url.lower().endswith(".pdf"):
                return r.url
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                text = a.get_text(" ", strip=True).lower()
                if ".pdf" in href.lower() or "pdf" in text:
                    if href.startswith("//"):
                        parsed = urlparse(r.url)
                        href = f"{parsed.scheme}:{href}"
                    elif href.startswith("/"):
                        parsed = urlparse(r.url)
                        href = f"{parsed.scheme}://{parsed.netloc}{href}"
                    elif not href.startswith("http"):
                        continue
                    return href
        except Exception:
            return None
        return None

    def get_remote_size(self, url: str) -> int:
        try:
            r = self.session.head(url, timeout=20, allow_redirects=True)
            size = r.headers.get("Content-Length")
            if size and size.isdigit():
                return int(size)
        except Exception:
            pass
        return 0

    def validate_output_folder(self):
        result = self.scanner.scan()
        mb = result["total_size_bytes"] / 1024 / 1024
        self.log(f"Output scan: {result['total_pdfs']} PDF files, {mb:.2f} MB total")
        self.log(f"Output folder: {result['folder']}")

        if result["new_or_changed"]:
            self.log(f"New/changed PDFs since app start: {len(result['new_or_changed'])}")
            for file_path, meta in result["new_or_changed"][:20]:
                self.log(f"  OK: {file_path} ({meta['size'] / 1024 / 1024:.2f} MB, valid={meta['valid']})")
        else:
            self.log("No new or changed PDFs since app start.")

        if result["invalid"]:
            self.log(f"Warning: {len(result['invalid'])} invalid PDF-looking files found.")
            for file_path, meta in result["invalid"][:20]:
                self.log(f"  INVALID: {file_path}")

        return result

    def find_duplicates(self):
        """Hash all PDFs in download folder and group by SHA256."""
        groups = defaultdict(list)
        for path in self.download_dir.rglob("*.pdf"):
            try:
                if not is_valid_pdf(path):
                    continue
                digest = hash_file_sha256(path)
                groups[digest].append(path)
            except Exception as e:
                self.log(f"Hash failed for {path}: {e}")
        duplicates = {h: paths for h, paths in groups.items() if len(paths) > 1}
        return duplicates

    def remove_duplicates(self, duplicates):
        """Keep the oldest copy in each duplicate group; delete the rest."""
        removed = 0
        freed_bytes = 0
        for digest, paths in duplicates.items():
            paths_sorted = sorted(paths, key=lambda p: p.stat().st_mtime)
            keep = paths_sorted[0]
            for p in paths_sorted[1:]:
                try:
                    freed_bytes += p.stat().st_size
                    p.unlink()
                    removed += 1
                    self.log(f"Removed duplicate: {p.name} (kept {keep.name})")
                except Exception as e:
                    self.log(f"Could not remove {p}: {e}")
        return removed, freed_bytes

    def download_pdf(self, item: dict, keyword: str, quality_score: int, quality_reasons: list) -> bool:
        url = self.normalize_pdf_url(item["url"])
        title = item.get("title") or keyword
        source = item.get("source") or ""

        filename = self.safe_filename(title, url)
        path = self.download_dir / filename
        tmp_path = path.with_suffix(".part")

        if self.file_exists_in_output_folder(filename):
            self.log(f"Skipping because PDF already exists in output folder: {filename}")
            return False

        if self.already_downloaded(url):
            self.log(f"Skipping because URL is already in database: {title}")
            return False

        remote_size = self.get_remote_size(url)
        ok, reason = self.limiter.can_download(self.conn, remote_size)
        if not ok:
            self.log(f"Stopping downloads for now: {reason}")
            return False

        self.log(f"Downloading [{source}] quality {quality_score}: {title}")
        self.log(f"Quality reasons: {', '.join(quality_reasons[:6])}")
        self.log(f"URL: {url}")

        try:
            with self.session.get(url, stream=True, timeout=60, allow_redirects=True) as r:
                r.raise_for_status()
                content_type = r.headers.get("Content-Type", "").lower()
                if "pdf" not in content_type and not r.url.lower().endswith(".pdf"):
                    self.log(f"Skipped non-PDF response: {content_type or 'unknown content type'}")
                    return False

                total = 0
                sha = hashlib.sha256()
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 128):
                        if STOP_FLAG.exists():
                            self.log("Stop flag found during download. Aborting current download.")
                            tmp_path.unlink(missing_ok=True)
                            return False
                        if not chunk:
                            continue
                        total += len(chunk)
                        ok, reason = self.limiter.can_download(self.conn, total)
                        if not ok:
                            self.log(f"Aborting download: {reason}")
                            tmp_path.unlink(missing_ok=True)
                            return False
                        sha.update(chunk)
                        f.write(chunk)

            if total < 1024:
                self.log("Skipped tiny file; likely not a real PDF.")
                tmp_path.unlink(missing_ok=True)
                return False

            if not is_valid_pdf(tmp_path):
                self.log("Skipped file because it does not start with PDF magic bytes.")
                tmp_path.unlink(missing_ok=True)
                return False

            if self.file_exists_in_output_folder(filename):
                self.log(f"Skipping final save because file now exists in output folder: {filename}")
                tmp_path.unlink(missing_ok=True)
                return False

            tmp_path.replace(path)

            confirmation = self.scanner.confirm_file(path)
            if not confirmation["exists"] or not confirmation["correct_folder"] or not confirmation["valid_pdf"]:
                self.log(f"Validation failed after save: {confirmation}")
                return False

            self.conn.execute(
                """
                INSERT OR IGNORE INTO downloads(
                    url, title, keyword, path, size_bytes, sha256,
                    quality_score, quality_reasons, source, downloaded_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    url, title, keyword, str(path), total, sha.hexdigest(),
                    quality_score, json.dumps(quality_reasons), source,
                    dt.datetime.now().isoformat(timespec="seconds"),
                ),
            )
            self.conn.commit()

            self.log(f"Saved and validated PDF: {path} ({total / 1024 / 1024:.2f} MB)")
            return True

        except Exception as e:
            self.log(f"Download failed: {e}")
            tmp_path.unlink(missing_ok=True)
            return False

    def search_and_download_once(self, keywords):
        if not self.internet_check():
            return

        downloaded_this_round = 0
        qfilter = QualityFilter(keywords, self.min_quality_score)

        for keyword in keywords:
            if STOP_FLAG.exists():
                self.log("Stop flag found. Scraper stopping.")
                break

            keyword = keyword.strip()
            if not keyword:
                continue

            self.log(f"Searching keyword: {keyword}")

            candidates = []
            if self.sources_enabled.get("openalex", True):
                candidates.extend(list(self.search_openalex(keyword)))
            if self.sources_enabled.get("arxiv", True):
                candidates.extend(list(self.search_arxiv(keyword)))
            if self.sources_enabled.get("pmc", True):
                candidates.extend(list(self.search_pmc(keyword)))

            unique = {}
            for item in candidates:
                url = self.normalize_pdf_url(item.get("url", ""))
                if not url:
                    continue
                unique[url] = item

            scored = []
            rejected = 0
            for url, item in unique.items():
                filename = self.safe_filename(item.get("title") or keyword, url)
                if self.file_exists_in_output_folder(filename):
                    self.log(f"Pre-filter skip, already in output folder: {filename}")
                    continue
                accepted, score, reasons = qfilter.accept(item)
                if accepted:
                    scored.append((score, reasons, item))
                else:
                    rejected += 1
                    self.log(f"Filtered low-value PDF score {score}: {item.get('title', 'Untitled')}")

            scored.sort(key=lambda row: row[0], reverse=True)

            self.log(
                f"Found {len(unique)} candidates for '{keyword}'. "
                f"Accepted {len(scored)}, rejected {rejected}, min score {self.min_quality_score}."
            )

            for score, reasons, item in scored:
                if STOP_FLAG.exists():
                    self.log("Stop flag found. Scraper stopping.")
                    break
                count, bytes_used = self.limiter.get_usage(self.conn)
                if count >= self.limiter.max_downloads or bytes_used >= self.limiter.max_bytes:
                    self.log("Daily limits reached. Waiting until tomorrow.")
                    return
                if self.download_pdf(item, keyword, score, reasons):
                    downloaded_this_round += 1
                time.sleep(2)

        self.log(f"Round complete. Downloaded {downloaded_this_round} PDFs this round.")
        self.validate_output_folder()


class KeywordRotator:
    def __init__(self, profiles):
        self.set_profiles(profiles)

    def set_profiles(self, profiles):
        self.profiles = profiles
        self.names = list(profiles.keys())
        self.index = 0

    def current_name(self):
        if not self.names:
            return "Manual"
        return self.names[self.index % len(self.names)]

    def current_keywords(self):
        if not self.names:
            return []
        return self.profiles[self.current_name()]

    def next(self):
        if self.names:
            self.index = (self.index + 1) % len(self.names)
        return self.current_name(), self.current_keywords()

    def set_profile(self, name):
        if name in self.names:
            self.index = self.names.index(name)


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1180x830")
        self.minsize(1040, 720)

        self.log_queue = queue.Queue()
        self.worker = None
        self.stop_event = threading.Event()
        self.active_scraper = None

        self.profiles = load_profiles()
        self.rotator = KeywordRotator(self.profiles)

        self.download_dir_var = tk.StringVar(value=os.getenv("DOWNLOAD_DIR", DEFAULT_DOWNLOAD_DIR))
        self.profile_var = tk.StringVar(value=self.rotator.current_name())
        self.keywords_var = tk.StringVar(value=", ".join(self.rotator.current_keywords()))
        self.auto_rotate_var = tk.BooleanVar(value=env_bool("AUTO_ROTATE_KEYWORDS", True))
        self.interval_var = tk.IntVar(value=env_int("SEARCH_INTERVAL_MINUTES", 30))
        self.max_downloads_var = tk.IntVar(value=env_int("MAX_DOWNLOADS_PER_DAY", 100))
        self.max_mb_var = tk.IntVar(value=env_int("MAX_MB_PER_DAY", 100))
        self.min_quality_var = tk.IntVar(value=env_int("MIN_QUALITY_SCORE", 35))

        self.src_openalex_var = tk.BooleanVar(value=env_bool("SOURCE_OPENALEX", True))
        self.src_arxiv_var = tk.BooleanVar(value=env_bool("SOURCE_ARXIV", True))
        self.src_pmc_var = tk.BooleanVar(value=env_bool("SOURCE_PMC", True))

        self.status_var = tk.StringVar(value="Stopped")
        self.folder_status_var = tk.StringVar(value="Output folder not scanned yet")
        self.profile_status_var = tk.StringVar(value=f"Profiles loaded: {len(self.profiles)}")
        self.stats_summary_var = tk.StringVar(value="No statistics loaded yet.")

        self._build_ui()
        self.after(300, self._poll_logs)

    def _build_ui(self):
        header = ttk.Frame(self)
        header.pack(fill="x", padx=10, pady=(10, 0))
        title_lbl = ttk.Label(header, text=f"{BRAND}  |  PDF Scraper",
                              font=("Segoe UI", 14, "bold"))
        title_lbl.pack(side="left")
        ttk.Label(header, text="Open-access academic PDF collector",
                  foreground="#555").pack(side="left", padx=12)

        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=10, pady=10)

        scraper_tab = ttk.Frame(self.notebook)
        stats_tab = ttk.Frame(self.notebook)
        tools_tab = ttk.Frame(self.notebook)
        self.notebook.add(scraper_tab, text="Scraper")
        self.notebook.add(stats_tab, text="Statistics & History")
        self.notebook.add(tools_tab, text="Duplicate Scanner")

        self._build_scraper_tab(scraper_tab)
        self._build_stats_tab(stats_tab)
        self._build_tools_tab(tools_tab)

        ttk.Label(self,
                  text=f"{BRAND} PDF Scraper - downloads only open-access PDFs from public APIs.",
                  foreground="#666").pack(fill="x", padx=10, pady=(0, 8))

    def _build_scraper_tab(self, parent):
        pad = {"padx": 10, "pady": 6}

        top = ttk.Frame(parent)
        top.pack(fill="x", **pad)

        ttk.Label(top, text="Download folder:").grid(row=0, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.download_dir_var, width=95).grid(row=0, column=1, sticky="we", padx=6)
        ttk.Button(top, text="Browse", command=self.browse_folder).grid(row=0, column=2)

        ttk.Label(top, text="Keyword profile:").grid(row=1, column=0, sticky="w")
        self.profile_combo = ttk.Combobox(
            top, textvariable=self.profile_var,
            values=list(self.profiles.keys()), state="normal",
        )
        self.profile_combo.grid(row=1, column=1, sticky="we", padx=6)
        self.profile_combo.bind("<<ComboboxSelected>>", self.profile_changed)

        profile_button_row = ttk.Frame(top)
        profile_button_row.grid(row=1, column=2, sticky="we")
        ttk.Button(profile_button_row, text="Next", command=self.next_profile).pack(side="left", padx=2)
        ttk.Button(profile_button_row, text="Save", command=self.save_current_profile).pack(side="left", padx=2)
        ttk.Button(profile_button_row, text="Delete", command=self.delete_current_profile).pack(side="left", padx=2)
        ttk.Button(profile_button_row, text="Reload", command=self.reload_profiles).pack(side="left", padx=2)

        ttk.Label(top, text="Subjects / keywords:").grid(row=2, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.keywords_var, width=95).grid(row=2, column=1, sticky="we", padx=6)
        ttk.Label(top, text="comma-separated").grid(row=2, column=2, sticky="w")

        ttk.Label(top, text="Interval minutes:").grid(row=3, column=0, sticky="w")
        ttk.Spinbox(top, from_=5, to=1440, textvariable=self.interval_var, width=10).grid(row=3, column=1, sticky="w", padx=6)

        ttk.Label(top, text="Max downloads/day:").grid(row=4, column=0, sticky="w")
        ttk.Spinbox(top, from_=1, to=100, textvariable=self.max_downloads_var, width=10).grid(row=4, column=1, sticky="w", padx=6)

        ttk.Label(top, text="Max MB/day:").grid(row=5, column=0, sticky="w")
        ttk.Spinbox(top, from_=1, to=100, textvariable=self.max_mb_var, width=10).grid(row=5, column=1, sticky="w", padx=6)

        ttk.Label(top, text="Minimum quality score:").grid(row=6, column=0, sticky="w")
        ttk.Spinbox(top, from_=0, to=100, textvariable=self.min_quality_var, width=10).grid(row=6, column=1, sticky="w", padx=6)

        ttk.Checkbutton(top, text="Auto-rotate keyword profiles each cycle",
                        variable=self.auto_rotate_var).grid(row=7, column=1, sticky="w", padx=6)

        sources_frame = ttk.LabelFrame(top, text="Sources")
        sources_frame.grid(row=8, column=0, columnspan=3, sticky="we", padx=6, pady=4)
        ttk.Checkbutton(sources_frame, text="OpenAlex", variable=self.src_openalex_var).pack(side="left", padx=8)
        ttk.Checkbutton(sources_frame, text="arXiv", variable=self.src_arxiv_var).pack(side="left", padx=8)
        ttk.Checkbutton(sources_frame, text="PubMed Central", variable=self.src_pmc_var).pack(side="left", padx=8)

        ttk.Label(top, textvariable=self.profile_status_var).grid(row=9, column=1, sticky="w", padx=6)
        top.columnconfigure(1, weight=1)

        buttons = ttk.Frame(parent)
        buttons.pack(fill="x", **pad)
        ttk.Button(buttons, text="Start 24/7 Scraper", command=self.start_worker).pack(side="left", padx=4)
        ttk.Button(buttons, text="Run One Search Now", command=self.run_once).pack(side="left", padx=4)
        ttk.Button(buttons, text="Validate Output Folder", command=self.validate_output_folder).pack(side="left", padx=4)
        ttk.Button(buttons, text="Stop", command=self.stop_worker).pack(side="left", padx=4)
        ttk.Button(buttons, text="Open Download Folder", command=self.open_download_folder).pack(side="left", padx=4)
        ttk.Label(buttons, textvariable=self.status_var).pack(side="right", padx=8)

        folder_status = ttk.Frame(parent)
        folder_status.pack(fill="x", padx=10, pady=(0, 6))
        ttk.Label(folder_status, textvariable=self.folder_status_var).pack(side="left")

        log_frame = ttk.LabelFrame(parent, text="Log")
        log_frame.pack(fill="both", expand=True, padx=10, pady=10)
        self.log_text = tk.Text(log_frame, wrap="word")
        self.log_text.pack(side="left", fill="both", expand=True)
        scroll = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scroll.pack(side="right", fill="y")
        self.log_text.configure(yscrollcommand=scroll.set)

    def _build_stats_tab(self, parent):
        top = ttk.Frame(parent)
        top.pack(fill="x", padx=10, pady=8)
        ttk.Button(top, text="Refresh Statistics", command=self.refresh_stats).pack(side="left", padx=4)
        ttk.Button(top, text="Export History to CSV", command=self.export_history_csv).pack(side="left", padx=4)
        ttk.Label(top, textvariable=self.stats_summary_var, foreground="#333").pack(side="left", padx=14)

        body = ttk.PanedWindow(parent, orient="horizontal")
        body.pack(fill="both", expand=True, padx=10, pady=8)

        left = ttk.LabelFrame(body, text="Breakdowns")
        body.add(left, weight=1)
        self.breakdown_text = tk.Text(left, wrap="word", height=20, width=40)
        self.breakdown_text.pack(fill="both", expand=True)

        right = ttk.LabelFrame(body, text="Recent downloads")
        body.add(right, weight=2)
        cols = ("date", "source", "keyword", "title", "mb", "score")
        self.history_tree = ttk.Treeview(right, columns=cols, show="headings", height=20)
        for col, label, w in [
            ("date", "Date", 140),
            ("source", "Source", 110),
            ("keyword", "Keyword", 140),
            ("title", "Title", 380),
            ("mb", "MB", 60),
            ("score", "Score", 60),
        ]:
            self.history_tree.heading(col, text=label)
            self.history_tree.column(col, width=w, anchor="w")
        self.history_tree.pack(side="left", fill="both", expand=True)
        sb = ttk.Scrollbar(right, orient="vertical", command=self.history_tree.yview)
        sb.pack(side="right", fill="y")
        self.history_tree.configure(yscrollcommand=sb.set)

    def _build_tools_tab(self, parent):
        info = ttk.Label(
            parent,
            text=("Scan the download folder for byte-identical duplicate PDFs (SHA-256). "
                  "Use 'Find Duplicates' first; then 'Remove Duplicates' deletes extra copies "
                  "and keeps the oldest one in each group."),
            wraplength=900, justify="left",
        )
        info.pack(fill="x", padx=12, pady=10)

        btns = ttk.Frame(parent)
        btns.pack(fill="x", padx=12, pady=4)
        ttk.Button(btns, text="Find Duplicates", command=self.find_duplicates_action).pack(side="left", padx=4)
        ttk.Button(btns, text="Remove Duplicates (keep oldest)",
                   command=self.remove_duplicates_action).pack(side="left", padx=4)

        self.dup_text = tk.Text(parent, wrap="word")
        self.dup_text.pack(fill="both", expand=True, padx=12, pady=10)
        self._last_duplicates = {}

    def refresh_profile_ui(self, selected_name=None):
        names = list(self.profiles.keys())
        self.profile_combo["values"] = names
        self.rotator.set_profiles(self.profiles)

        if selected_name and selected_name in self.profiles:
            self.rotator.set_profile(selected_name)
            self.profile_var.set(selected_name)
            self.keywords_var.set(", ".join(self.profiles[selected_name]))
        elif names:
            current = names[0]
            self.profile_var.set(current)
            self.keywords_var.set(", ".join(self.profiles[current]))
        else:
            self.profile_var.set("Manual")
            self.keywords_var.set("")

        self.profile_status_var.set(f"Profiles loaded: {len(self.profiles)}")

    def profile_changed(self, event=None):
        name = self.profile_var.get().strip()
        self.rotator.set_profile(name)
        if name in self.profiles:
            self.keywords_var.set(", ".join(self.profiles[name]))
            self.profile_status_var.set(f"Selected profile: {name}")

    def next_profile(self):
        name, keywords = self.rotator.next()
        self.profile_var.set(name)
        self.keywords_var.set(", ".join(keywords))
        self.profile_status_var.set(f"Selected profile: {name}")

    def save_current_profile(self):
        name = self.profile_var.get().strip()
        keywords = self.parse_keywords()
        if not name:
            messagebox.showwarning(APP_NAME, "Type a profile name first.")
            return
        if not keywords:
            messagebox.showwarning(APP_NAME, "Add at least one keyword before saving.")
            return
        self.profiles[name] = keywords
        try:
            self.profiles = save_profiles(self.profiles)
            self.refresh_profile_ui(selected_name=name)
            self.log_queue.put(f"Saved keyword profile: {name} ({len(keywords)} keywords)")
            messagebox.showinfo(APP_NAME, f"Saved profile: {name}")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Could not save profile:\n{e}")

    def delete_current_profile(self):
        name = self.profile_var.get().strip()
        if name not in self.profiles:
            messagebox.showwarning(APP_NAME, "That profile is not saved yet.")
            return
        if len(self.profiles) <= 1:
            messagebox.showwarning(APP_NAME, "You need at least one profile.")
            return
        if not messagebox.askyesno(APP_NAME, f"Delete keyword profile '{name}'?"):
            return
        try:
            del self.profiles[name]
            self.profiles = save_profiles(self.profiles)
            self.refresh_profile_ui()
            self.log_queue.put(f"Deleted keyword profile: {name}")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Could not delete profile:\n{e}")

    def reload_profiles(self):
        self.profiles = load_profiles()
        self.refresh_profile_ui()
        self.log_queue.put(f"Reloaded keyword profiles from {PROFILE_FILE}")
        messagebox.showinfo(APP_NAME, f"Reloaded {len(self.profiles)} profiles.")

    def browse_folder(self):
        folder = filedialog.askdirectory(initialdir=self.download_dir_var.get())
        if folder:
            self.download_dir_var.set(folder)

    def parse_keywords(self):
        return clean_keyword_list(self.keywords_var.get().split(","))

    def sources_dict(self):
        return {
            "openalex": bool(self.src_openalex_var.get()),
            "arxiv": bool(self.src_arxiv_var.get()),
            "pmc": bool(self.src_pmc_var.get()),
        }

    def scraper(self):
        self.active_scraper = PDFScraper(
            download_dir=Path(self.download_dir_var.get()),
            log_queue=self.log_queue,
            max_downloads=min(int(self.max_downloads_var.get()), 100),
            max_mb=min(int(self.max_mb_var.get()), 100),
            min_quality_score=int(self.min_quality_var.get()),
            sources_enabled=self.sources_dict(),
        )
        return self.active_scraper

    def start_worker(self):
        if self.worker and self.worker.is_alive():
            messagebox.showinfo(APP_NAME, "Scraper is already running.")
            return
        if STOP_FLAG.exists():
            STOP_FLAG.unlink()
        self.stop_event.clear()
        self.worker = threading.Thread(target=self.worker_loop, daemon=True)
        self.worker.start()
        self.status_var.set("Running 24/7")

    def run_once(self):
        if self.worker and self.worker.is_alive():
            messagebox.showinfo(APP_NAME, "Scraper is already running.")
            return
        if STOP_FLAG.exists():
            STOP_FLAG.unlink()
        self.stop_event.clear()
        self.worker = threading.Thread(target=self.worker_once, daemon=True)
        self.worker.start()
        self.status_var.set("Running one search")

    def rotate_after_cycle_if_enabled(self):
        if self.auto_rotate_var.get():
            name, keywords = self.rotator.next()
            self.profile_var.set(name)
            self.keywords_var.set(", ".join(keywords))
            self.log_queue.put(f"Auto-rotated to keyword profile: {name}")

    def worker_once(self):
        try:
            scraper = self.scraper()
            scraper.search_and_download_once(self.parse_keywords())
            self.update_folder_status(scraper.validate_output_folder())
            self.rotate_after_cycle_if_enabled()
        finally:
            self.status_var.set("Stopped")

    def worker_loop(self):
        scraper = self.scraper()
        while not self.stop_event.is_set() and not STOP_FLAG.exists():
            scraper.search_and_download_once(self.parse_keywords())
            result = scraper.validate_output_folder()
            self.update_folder_status(result)
            self.rotate_after_cycle_if_enabled()
            interval_seconds = max(5, int(self.interval_var.get())) * 60
            scraper.log(f"Sleeping for {interval_seconds // 60} minutes.")
            for _ in range(interval_seconds):
                if self.stop_event.is_set() or STOP_FLAG.exists():
                    break
                time.sleep(1)
        self.status_var.set("Stopped")
        scraper.log("Scraper stopped.")

    def validate_output_folder(self):
        def run_scan():
            try:
                scraper = self.active_scraper or self.scraper()
                result = scraper.validate_output_folder()
                self.update_folder_status(result)
            except Exception as e:
                self.log_queue.put(f"Validation scan failed: {e}")
        threading.Thread(target=run_scan, daemon=True).start()

    def update_folder_status(self, result):
        total_pdfs = result.get("total_pdfs", 0)
        total_mb = result.get("total_size_bytes", 0) / 1024 / 1024
        new_count = len(result.get("new_or_changed", []))
        invalid_count = len(result.get("invalid", []))
        self.folder_status_var.set(
            f"Output scan: {total_pdfs} PDFs | {total_mb:.2f} MB | "
            f"{new_count} new/changed | {invalid_count} invalid"
        )

    def stop_worker(self):
        self.stop_event.set()
        STOP_FLAG.write_text("STOP", encoding="utf-8")
        self.status_var.set("Stopping...")

    def open_download_folder(self):
        folder = Path(self.download_dir_var.get())
        folder.mkdir(parents=True, exist_ok=True)
        os.startfile(str(folder))

    def _read_history(self, limit=500):
        if not DB_FILE.exists():
            return []
        try:
            conn = sqlite3.connect(DB_FILE)
            try:
                rows = conn.execute(
                    """SELECT downloaded_at, COALESCE(source,''), COALESCE(keyword,''),
                              COALESCE(title,''), COALESCE(size_bytes,0),
                              COALESCE(quality_score,0)
                       FROM downloads ORDER BY downloaded_at DESC LIMIT ?""",
                    (limit,),
                ).fetchall()
            finally:
                conn.close()
            return rows
        except Exception as e:
            self.log_queue.put(f"History read failed: {e}")
            return []

    def refresh_stats(self):
        rows = self._read_history(limit=1000)
        for item in self.history_tree.get_children():
            self.history_tree.delete(item)

        if not rows:
            self.stats_summary_var.set("No download history yet.")
            self.breakdown_text.delete("1.0", "end")
            self.breakdown_text.insert("1.0", "No data.")
            return

        total = len(rows)
        total_bytes = sum(r[4] for r in rows)
        today = dt.date.today().isoformat()
        today_count = sum(1 for r in rows if (r[0] or "").startswith(today))

        by_source = defaultdict(int)
        by_keyword = defaultdict(int)
        by_date = defaultdict(int)
        for downloaded_at, source, keyword, title, size, score in rows:
            by_source[source or "unknown"] += 1
            by_keyword[keyword or "unknown"] += 1
            by_date[(downloaded_at or "")[:10]] += 1

        self.stats_summary_var.set(
            f"Total: {total} PDFs | {total_bytes / 1024 / 1024:.1f} MB | Today: {today_count}"
        )

        lines = ["By source:"]
        for src, n in sorted(by_source.items(), key=lambda x: x[1], reverse=True):
            lines.append(f"  {src or '(none)'}: {n}")
        lines.append("\nTop keywords:")
        for kw, n in sorted(by_keyword.items(), key=lambda x: x[1], reverse=True)[:15]:
            lines.append(f"  {kw}: {n}")
        lines.append("\nLast 14 days:")
        for d, n in sorted(by_date.items(), reverse=True)[:14]:
            lines.append(f"  {d}: {n}")
        self.breakdown_text.delete("1.0", "end")
        self.breakdown_text.insert("1.0", "\n".join(lines))

        for downloaded_at, source, keyword, title, size, score in rows[:300]:
            mb = (size or 0) / 1024 / 1024
            self.history_tree.insert(
                "", "end",
                values=(downloaded_at, source, keyword,
                        (title or "")[:200], f"{mb:.2f}", score),
            )

    def export_history_csv(self):
        rows = self._read_history(limit=100000)
        if not rows:
            messagebox.showinfo(APP_NAME, "No history to export yet.")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            initialfile=f"greynoc_history_{dt.date.today().isoformat()}.csv",
            filetypes=[("CSV", "*.csv")],
        )
        if not path:
            return
        try:
            import csv
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["downloaded_at", "source", "keyword", "title", "size_bytes", "quality_score"])
                for r in rows:
                    w.writerow(r)
            messagebox.showinfo(APP_NAME, f"Exported {len(rows)} rows to:\n{path}")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Export failed:\n{e}")

    def find_duplicates_action(self):
        self.dup_text.delete("1.0", "end")
        self.dup_text.insert("end", "Hashing PDFs... please wait.\n")
        self.update_idletasks()

        def run():
            try:
                scraper = self.active_scraper or self.scraper()
                dups = scraper.find_duplicates()
                self._last_duplicates = dups
                self.dup_text.delete("1.0", "end")
                if not dups:
                    self.dup_text.insert("end", "No duplicates found.\n")
                    return
                total_extra = sum(len(v) - 1 for v in dups.values())
                self.dup_text.insert(
                    "end", f"Found {len(dups)} duplicate groups, {total_extra} extra copies.\n\n"
                )
                for digest, paths in dups.items():
                    self.dup_text.insert("end", f"SHA256 {digest[:16]}...  ({len(paths)} copies)\n")
                    for p in paths:
                        self.dup_text.insert("end", f"  {p}\n")
                    self.dup_text.insert("end", "\n")
            except Exception as e:
                self.dup_text.insert("end", f"Duplicate scan failed: {e}\n")

        threading.Thread(target=run, daemon=True).start()

    def remove_duplicates_action(self):
        if not self._last_duplicates:
            messagebox.showinfo(APP_NAME, "Run 'Find Duplicates' first.")
            return
        total_extra = sum(len(v) - 1 for v in self._last_duplicates.values())
        if not messagebox.askyesno(APP_NAME, f"Delete {total_extra} duplicate file(s)? Oldest copies are kept."):
            return
        scraper = self.active_scraper or self.scraper()
        removed, freed = scraper.remove_duplicates(self._last_duplicates)
        self.dup_text.insert("end", f"\nRemoved {removed} files, freed {freed / 1024 / 1024:.2f} MB.\n")
        self._last_duplicates = {}
        messagebox.showinfo(APP_NAME, f"Removed {removed} files.\nFreed {freed / 1024 / 1024:.2f} MB.")

    def _poll_logs(self):
        while True:
            try:
                line = self.log_queue.get_nowait()
            except queue.Empty:
                break
            self.log_text.insert("end", line + "\n")
            self.log_text.see("end")
        self.after(300, self._poll_logs)

    def on_close(self):
        self.stop_worker()
        self.destroy()


if __name__ == "__main__":
    app = App()
    app.protocol("WM_DELETE_WINDOW", app.on_close)
    app.mainloop()
