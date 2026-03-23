#!/usr/bin/env python3
"""
Batch scanner for GitHub repositories.

Workflow per batch:
1) Clone repositories to local temporary directory.
2) Run license_check in parallel.
3) Save per-repo JSON results and a batch manifest.
4) Upload results to OSS (S3-compatible endpoint).
5) Cleanup cloned repositories (and optionally local result files).
"""

from __future__ import annotations

import argparse
import concurrent.futures as futures
import contextlib
import datetime as dt
import hashlib
import json
import multiprocessing as mp
import os
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import tomllib


DEFAULT_LANG = "C.UTF-8"
DEFAULT_MIRROR_PREFIX = "github"
DISPLAY_TZ = ZoneInfo("Asia/Shanghai")
_MIRROR_CLIENT = None
_MIRROR_TOKEN_CACHE: Dict[str, Any] = {"token": "", "expires_at": 0.0}
DEFAULT_SCAN_IGNORE_PATTERNS = (
    ".git/",
    ".github/",
    ".idea/",
    "node_modules/",
    "dist/",
    "build/",
    "target/",
    ".next/",
    ".nuxt/",
    ".cache/",
    "__pycache__/",
    "*.min.js",
    "*.min.css",
    "*.pdf",
    "*.jpg",
    "*.jpeg",
    "*.png",
    "*.webp",
    "*.avif",
    "*.ico",
    "*.svgz",
    "*.gif",
    "*.bmp",
    "*.tif",
    "*.tiff",
    "*.psd",
    "*.mp3",
    "*.mp4",
    "*.avi",
    "*.mkv",
    "*.flv",
    "*.webm",
    "*.wav",
    "*.WAV",
    "*.ogg",
    "*.MOV",
    "*.mov",
    "*.mid",
    "*.cda",
    "*.rmvb",
    "*.zip",
    "*.tar",
    "*.gz",
    "*.bz2",
    "*.xz",
    "*.7z",
    "*.rar",
    "*.jar",
    "*.war",
    "*.ear",
    "*.class",
    "*.exe",
    "*.dll",
    "*.so",
    "*.dylib",
    "*.woff",
    "*.woff2",
    "*.ttf",
    "*.otf",
    "*.eot",
    "*.parquet",
    "*.feather",
    "*.npy",
    "*.npz",
    "*.pkl",
    "*.pickle",
    "*.ipynb",
    "*.html",
    "*.htm",
)


@dataclass(frozen=True)
class RepoTask:
    index: int
    url: str
    ref: Optional[str] = None


def now_iso() -> str:
    return dt.datetime.now(DISPLAY_TZ).replace(microsecond=0).isoformat()


def now_run_id() -> str:
    return dt.datetime.now(DISPLAY_TZ).strftime("run_%Y%m%d_%H%M%S")


def sanitize_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-")
    return cleaned or "repo"


def repo_slug(task: RepoTask) -> str:
    # stable-ish slug: sanitized tail + short hash
    tail = task.url.rstrip("/").split("/")[-1]
    tail = tail[:-4] if tail.endswith(".git") else tail
    short = hashlib.sha1(task.url.encode("utf-8")).hexdigest()[:10]
    return f"{sanitize_name(tail)}-{short}"


def normalize_ref(ref: Optional[str]) -> str:
    value = (ref or "").strip()
    if not value:
        return ""
    for prefix in ("refs/heads/", "origin/"):
        if value.startswith(prefix):
            return value[len(prefix) :]
    return value


def github_repo_full_name(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    if path.endswith(".git"):
        path = path[:-4]
    if "/" not in path:
        raise ValueError(f"invalid github url: {url}")
    return path


def build_mirror_client(mirror_cfg: Dict[str, Any]):
    global _MIRROR_CLIENT
    if _MIRROR_CLIENT is not None:
        return _MIRROR_CLIENT
    try:
        import boto3  # pylint: disable=import-outside-toplevel
        from botocore.config import Config  # pylint: disable=import-outside-toplevel
    except ImportError as e:  # noqa: BLE001
        raise RuntimeError("boto3 is required for mirror download. Install requirements.batch.txt first.") from e

    config = Config(
        connect_timeout=int(mirror_cfg.get("connect_timeout_sec", 10)),
        read_timeout=int(mirror_cfg.get("read_timeout_sec", 60)),
        retries={"max_attempts": int(mirror_cfg.get("max_attempts", 2))},
        s3={"addressing_style": "path"},
    )
    _MIRROR_CLIENT = boto3.client(
        "s3",
        endpoint_url=mirror_cfg["endpoint_url"],
        aws_access_key_id=mirror_cfg["access_key"],
        aws_secret_access_key=mirror_cfg["secret_key"],
        region_name=mirror_cfg.get("region", "us-east-1"),
        config=config,
    )
    return _MIRROR_CLIENT


def resolve_mirror_config(raw_cfg: Dict[str, Any]) -> Dict[str, Any]:
    endpoint = raw_cfg.get("endpoint_url") or raw_cfg.get("endpoint") or os.getenv("MIRROR_ENDPOINT", "")
    if endpoint and not str(endpoint).startswith(("http://", "https://")):
        endpoint = f"https://{endpoint}"
    access_key = os.getenv(raw_cfg.get("access_key_env", "MINIO_ACCESS_KEY"), "") or raw_cfg.get("access_key", "")
    secret_key = os.getenv(raw_cfg.get("secret_key_env", "MINIO_SECRET_KEY"), "") or raw_cfg.get("secret_key", "")
    token_url = raw_cfg.get("token_url") or os.getenv("MIRROR_TOKEN_URL", "")
    repo_info_url = raw_cfg.get("repo_info_url") or os.getenv("MIRROR_REPO_INFO_URL", "")
    client_id = os.getenv(raw_cfg.get("client_id_env", "MIRROR_CLIENT_ID"), "") or raw_cfg.get("client_id", "")
    client_secret = (
        os.getenv(raw_cfg.get("client_secret_env", "MIRROR_CLIENT_SECRET"), "")
        or raw_cfg.get("client_secret", "")
    )
    return {
        "enabled": bool(raw_cfg.get("enabled", False)),
        "endpoint_url": str(endpoint).strip(),
        "bucket": str(raw_cfg.get("bucket", "")),
        "prefix": str(raw_cfg.get("prefix", DEFAULT_MIRROR_PREFIX)).strip("/") or DEFAULT_MIRROR_PREFIX,
        "token_url": str(token_url).strip(),
        "repo_info_url": str(repo_info_url).strip(),
        "client_id": str(client_id),
        "client_secret": str(client_secret),
        "access_key": str(access_key),
        "secret_key": str(secret_key),
        "region": str(raw_cfg.get("region", "us-east-1")),
        "connect_timeout_sec": int(raw_cfg.get("connect_timeout_sec", 10)),
        "read_timeout_sec": int(raw_cfg.get("read_timeout_sec", 60)),
        "max_attempts": int(raw_cfg.get("max_attempts", 2)),
    }


def fetch_mirror_access_token(mirror_cfg: Dict[str, Any]) -> str:
    now = time.time()
    cached_token = _MIRROR_TOKEN_CACHE.get("token", "")
    expires_at = float(_MIRROR_TOKEN_CACHE.get("expires_at", 0.0))
    if cached_token and now < expires_at - 30:
        return cached_token

    token_url = mirror_cfg.get("token_url", "")
    client_id = mirror_cfg.get("client_id", "")
    client_secret = mirror_cfg.get("client_secret", "")
    if not token_url:
        raise RuntimeError("mirror token_url is empty")
    if not client_id or not client_secret:
        raise RuntimeError("mirror client_id/client_secret is empty")

    body = (
        f"grant_type=client_credentials&client_id={quote(client_id, safe='')}"
        f"&client_secret={quote(client_secret, safe='')}"
    ).encode("utf-8")
    req = Request(
        token_url,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    timeout_sec = max(
        int(mirror_cfg.get("connect_timeout_sec", 10)),
        int(mirror_cfg.get("read_timeout_sec", 60)),
    )
    with urlopen(req, timeout=timeout_sec) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    token = str(payload.get("access_token", "")).strip()
    if not token:
        raise RuntimeError(f"mirror token response missing access_token: {payload}")
    expires_in = int(payload.get("expires_in", 300) or 300)
    _MIRROR_TOKEN_CACHE["token"] = token
    _MIRROR_TOKEN_CACHE["expires_at"] = now + expires_in
    return token


def fetch_mirror_zip_path(task: RepoTask, mirror_cfg: Dict[str, Any]) -> str:
    repo_info_url = mirror_cfg.get("repo_info_url", "")
    if not repo_info_url:
        raise RuntimeError("mirror repo_info_url is empty")
    full_name = github_repo_full_name(task.url)
    token = fetch_mirror_access_token(mirror_cfg)
    sep = "&" if "?" in repo_info_url else "?"
    request_url = (
        f"{repo_info_url}{sep}source=github&fullName={quote(full_name, safe='')}"
    )
    req = Request(
        request_url,
        headers={"Authorization": f"Bearer {token}"},
        method="GET",
    )
    timeout_sec = max(
        int(mirror_cfg.get("connect_timeout_sec", 10)),
        int(mirror_cfg.get("read_timeout_sec", 60)),
    )
    with urlopen(req, timeout=timeout_sec) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if payload.get("code") != 200:
        raise RuntimeError(f"mirror repo info error: {payload}")
    data = payload.get("data") or {}
    raw_zip_path = data.get("s3ZipFilePath")
    zip_path = str(raw_zip_path).strip() if raw_zip_path is not None else ""
    if not zip_path:
        raise RuntimeError(f"mirror repo info missing s3ZipFilePath: {payload}")
    return zip_path


def extract_zip(zip_path: Path, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(dest_dir)
    entries = [p for p in dest_dir.iterdir() if not p.name.startswith(".")]
    if len(entries) == 1 and entries[0].is_dir():
        return entries[0]
    return dest_dir


def read_toml(path: Path) -> Dict[str, Any]:
    with path.open("rb") as f:
        return tomllib.load(f)


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def chunked_iter(items: Iterable[RepoTask], size: int) -> Iterator[List[RepoTask]]:
    batch: List[RepoTask] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def parse_repo_line(line: str) -> Optional[Tuple[str, Optional[str]]]:
    """
    Supported formats:
    - https://github.com/org/repo.git
    - https://github.com/org/repo.git,main
    - https://github.com/org/repo.git main
    """
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None

    if stripped.startswith("{"):
        try:
            item = json.loads(stripped)
        except json.JSONDecodeError:
            return None
        clone_url = str(item.get("clone_url") or "").strip()
        full_name = str(item.get("full_name") or "").strip()
        html_url = str(item.get("html_url") or "").strip()
        default_branch = str(item.get("default_branch") or "").strip() or None
        if not clone_url:
            if full_name:
                clone_url = f"https://github.com/{full_name}.git"
            elif html_url:
                clone_url = html_url if html_url.endswith(".git") else f"{html_url}.git"
        if not clone_url:
            return None
        return clone_url, default_branch

    if "," in stripped:
        parts = [p.strip() for p in stripped.split(",", 1)]
    else:
        parts = stripped.split(maxsplit=1)

    if not parts:
        return None

    url = parts[0]
    ref = parts[1] if len(parts) > 1 and parts[1] else None
    return url, ref


def iter_repo_tasks(repos_file: Path) -> Iterator[RepoTask]:
    with repos_file.open("r", encoding="utf-8") as f:
        for line_idx, line in enumerate(f, start=1):
            parsed = parse_repo_line(line)
            if not parsed:
                continue
            url, ref = parsed
            yield RepoTask(index=line_idx, url=url, ref=ref)


def task_ref_value(ref: Optional[str]) -> str:
    return (ref or "").strip()


def task_key(url: str, ref: Optional[str]) -> str:
    return f"{url}@@{task_ref_value(ref)}"


def completed_state_statuses() -> Tuple[str, ...]:
    return ("success", "scan_failed", "scan_skipped")


def is_task_completed(conn: sqlite3.Connection, url: str, ref: Optional[str]) -> bool:
    statuses = completed_state_statuses()
    placeholders = ", ".join("?" for _ in statuses)
    row = conn.execute(
        f"SELECT 1 FROM repo_state WHERE repo_url = ? AND repo_ref = ? AND status IN ({placeholders}) LIMIT 1",
        (url, task_ref_value(ref), *statuses),
    ).fetchone()
    return row is not None


def _shuffle_key(task: RepoTask, seed: int) -> str:
    raw = f"{seed}\t{task.url}\t{task_ref_value(task.ref)}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def prepare_task_buckets(
    repos_file: Path,
    spool_dir: Path,
    shuffle_seed: int,
    shuffle_bucket_count: int,
    state_db_path: Path,
    state_enabled: bool,
) -> Dict[str, Any]:
    spool_dir.mkdir(parents=True, exist_ok=True)
    bucket_count = max(1, int(shuffle_bucket_count))
    bucket_paths = [spool_dir / f"bucket_{idx:04d}.jsonl" for idx in range(bucket_count)]
    seen_db_path = spool_dir / "seen_tasks.sqlite3"
    seen_conn = sqlite3.connect(str(seen_db_path))
    state_conn = sqlite3.connect(str(state_db_path)) if state_enabled and state_db_path.exists() else None
    stats = {
        "input_total": 0,
        "accepted_total": 0,
        "duplicate_total": 0,
        "skipped_completed": 0,
    }
    try:
        seen_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS seen_tasks (
                repo_url TEXT NOT NULL,
                repo_ref TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (repo_url, repo_ref)
            )
            """
        )
        seen_conn.commit()
        bucket_files = [path.open("w", encoding="utf-8") for path in bucket_paths]
        try:
            for task in iter_repo_tasks(repos_file):
                stats["input_total"] += 1
                inserted = seen_conn.execute(
                    "INSERT OR IGNORE INTO seen_tasks (repo_url, repo_ref) VALUES (?, ?)",
                    (task.url, task_ref_value(task.ref)),
                ).rowcount
                if not inserted:
                    stats["duplicate_total"] += 1
                    continue
                if state_conn is not None and is_task_completed(state_conn, task.url, task.ref):
                    stats["skipped_completed"] += 1
                    continue
                sort_key = _shuffle_key(task, shuffle_seed)
                bucket_idx = int(sort_key[:8], 16) % bucket_count
                payload = {
                    "sort_key": sort_key,
                    "index": task.index,
                    "url": task.url,
                    "ref": task.ref,
                }
                bucket_files[bucket_idx].write(json.dumps(payload, ensure_ascii=False) + "\n")
                stats["accepted_total"] += 1
            seen_conn.commit()
        finally:
            for fh in bucket_files:
                fh.close()
    finally:
        seen_conn.close()
        if state_conn is not None:
            state_conn.close()

    bucket_order = list(range(bucket_count))
    bucket_order.sort(key=lambda idx: hashlib.sha1(f"{shuffle_seed}:{idx}".encode("utf-8")).hexdigest())
    stats["bucket_paths"] = [str(bucket_paths[idx]) for idx in bucket_order if bucket_paths[idx].exists()]
    return stats


def iter_bucketed_tasks(bucket_paths: Sequence[str]) -> Iterator[RepoTask]:
    for bucket_path_str in bucket_paths:
        bucket_path = Path(bucket_path_str)
        if not bucket_path.exists():
            continue
        rows: List[Dict[str, Any]] = []
        with bucket_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                stripped = line.strip()
                if not stripped:
                    continue
                rows.append(json.loads(stripped))
        rows.sort(key=lambda item: (item["sort_key"], int(item["index"])))
        for item in rows:
            yield RepoTask(index=int(item["index"]), url=str(item["url"]), ref=item.get("ref"))


def init_state_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS repo_state (
                repo_url TEXT NOT NULL,
                repo_ref TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                message TEXT,
                run_id TEXT,
                result_file TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (repo_url, repo_ref)
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def load_completed_keys(db_path: Path) -> set[str]:
    if not db_path.exists():
        return set()
    conn = sqlite3.connect(str(db_path))
    try:
        statuses = completed_state_statuses()
        placeholders = ", ".join("?" for _ in statuses)
        rows = conn.execute(
            f"SELECT repo_url, repo_ref FROM repo_state WHERE status IN ({placeholders})",
            statuses,
        ).fetchall()
        return {task_key(url, ref) for (url, ref) in rows}
    finally:
        conn.close()


def update_repo_state(
    db_path: Path,
    repo_url: str,
    repo_ref: Optional[str],
    status: str,
    message: str,
    run_id: str,
    result_file: Optional[str],
) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO repo_state (
                repo_url, repo_ref, status, message, run_id, result_file, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(repo_url, repo_ref) DO UPDATE SET
                status=excluded.status,
                message=excluded.message,
                run_id=excluded.run_id,
                result_file=excluded.result_file,
                updated_at=excluded.updated_at
            """,
            (
                repo_url,
                task_ref_value(repo_ref),
                status,
                message[:2000] if message else "",
                run_id,
                result_file or "",
                now_iso(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def append_log_line(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")


def count_scan_candidates(repo_path: str) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    """
    Count candidate files using ScanCode's own traversal and ignore handling.
    This avoids local heuristics and keeps thresholding aligned with ScanCode input.
    """
    try:
        from scancode import cli  # pylint: disable=import-outside-toplevel
    except Exception as e:  # noqa: BLE001
        return None, None, f"import scancode failed: {type(e).__name__}: {e}"

    try:
        with open(os.devnull, "w", encoding="utf-8") as devnull:
            with contextlib.redirect_stdout(devnull), contextlib.redirect_stderr(devnull):
                _rc, results = cli.run_scan(
                    repo_path,
                    license=False,
                    info=True,
                    classify=False,
                    include=("*",),
                    ignore=DEFAULT_SCAN_IGNORE_PATTERNS,
                    facet=(),
                    strip_root=True,
                    return_results=True,
                    processes=0,
                )
    except Exception as e:  # noqa: BLE001
        return None, None, f"scancode candidate counting failed: {type(e).__name__}: {e}"

    files = results.get("files", [])
    total_files = 0
    total_bytes = 0
    for entry in files:
        if isinstance(entry, dict) and entry.get("type") == "file":
            total_files += 1
            size = entry.get("size")
            if isinstance(size, int):
                total_bytes += size
    return total_files, total_bytes, None


def should_skip_for_mirror(message: str) -> bool:
    lower_message = message.lower()
    return (
        "mirror repo info missing s3zipfilepath" in lower_message
        or "mirror lookup failed: runtimeerror: mirror repo info missing s3zipfilepath" in lower_message
        or "nosuchkey" in lower_message
        or "not found" in lower_message and "mirror" in lower_message
    )


def run_cmd(
    cmd: Sequence[str],
    timeout_sec: int,
    extra_env: Optional[Dict[str, str]] = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        list(cmd),
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout_sec,
        env=env,
    )


def clone_one(
    task: RepoTask,
    batch_clone_dir: Path,
    git_clone_depth: int,
    clone_timeout_sec: int,
    mirror_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    started = time.time()
    slug = repo_slug(task)
    target = batch_clone_dir / slug

    if mirror_cfg.get("enabled"):
        try:
            client = build_mirror_client(mirror_cfg)
            zip_key = fetch_mirror_zip_path(task, mirror_cfg)
        except Exception as e:  # noqa: BLE001
            return {
                "task": task,
                "success": False,
                "skipped": should_skip_for_mirror(f"mirror lookup failed: {type(e).__name__}: {e}"),
                "repo_path": None,
                "duration_sec": round(time.time() - started, 3),
                "message": f"mirror lookup failed: {type(e).__name__}: {e}",
            }

        bucket = mirror_cfg.get("bucket", "")
        if not bucket:
            return {
                "task": task,
                "success": False,
                "skipped": False,
                "repo_path": None,
                "duration_sec": round(time.time() - started, 3),
                "message": "mirror bucket is empty",
            }

        zip_path = target.with_suffix(".zip")
        try:
            client.download_file(bucket, zip_key, str(zip_path))
            repo_root = extract_zip(zip_path, target)
        except Exception as e:  # noqa: BLE001
            return {
                "task": task,
                "success": False,
                "skipped": should_skip_for_mirror(f"mirror download/extract failed: {zip_key}: {type(e).__name__}: {e}"),
                "repo_path": None,
                "duration_sec": round(time.time() - started, 3),
                "message": f"mirror download/extract failed: {zip_key}: {type(e).__name__}: {e}",
            }
        finally:
            if zip_path.exists():
                zip_path.unlink()

        return {
            "task": task,
            "success": True,
            "skipped": False,
            "repo_path": str(repo_root),
            "duration_sec": round(time.time() - started, 3),
            "message": f"ok (mirror: {zip_key})",
        }

    git_env = {"GIT_TERMINAL_PROMPT": "0"}
    cmd = ["git", "clone", "--depth", str(git_clone_depth), task.url, str(target)]
    try:
        cp = run_cmd(cmd, timeout_sec=clone_timeout_sec, extra_env=git_env)
    except subprocess.TimeoutExpired:
        return {
            "task": task,
            "success": False,
            "skipped": False,
            "repo_path": None,
            "duration_sec": round(time.time() - started, 3),
            "message": f"git clone timeout after {clone_timeout_sec}s",
        }
    if cp.returncode != 0:
        return {
            "task": task,
            "success": False,
            "skipped": False,
            "repo_path": None,
            "duration_sec": round(time.time() - started, 3),
            "message": f"git clone failed: {cp.stderr.strip()[:500]}",
        }

    if task.ref:
        try:
            cp_ref = run_cmd(
                ["git", "-C", str(target), "checkout", task.ref],
                timeout_sec=clone_timeout_sec,
                extra_env=git_env,
            )
        except subprocess.TimeoutExpired:
            return {
                "task": task,
                "success": False,
                "skipped": False,
                "repo_path": None,
                "duration_sec": round(time.time() - started, 3),
                "message": f"git checkout timeout after {clone_timeout_sec}s",
            }
        if cp_ref.returncode != 0:
            return {
                "task": task,
                "success": False,
                "skipped": False,
                "repo_path": None,
                "duration_sec": round(time.time() - started, 3),
                "message": f"git checkout failed: {cp_ref.stderr.strip()[:500]}",
            }

    return {
        "task": task,
        "success": True,
        "skipped": False,
        "repo_path": str(target),
        "duration_sec": round(time.time() - started, 3),
        "message": "ok",
    }


_WORKER_LICENSE_CHECK = None


def worker_init(src_dir: str, env_overrides: Dict[str, str]) -> None:
    for key, value in env_overrides.items():
        if value:
            os.environ[key] = value
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
    global _WORKER_LICENSE_CHECK
    from license_api import license_check  # pylint: disable=import-outside-toplevel

    _WORKER_LICENSE_CHECK = license_check


def _scan_worker(repo_path: str, queue: "mp.Queue[Tuple[bool, Dict[str, Any], str]]") -> None:
    try:
        assert _WORKER_LICENSE_CHECK is not None, "worker not initialized"
        with open(os.devnull, "w", encoding="utf-8") as devnull:
            with contextlib.redirect_stdout(devnull), contextlib.redirect_stderr(devnull):
                success, results, message = _WORKER_LICENSE_CHECK(repo_path)
    except Exception as e:  # noqa: BLE001
        success = False
        results = {}
        message = f"unexpected worker error: {type(e).__name__}: {e}"
    queue.put((success, results, message))


def _candidate_worker(repo_path: str, queue: "mp.Queue[Tuple[Optional[int], Optional[int], Optional[str]]]") -> None:
    try:
        with open(os.devnull, "w", encoding="utf-8") as devnull:
            with contextlib.redirect_stdout(devnull), contextlib.redirect_stderr(devnull):
                candidate_file_count, candidate_total_bytes, candidate_count_err = count_scan_candidates(repo_path)
    except Exception as e:  # noqa: BLE001
        candidate_file_count = None
        candidate_total_bytes = None
        candidate_count_err = f"candidate worker error: {type(e).__name__}: {e}"
    queue.put((candidate_file_count, candidate_total_bytes, candidate_count_err))


def candidate_one(args: Tuple[str, int]) -> Dict[str, Any]:
    repo_path, candidate_timeout_sec = args
    started = time.time()
    if candidate_timeout_sec > 0:
        queue: "mp.Queue[Tuple[Optional[int], Optional[int], Optional[str]]]" = mp.Queue()
        proc = mp.Process(target=_candidate_worker, args=(repo_path, queue))
        proc.start()
        proc.join(timeout=candidate_timeout_sec)
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=5)
            candidate_file_count = None
            candidate_total_bytes = None
            candidate_count_err = f"candidate counting timeout after {candidate_timeout_sec}s"
        else:
            try:
                candidate_file_count, candidate_total_bytes, candidate_count_err = queue.get_nowait()
            except Exception:  # noqa: BLE001
                candidate_file_count = None
                candidate_total_bytes = None
                candidate_count_err = "candidate worker exited without result"
    else:
        candidate_file_count, candidate_total_bytes, candidate_count_err = count_scan_candidates(repo_path)

    return {
        "repo_path": repo_path,
        "candidate_file_count": candidate_file_count,
        "candidate_total_bytes": candidate_total_bytes,
        "candidate_count_err": candidate_count_err,
        "duration_sec": round(time.time() - started, 3),
    }


def scan_one(args: Tuple[RepoTask, str, str, int]) -> Dict[str, Any]:
    task, repo_path, output_file, scan_timeout_sec = args
    started = time.time()
    if scan_timeout_sec > 0:
        queue: "mp.Queue[Tuple[bool, Dict[str, Any], str]]" = mp.Queue()
        proc = mp.Process(target=_scan_worker, args=(repo_path, queue))
        proc.start()
        proc.join(timeout=scan_timeout_sec)
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=5)
            success = False
            results = {}
            message = f"scan timeout after {scan_timeout_sec}s"
        else:
            try:
                success, results, message = queue.get_nowait()
            except Exception:  # noqa: BLE001
                success = False
                results = {}
                message = "scan worker exited without result"
    else:
        try:
            assert _WORKER_LICENSE_CHECK is not None, "worker not initialized"
            with open(os.devnull, "w", encoding="utf-8") as devnull:
                with contextlib.redirect_stdout(devnull), contextlib.redirect_stderr(devnull):
                    success, results, message = _WORKER_LICENSE_CHECK(repo_path)
        except Exception as e:  # noqa: BLE001
            success = False
            results = {}
            message = f"unexpected worker error: {type(e).__name__}: {e}"

    record = {
        "repo_url": task.url,
        "repo_ref": task.ref,
        "repo_path": repo_path,
        "success": success,
        "message": message,
        "duration_sec": round(time.time() - started, 3),
        "finished_at": now_iso(),
        "results": results if success else {},
    }
    out_path = Path(output_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)

    return {
        "repo_url": task.url,
        "repo_ref": task.ref,
        "repo_path": repo_path,
        "success": success,
        "message": message,
        "duration_sec": record["duration_sec"],
        "result_file": str(out_path),
    }


def build_oss_client(oss_cfg: Dict[str, Any]):
    provider = (oss_cfg.get("provider") or "s3").lower()
    if provider in ("aliyun", "aliyun-oss", "oss"):
        try:
            import oss2  # pylint: disable=import-outside-toplevel
        except ImportError as e:  # noqa: BLE001
            raise RuntimeError("oss2 is required for Aliyun OSS upload. Install requirements.batch.txt first.") from e

        access_key_id = oss_cfg.get("access_key_id", "")
        access_key_secret = oss_cfg.get("access_key_secret", "")
        if not access_key_id or not access_key_secret:
            raise RuntimeError("Aliyun OSS credentials are empty.")

        endpoint = oss_cfg["endpoint_url"]
        auth = oss2.Auth(access_key_id, access_key_secret)
        bucket = oss2.Bucket(auth, endpoint, oss_cfg["bucket"])
        return {"provider": "aliyun", "bucket": bucket}

    try:
        import boto3  # pylint: disable=import-outside-toplevel
    except ImportError as e:  # noqa: BLE001
        raise RuntimeError("boto3 is required for OSS upload. Install requirements.batch.txt first.") from e

    access_key = oss_cfg.get("access_key", "")
    secret_key = oss_cfg.get("secret_key", "")
    if not access_key or not secret_key:
        raise RuntimeError("OSS credentials are empty. Set access_key/secret_key or env vars in config.")

    client = boto3.client(
        "s3",
        endpoint_url=oss_cfg["endpoint_url"],
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=oss_cfg.get("region", "us-east-1"),
    )
    return {"provider": "s3", "client": client}


def upload_files_to_oss(
    oss_cfg: Dict[str, Any],
    local_files: Sequence[Path],
    key_prefix: str,
) -> Dict[str, Any]:
    upload_client = build_oss_client(oss_cfg)
    provider = upload_client["provider"]
    prefix = oss_cfg.get("prefix", "").strip("/")
    uploaded = 0
    failed: List[str] = []
    uploaded_urls: List[str] = []
    for file_path in local_files:
        key = "/".join(
            p for p in (prefix, key_prefix.strip("/"), file_path.name) if p
        )
        try:
            if provider == "aliyun":
                upload_client["bucket"].put_object_from_file(key, str(file_path))
            else:
                upload_client["client"].upload_file(str(file_path), oss_cfg["bucket"], key)
            uploaded += 1
            public_base_url = (oss_cfg.get("public_base_url") or "").rstrip("/")
            if public_base_url:
                uploaded_urls.append(f"{public_base_url}/{key}")
        except Exception as e:  # noqa: BLE001
            failed.append(f"{file_path.name}: {type(e).__name__}: {e}")
    return {"uploaded": uploaded, "failed": failed, "uploaded_urls": uploaded_urls}


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def with_scheme(endpoint: str) -> str:
    value = endpoint.strip()
    if not value:
        return value
    if value.startswith("http://") or value.startswith("https://"):
        return value
    return f"https://{value}"


def merged_section(cfg: Dict[str, Any], lower: str, upper: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    result.update(cfg.get(lower, {}))
    result.update(cfg.get(upper, {}))
    return result


def resolve_state_config(raw_state_cfg: Dict[str, Any], work_dir: Path) -> Dict[str, Any]:
    enabled = bool(raw_state_cfg.get("enabled", True))
    db_path = raw_state_cfg.get("db_path", str(work_dir / "scan_state.db"))
    return {
        "enabled": enabled,
        "db_path": str(Path(db_path).resolve()),
    }


def resolve_remote_config(raw_cfg: Dict[str, Any], run_id: str) -> Dict[str, Any]:
    enabled = bool(raw_cfg.get("enabled", False))
    host = str(raw_cfg.get("host", "")).strip()
    user = str(raw_cfg.get("user", "")).strip()
    base_dir = str(raw_cfg.get("base_dir", "")).rstrip("/")
    port = int(raw_cfg.get("port", 22))
    ssh_key = str(raw_cfg.get("ssh_key", "")).strip()
    use_sshpass = bool(raw_cfg.get("use_sshpass", False))
    password_env = str(raw_cfg.get("password_env", "REMOTE_PASSWORD"))
    password = os.getenv(password_env, "") or str(raw_cfg.get("password", ""))
    return {
        "enabled": enabled,
        "host": host,
        "user": user,
        "base_dir": base_dir,
        "port": port,
        "ssh_key": ssh_key,
        "use_sshpass": use_sshpass,
        "password": password,
        "password_env": password_env,
        "run_dir": f"{base_dir}/{run_id}" if base_dir else "",
    }


def build_ssh_cmd(remote_cfg: Dict[str, Any]) -> List[str]:
    cmd = ["ssh", "-p", str(remote_cfg["port"])]
    if remote_cfg.get("ssh_key"):
        cmd.extend(["-i", remote_cfg["ssh_key"]])
    return cmd


def ensure_remote_dir(remote_cfg: Dict[str, Any]) -> Optional[str]:
    if not (remote_cfg.get("host") and remote_cfg.get("user") and remote_cfg.get("run_dir")):
        return "remote config missing host/user/base_dir"
    remote = f"{remote_cfg['user']}@{remote_cfg['host']}"
    cmd = build_ssh_cmd(remote_cfg) + [remote, f"mkdir -p {shlex.quote(remote_cfg['run_dir'])}"]
    env = os.environ.copy()
    if remote_cfg.get("use_sshpass") and remote_cfg.get("password"):
        env["SSHPASS"] = remote_cfg["password"]
        cmd = ["sshpass", "-e"] + cmd
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
    except FileNotFoundError as e:
        return f"remote mkdir failed: {e}"
    except subprocess.CalledProcessError as e:
        msg = (e.stderr or e.stdout or str(e)).strip()
        return f"remote mkdir failed: {msg[:500]}"
    return None


def rsync_to_remote(remote_cfg: Dict[str, Any], sources: Sequence[Path]) -> Optional[str]:
    if not (remote_cfg.get("host") and remote_cfg.get("user") and remote_cfg.get("run_dir")):
        return "remote config missing host/user/base_dir"
    if not sources:
        return None
    remote = f"{remote_cfg['user']}@{remote_cfg['host']}:{remote_cfg['run_dir']}/"
    ssh_cmd = "ssh -p {}".format(remote_cfg["port"])
    if remote_cfg.get("ssh_key"):
        ssh_cmd += f" -i {shlex.quote(remote_cfg['ssh_key'])}"
    cmd = ["rsync", "-az", "--partial", "-e", ssh_cmd]
    cmd.extend(str(p) for p in sources)
    cmd.append(remote)
    env = os.environ.copy()
    if remote_cfg.get("use_sshpass") and remote_cfg.get("password"):
        env["SSHPASS"] = remote_cfg["password"]
        cmd = ["sshpass", "-e"] + cmd
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
    except FileNotFoundError as e:
        return f"rsync failed: {e}"
    except subprocess.CalledProcessError as e:
        msg = (e.stderr or e.stdout or str(e)).strip()
        return f"rsync failed: {msg[:500]}"
    return None


def resolve_oss_config(raw_oss_cfg: Dict[str, Any]) -> Dict[str, Any]:
    # Support both generic [oss] and Aliyun-style [OSS] keys.
    provider = str(raw_oss_cfg.get("provider", "")).lower()
    looks_like_aliyun = bool(
        raw_oss_cfg.get("access_key_id")
        or raw_oss_cfg.get("access_key_secret")
        or raw_oss_cfg.get("public_base_url")
    )
    if not provider:
        provider = "aliyun" if looks_like_aliyun else "s3"

    endpoint = os.getenv("OSS_ENDPOINT", "") or raw_oss_cfg.get("endpoint_url") or raw_oss_cfg.get("endpoint") or ""
    bucket = os.getenv("OSS_BUCKET", "") or raw_oss_cfg.get("bucket", "")

    if provider in ("aliyun", "aliyun-oss", "oss"):
        access_key_id = os.getenv("OSS_ACCESS_KEY_ID", "") or raw_oss_cfg.get("access_key_id", "")
        access_key_secret = os.getenv("OSS_ACCESS_KEY_SECRET", "") or raw_oss_cfg.get("access_key_secret", "")
        return {
            "provider": "aliyun",
            "enabled": bool(raw_oss_cfg.get("enabled", False)),
            "endpoint_url": with_scheme(str(endpoint)),
            "bucket": str(bucket),
            "prefix": str(raw_oss_cfg.get("prefix", "")).strip("/"),
            "access_key_id": str(access_key_id),
            "access_key_secret": str(access_key_secret),
            "public_base_url": str(raw_oss_cfg.get("public_base_url", "")).strip(),
        }

    access_key = os.getenv(raw_oss_cfg.get("access_key_env", ""), "") or raw_oss_cfg.get("access_key", "")
    secret_key = os.getenv(raw_oss_cfg.get("secret_key_env", ""), "") or raw_oss_cfg.get("secret_key", "")
    return {
        "provider": "s3",
        "enabled": bool(raw_oss_cfg.get("enabled", False)),
        "endpoint_url": with_scheme(str(endpoint)),
        "region": str(raw_oss_cfg.get("region", "us-east-1")),
        "bucket": str(bucket),
        "prefix": str(raw_oss_cfg.get("prefix", "")).strip("/"),
        "access_key": str(access_key),
        "secret_key": str(secret_key),
        "public_base_url": str(raw_oss_cfg.get("public_base_url", "")).strip(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch GitHub license scanner with OSS upload.")
    parser.add_argument(
        "--config",
        default="config/batch_scan.toml",
        help="Path to TOML config file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only parse tasks and print plan without cloning/scanning.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg_path = Path(args.config).resolve()
    if not cfg_path.exists():
        print(f"config not found: {cfg_path}")
        return 2

    cfg = read_toml(cfg_path)
    run_cfg = cfg.get("run", {})
    github_cfg = cfg.get("github", {})
    oss_cfg = resolve_oss_config(merged_section(cfg, "oss", "OSS"))
    mirror_cfg = resolve_mirror_config(merged_section(cfg, "mirror", "MIRROR"))
    mongo_cfg = cfg.get("mongodb", {})

    repos_file = Path(run_cfg.get("repos_file", "config/repos.txt")).resolve()
    if not repos_file.exists():
        print(f"repos file not found: {repos_file}")
        return 2

    batch_size = int(run_cfg.get("batch_size", 100))
    clone_workers = int(run_cfg.get("clone_workers", 8))
    scan_workers = int(run_cfg.get("scan_workers", os.cpu_count() or 4))
    candidate_workers = int(run_cfg.get("candidate_workers", scan_workers))
    scan_progress_step = int(run_cfg.get("scan_progress_step", 10))
    candidate_progress_step = int(run_cfg.get("candidate_progress_step", 10))
    candidate_timeout_sec = int(run_cfg.get("candidate_timeout_sec", 100))
    scan_timeout_sec = int(run_cfg.get("scan_timeout_sec", 0))
    scan_max_candidate_files = int(run_cfg.get("scan_max_candidate_files", run_cfg.get("scan_max_files", 0)))
    scan_max_candidate_bytes = int(run_cfg.get("scan_max_candidate_bytes", 0))
    shuffle_seed = int(run_cfg.get("shuffle_seed", 20260321))
    shuffle_bucket_count = int(run_cfg.get("shuffle_bucket_count", 256))
    clone_timeout_sec = int(github_cfg.get("clone_timeout_sec", 1800))
    git_clone_depth = int(github_cfg.get("git_clone_depth", 1))
    cleanup_clone = bool(run_cfg.get("cleanup_clone_after_batch", True))
    cleanup_local_result = bool(run_cfg.get("cleanup_local_result_after_upload", False))

    work_dir = ensure_dir(Path(run_cfg.get("work_dir", "/tmp/license-batch-run")).resolve())
    state_cfg = resolve_state_config(merged_section(cfg, "state", "STATE"), work_dir)
    run_id = now_run_id()
    run_dir = ensure_dir(work_dir / run_id)
    clone_root = ensure_dir(run_dir / "clones")
    result_root = ensure_dir(run_dir / "results")
    manifest_root = ensure_dir(run_dir / "manifests")
    spool_root = ensure_dir(run_dir / "task_spool")
    timeout_log_path = run_dir / "scan_timeouts.log"
    mirror_skip_log_path = run_dir / "mirror_skips.log"
    file_limit_skip_log_path = run_dir / "scan_candidate_file_limit_skips.log"
    remote_cfg = resolve_remote_config(merged_section(cfg, "remote", "REMOTE"), run_id)
    state_db_path = Path(state_cfg["db_path"])
    if state_cfg["enabled"]:
        init_state_db(state_db_path)
    task_prep = prepare_task_buckets(
        repos_file=repos_file,
        spool_dir=spool_root,
        shuffle_seed=shuffle_seed,
        shuffle_bucket_count=shuffle_bucket_count,
        state_db_path=state_db_path,
        state_enabled=bool(state_cfg["enabled"]),
    )

    print(f"config: {cfg_path}")
    print(f"repos: {repos_file}")
    print(f"input_tasks: {task_prep['input_total']}")
    print(
        f"total_tasks: {task_prep['accepted_total']} "
        f"(duplicates={task_prep['duplicate_total']} skipped_completed={task_prep['skipped_completed']})"
    )
    if state_cfg["enabled"]:
        print(f"state_db: {state_db_path}")
        print(
            f"batch_size: {batch_size}, clone_workers: {clone_workers}, "
            f"candidate_workers: {candidate_workers}, scan_workers: {scan_workers}"
        )
    if candidate_timeout_sec > 0:
        print(f"candidate_timeout_sec: {candidate_timeout_sec}")
    if scan_timeout_sec > 0:
        print(f"scan_timeout_sec: {scan_timeout_sec}")
    if scan_max_candidate_files > 0:
        print(f"scan_max_candidate_files: {scan_max_candidate_files}")
    if scan_max_candidate_bytes > 0:
        print(f"scan_max_candidate_bytes: {scan_max_candidate_bytes}")
    print(f"shuffle_seed: {shuffle_seed}, shuffle_bucket_count: {shuffle_bucket_count}")
    print(f"run_dir: {run_dir}")
    if mirror_cfg.get("enabled"):
        print(f"mirror: {mirror_cfg.get('endpoint_url')} bucket={mirror_cfg.get('bucket')} prefix={mirror_cfg.get('prefix')}")
    if remote_cfg.get("enabled"):
        print(f"remote: {remote_cfg.get('user')}@{remote_cfg.get('host')}:{remote_cfg.get('run_dir')}")
        remote_err = ensure_remote_dir(remote_cfg)
        if remote_err:
            print(f"remote warning: {remote_err}")

    if args.dry_run:
        return 0

    env_overrides = {
        "LANG": run_cfg.get("lang", DEFAULT_LANG),
        "LC_ALL": run_cfg.get("lc_all", DEFAULT_LANG),
        "SCANCODE_CACHE": run_cfg.get("scancode_cache", "/tmp/scancode-cache"),
        "SCANCODE_TEMP": run_cfg.get("scancode_temp", "/tmp/scancode-tmp"),
        "LICENSE_SCAN_BACKEND": str(run_cfg.get("license_scan_backend", "auto")),
        "MONGODB_HOST": str(mongo_cfg.get("host", "")),
        "MONGODB_PORT": str(mongo_cfg.get("port", "")),
        "MONGODB_USER": str(mongo_cfg.get("user", "")),
        "MONGODB_PASSWORD": str(mongo_cfg.get("password", "")),
        "MONGODB_DB": str(mongo_cfg.get("db", "")),
        "MONGODB_URL": str(mongo_cfg.get("url", "")),
    }
    for key, value in env_overrides.items():
        if value:
            os.environ[key] = value

    src_dir = str((Path(__file__).resolve().parent / "src").resolve())
    overall = {
        "run_id": run_id,
        "started_at": now_iso(),
        "finished_at": None,
        "total_tasks": int(task_prep["accepted_total"]),
        "input_tasks": int(task_prep["input_total"]),
        "duplicate_tasks": int(task_prep["duplicate_total"]),
        "skipped_completed": int(task_prep["skipped_completed"]),
        "total_success": 0,
        "total_failed": 0,
        "total_skipped": 0,
        "batches": [],
    }

    batch_count = 0
    task_iter = iter_bucketed_tasks(task_prep["bucket_paths"])
    default_ref = github_cfg.get("default_ref")
    if default_ref:
        task_iter = (
            RepoTask(index=t.index, url=t.url, ref=t.ref or default_ref)
            for t in task_iter
        )
    for batch_idx, batch_tasks in enumerate(chunked_iter(task_iter, batch_size), start=1):
        batch_count += 1
        batch_started = time.time()
        batch_name = f"batch_{batch_idx:06d}"
        batch_clone_dir = ensure_dir(clone_root / batch_name)
        batch_result_dir = ensure_dir(result_root / batch_name)
        batch_manifest = manifest_root / f"{batch_name}.json"

        print(f"[{batch_name}] cloning {len(batch_tasks)} repos...")
        clone_phase_started = time.time()
        clone_results: List[Dict[str, Any]] = []
        with futures.ThreadPoolExecutor(max_workers=clone_workers) as ex:
            futs = [
                ex.submit(
                    clone_one,
                    task,
                    batch_clone_dir,
                    git_clone_depth,
                    clone_timeout_sec,
                    mirror_cfg,
                )
                for task in batch_tasks
            ]
            for fut in futures.as_completed(futs):
                clone_results.append(fut.result())
        clone_phase_sec = round(time.time() - clone_phase_started, 3)

        to_scan: List[Tuple[RepoTask, str, str, int]] = []
        records: List[Dict[str, Any]] = []
        scan_candidates: List[Tuple[RepoTask, str, Path, float]] = []
        for item in clone_results:
            task = item["task"]
            result_file = batch_result_dir / f"{repo_slug(task)}.json"
            if not item["success"]:
                stage = "clone_skipped" if item.get("skipped") else "clone"
                failed_record = {
                    "repo_url": task.url,
                    "repo_ref": task.ref,
                    "repo_path": None,
                    "success": False,
                    "skipped": bool(item.get("skipped")),
                    "message": item["message"],
                    "duration_sec": item["duration_sec"],
                    "finished_at": now_iso(),
                    "results": {},
                    "stage": stage,
                }
                write_json(result_file, failed_record)
                records.append(
                    {
                        "repo_url": task.url,
                        "repo_ref": task.ref,
                        "repo_path": None,
                        "success": False,
                        "skipped": bool(item.get("skipped")),
                        "message": item["message"],
                        "duration_sec": item["duration_sec"],
                        "result_file": str(result_file),
                        "stage": stage,
                    }
                )
                if item.get("skipped"):
                    append_log_line(
                        mirror_skip_log_path,
                        json.dumps(
                            {
                                "batch": batch_name,
                                "repo_url": task.url,
                                "repo_ref": task.ref,
                                "message": item["message"],
                                "finished_at": now_iso(),
                            },
                            ensure_ascii=False,
                        ),
                    )
                if state_cfg["enabled"]:
                    update_repo_state(
                        db_path=state_db_path,
                        repo_url=task.url,
                        repo_ref=task.ref,
                        status="clone_skipped" if item.get("skipped") else "clone_failed",
                        message=item["message"],
                        run_id=run_id,
                        result_file=str(result_file),
                    )
                continue
            repo_path = str(item["repo_path"])
            scan_candidates.append((task, repo_path, result_file, float(item["duration_sec"])))

        candidate_phase_started = time.time()
        candidate_phase_sec = 0.0
        candidate_results: Dict[str, Dict[str, Any]] = {}
        if scan_candidates and (scan_max_candidate_files > 0 or scan_max_candidate_bytes > 0):
            print(f"[{batch_name}] counting scan candidates for {len(scan_candidates)} repos in parallel...")
            candidate_done = 0
            with futures.ThreadPoolExecutor(max_workers=max(1, candidate_workers)) as ex:
                submitted = {
                    ex.submit(candidate_one, (repo_path, candidate_timeout_sec)): repo_path
                    for _task, repo_path, _result_file, _clone_duration in scan_candidates
                }
                for fut in futures.as_completed(submitted):
                    repo_path = submitted[fut]
                    try:
                        result = fut.result()
                    except Exception as e:  # noqa: BLE001
                        result = {
                            "repo_path": repo_path,
                            "candidate_file_count": None,
                            "candidate_total_bytes": None,
                            "candidate_count_err": f"candidate future error: {type(e).__name__}: {e}",
                            "duration_sec": 0.0,
                        }
                    candidate_results[repo_path] = result
                    candidate_done += 1
                    if candidate_done == len(scan_candidates) or (
                        candidate_progress_step > 0 and candidate_done % candidate_progress_step == 0
                    ):
                        print(f"[{batch_name}] candidate progress: {candidate_done}/{len(scan_candidates)}")
            candidate_phase_sec = round(time.time() - candidate_phase_started, 3)

        for task, repo_path, result_file, clone_duration_sec in scan_candidates:
            candidate_result = candidate_results.get(repo_path, {})
            candidate_file_count = candidate_result.get("candidate_file_count")
            candidate_total_bytes = candidate_result.get("candidate_total_bytes")
            candidate_count_err = candidate_result.get("candidate_count_err")
            candidate_duration_sec = float(candidate_result.get("duration_sec", 0.0) or 0.0)
            if scan_max_candidate_files > 0 or scan_max_candidate_bytes > 0:
                if candidate_count_err:
                    append_log_line(
                        file_limit_skip_log_path,
                        json.dumps(
                            {
                                "batch": batch_name,
                                "repo_url": task.url,
                                "repo_ref": task.ref,
                                "repo_path": repo_path,
                                "candidate_file_count": candidate_file_count,
                                "candidate_total_bytes": candidate_total_bytes,
                                "candidate_duration_sec": candidate_duration_sec,
                                "message": candidate_count_err,
                                "finished_at": now_iso(),
                            },
                            ensure_ascii=False,
                        ),
                    )
                elif (
                    scan_max_candidate_files > 0
                    and candidate_file_count is not None
                    and candidate_file_count > scan_max_candidate_files
                ):
                    message = (
                        "scan skipped: candidate file count "
                        f"{candidate_file_count} exceeds limit {scan_max_candidate_files}"
                    )
                    skipped_record = {
                        "repo_url": task.url,
                        "repo_ref": task.ref,
                        "repo_path": repo_path,
                        "success": False,
                        "skipped": True,
                        "message": message,
                        "duration_sec": round(clone_duration_sec + candidate_duration_sec, 3),
                        "finished_at": now_iso(),
                        "results": {},
                        "stage": "scan_skipped",
                        "candidate_file_count": candidate_file_count,
                        "candidate_total_bytes": candidate_total_bytes,
                        "clone_duration_sec": clone_duration_sec,
                        "candidate_duration_sec": candidate_duration_sec,
                    }
                    write_json(result_file, skipped_record)
                    records.append(
                        {
                            "repo_url": task.url,
                            "repo_ref": task.ref,
                            "repo_path": repo_path,
                            "success": False,
                            "skipped": True,
                            "message": message,
                            "duration_sec": round(clone_duration_sec + candidate_duration_sec, 3),
                            "result_file": str(result_file),
                            "stage": "scan_skipped",
                            "candidate_file_count": candidate_file_count,
                            "candidate_total_bytes": candidate_total_bytes,
                            "clone_duration_sec": clone_duration_sec,
                            "candidate_duration_sec": candidate_duration_sec,
                        }
                    )
                    append_log_line(
                        file_limit_skip_log_path,
                        json.dumps(
                            {
                                "batch": batch_name,
                                "repo_url": task.url,
                                "repo_ref": task.ref,
                                "repo_path": repo_path,
                                "candidate_file_count": candidate_file_count,
                                "candidate_total_bytes": candidate_total_bytes,
                                "candidate_duration_sec": candidate_duration_sec,
                                "scan_max_candidate_files": scan_max_candidate_files,
                                "scan_max_candidate_bytes": scan_max_candidate_bytes,
                                "message": message,
                                "finished_at": now_iso(),
                            },
                            ensure_ascii=False,
                        ),
                    )
                    if state_cfg["enabled"]:
                        update_repo_state(
                            db_path=state_db_path,
                            repo_url=task.url,
                            repo_ref=task.ref,
                            status="scan_skipped",
                            message=message,
                            run_id=run_id,
                            result_file=str(result_file),
                        )
                    continue
                elif (
                    scan_max_candidate_bytes > 0
                    and candidate_total_bytes is not None
                    and candidate_total_bytes > scan_max_candidate_bytes
                ):
                    message = (
                        "scan skipped: candidate total bytes "
                        f"{candidate_total_bytes} exceeds limit {scan_max_candidate_bytes}"
                    )
                    skipped_record = {
                        "repo_url": task.url,
                        "repo_ref": task.ref,
                        "repo_path": repo_path,
                        "success": False,
                        "skipped": True,
                        "message": message,
                        "duration_sec": round(clone_duration_sec + candidate_duration_sec, 3),
                        "finished_at": now_iso(),
                        "results": {},
                        "stage": "scan_skipped",
                        "candidate_file_count": candidate_file_count,
                        "candidate_total_bytes": candidate_total_bytes,
                        "clone_duration_sec": clone_duration_sec,
                        "candidate_duration_sec": candidate_duration_sec,
                    }
                    write_json(result_file, skipped_record)
                    records.append(
                        {
                            "repo_url": task.url,
                            "repo_ref": task.ref,
                            "repo_path": repo_path,
                            "success": False,
                            "skipped": True,
                            "message": message,
                            "duration_sec": round(clone_duration_sec + candidate_duration_sec, 3),
                            "result_file": str(result_file),
                            "stage": "scan_skipped",
                            "candidate_file_count": candidate_file_count,
                            "candidate_total_bytes": candidate_total_bytes,
                            "clone_duration_sec": clone_duration_sec,
                            "candidate_duration_sec": candidate_duration_sec,
                        }
                    )
                    append_log_line(
                        file_limit_skip_log_path,
                        json.dumps(
                            {
                                "batch": batch_name,
                                "repo_url": task.url,
                                "repo_ref": task.ref,
                                "repo_path": repo_path,
                                "candidate_file_count": candidate_file_count,
                                "candidate_total_bytes": candidate_total_bytes,
                                "candidate_duration_sec": candidate_duration_sec,
                                "scan_max_candidate_files": scan_max_candidate_files,
                                "scan_max_candidate_bytes": scan_max_candidate_bytes,
                                "message": message,
                                "finished_at": now_iso(),
                            },
                            ensure_ascii=False,
                        ),
                    )
                    if state_cfg["enabled"]:
                        update_repo_state(
                            db_path=state_db_path,
                            repo_url=task.url,
                            repo_ref=task.ref,
                            status="scan_skipped",
                            message=message,
                            run_id=run_id,
                            result_file=str(result_file),
                        )
                    continue
            to_scan.append((task, repo_path, str(result_file), scan_timeout_sec))

        scan_phase_started = time.time()
        scan_phase_sec = 0.0
        print(f"[{batch_name}] scanning {len(to_scan)} repos in parallel...")
        if to_scan:
            scan_total = len(to_scan)
            scan_done = 0
            with futures.ProcessPoolExecutor(
                max_workers=scan_workers,
                initializer=worker_init,
                initargs=(src_dir, env_overrides),
            ) as ex:
                submitted = {ex.submit(scan_one, one): one for one in to_scan}
                for fut in futures.as_completed(submitted):
                    task, repo_path, output_file, _scan_timeout = submitted[fut]
                    try:
                        result = fut.result()
                    except Exception as e:  # noqa: BLE001
                        result = {
                            "repo_url": task.url,
                            "repo_ref": task.ref,
                            "repo_path": repo_path,
                            "success": False,
                            "message": f"worker future error: {type(e).__name__}: {e}",
                            "duration_sec": 0.0,
                            "result_file": output_file,
                        }
                    result["stage"] = "scan"
                    records.append(result)
                    if result.get("message", "").startswith("scan timeout after "):
                        append_log_line(
                            timeout_log_path,
                            json.dumps(
                                {
                                    "batch": batch_name,
                                    "repo_url": result["repo_url"],
                                    "repo_ref": result.get("repo_ref"),
                                    "repo_path": result.get("repo_path"),
                                    "result_file": result.get("result_file"),
                                    "message": result.get("message", ""),
                                    "finished_at": result.get("finished_at"),
                                },
                                ensure_ascii=False,
                            ),
                        )
                    if state_cfg["enabled"]:
                        update_repo_state(
                            db_path=state_db_path,
                            repo_url=result["repo_url"],
                            repo_ref=result.get("repo_ref"),
                            status="success" if result["success"] else "scan_failed",
                            message=result.get("message", ""),
                            run_id=run_id,
                            result_file=result.get("result_file"),
                        )
                    scan_done += 1
                    if scan_done == scan_total or (scan_progress_step > 0 and scan_done % scan_progress_step == 0):
                        print(f"[{batch_name}] scan progress: {scan_done}/{scan_total}")
            scan_phase_sec = round(time.time() - scan_phase_started, 3)

        success_count = sum(1 for r in records if r["success"])
        skipped_count = sum(1 for r in records if r.get("skipped"))
        fail_count = len(records) - success_count - skipped_count
        batch_data = {
            "batch": batch_name,
            "started_at": now_iso(),
            "duration_sec": round(time.time() - batch_started, 3),
            "total": len(records),
            "success": success_count,
            "skipped": skipped_count,
            "failed": fail_count,
            "clone_wall_sec": clone_phase_sec,
            "candidate_wall_sec": candidate_phase_sec,
            "scan_wall_sec": scan_phase_sec,
            "records": records,
        }
        write_json(batch_manifest, batch_data)

        upload_status = None
        if oss_cfg.get("enabled", False):
            upload_files: List[Path] = [batch_manifest]
            upload_files.extend(Path(r["result_file"]) for r in records if r.get("result_file"))
            print(f"[{batch_name}] uploading {len(upload_files)} files to OSS...")
            upload_status = upload_files_to_oss(
                oss_cfg=oss_cfg,
                local_files=upload_files,
                key_prefix=f"{run_id}/{batch_name}",
            )
            print(
                f"[{batch_name}] upload done: uploaded={upload_status['uploaded']} "
                f"failed={len(upload_status['failed'])}"
            )

        if remote_cfg.get("enabled"):
            sync_targets: List[Path] = [manifest_root, result_root]
            if mirror_skip_log_path.exists():
                sync_targets.append(mirror_skip_log_path)
            if file_limit_skip_log_path.exists():
                sync_targets.append(file_limit_skip_log_path)
            if timeout_log_path.exists():
                sync_targets.append(timeout_log_path)
            remote_err = rsync_to_remote(remote_cfg, sync_targets)
            if remote_err:
                print(f"[{batch_name}] remote sync warning: {remote_err}")

        if cleanup_clone and batch_clone_dir.exists():
            shutil.rmtree(batch_clone_dir, ignore_errors=True)
        if cleanup_local_result and batch_result_dir.exists():
            shutil.rmtree(batch_result_dir, ignore_errors=True)

        overall["total_success"] += success_count
        overall["total_failed"] += fail_count
        overall["total_skipped"] += skipped_count
        overall["batches"].append(
            {
                "batch": batch_name,
                "total": len(records),
                "success": success_count,
                "skipped": skipped_count,
                "failed": fail_count,
                "manifest": str(batch_manifest),
                "upload": upload_status,
            }
        )
        print(
            f"[{batch_name}] finished: success={success_count} skipped={skipped_count} failed={fail_count} "
            f"duration={batch_data['duration_sec']}s"
        )

    overall["finished_at"] = now_iso()
    overall["batch_count"] = batch_count
    overall_path = run_dir / "run_summary.json"
    write_json(overall_path, overall)
    print(f"run summary: {overall_path}")
    print(
        f"total success={overall['total_success']} "
        f"skipped={overall['total_skipped']} failed={overall['total_failed']}"
    )
    if remote_cfg.get("enabled"):
        sync_targets: List[Path] = [overall_path, manifest_root, result_root]
        if mirror_skip_log_path.exists():
            sync_targets.append(mirror_skip_log_path)
        if file_limit_skip_log_path.exists():
            sync_targets.append(file_limit_skip_log_path)
        if timeout_log_path.exists():
            sync_targets.append(timeout_log_path)
        remote_err = rsync_to_remote(remote_cfg, sync_targets)
        if remote_err:
            print(f"remote sync warning: {remote_err}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
