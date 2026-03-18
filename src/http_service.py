import hashlib
import os
import re
import shutil
import subprocess
import traceback
from ctypes import CDLL
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from pymongo import ASCENDING, MongoClient

from localconfig import local_mongodb_db, local_mongodb_host, local_mongodb_port, local_mongodb_url


DEFAULT_HOST = os.getenv("LICENSE_API_HOST", "0.0.0.0")
DEFAULT_PORT = int(os.getenv("LICENSE_API_PORT", "8002"))
WORK_DIR = Path(os.getenv("LICENSE_API_WORK_DIR", "/tmp/license-api-work"))
MAX_SEARCH_RESULTS = 200

TASK_COLL_NAME = os.getenv("LICENSE_TASK_COLLECTION", "license_api_tasks")
LATEST_COLL_NAME = os.getenv("LICENSE_LATEST_COLLECTION", "license_api_repo_latest")

_MONGO_CLIENT: MongoClient | None = None
_COLL_TASKS = None
_COLL_LATEST = None
_MONGO_URL_IN_USE: str | None = None

_GITHUB_PATH_RE = re.compile(r"^/([^/]+)/([^/]+?)(?:\.git)?/?$")


def _ensure_icu_runtime_path() -> None:
    try:
        CDLL("libicui18n.so.78")
        return
    except OSError:
        pass

    home = Path.home()
    candidates: list[Path] = [
        Path(os.getenv("CONDA_PREFIX", "")) / "lib",
        home / "miniconda3" / "envs" / "lc" / "lib",
        home / "miniconda3" / "lib",
        Path("/opt/conda/lib"),
    ]
    for p in Path("/home").glob("*/miniconda3/envs/*/lib"):
        candidates.append(p)
    for p in Path("/home").glob("*/miniconda3/lib"):
        candidates.append(p)

    current = os.getenv("LD_LIBRARY_PATH", "")
    parts = [p for p in current.split(":") if p]

    for cand in candidates:
        if not cand.exists():
            continue
        if any(cand.glob("libicui18n.so*")) and str(cand) not in parts:
            parts.insert(0, str(cand))
            os.environ["LD_LIBRARY_PATH"] = ":".join(parts)
            break


def prepare_env() -> None:
    os.environ.setdefault("LANG", "C.UTF-8")
    os.environ.setdefault("LC_ALL", "C.UTF-8")
    os.environ.setdefault("SCANCODE_CACHE", "/tmp/scancode-cache")
    os.environ.setdefault("SCANCODE_TEMP", "/tmp/scancode-tmp")
    _ensure_icu_runtime_path()


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _repo_meta(repo_url: str) -> tuple[str, int]:
    parsed = urlparse(repo_url.strip())
    if parsed.scheme not in ("http", "https"):
        raise ValueError("repo_url must start with http:// or https://")
    if parsed.netloc.lower() != "github.com":
        raise ValueError("repo_url must be a GitHub repository URL")

    m = _GITHUB_PATH_RE.match(parsed.path.strip())
    if not m:
        raise ValueError("repo_url format is invalid")

    repo_name = f"{m.group(1)}/{m.group(2)}"
    repo_id = int(hashlib.sha1(repo_name.encode("utf-8")).hexdigest()[:8], 16)
    return repo_name, repo_id


def _task_public_status(task: dict[str, Any]) -> str:
    raw = task.get("task_status", "not_found")
    if raw in ("submit", "running"):
        return "submit"
    return raw


def _clone_repo(repo_url: str, target_dir: Path) -> None:
    if target_dir.exists():
        shutil.rmtree(target_dir, ignore_errors=True)
    target_dir.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "git",
        "clone",
        "--depth",
        "1",
        repo_url,
        str(target_dir),
    ]
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=1800,
    )
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        detail = stderr or stdout or f"git clone failed with code {proc.returncode}"
        raise RuntimeError(detail)


def _mongo_collections():
    global _MONGO_CLIENT, _COLL_TASKS, _COLL_LATEST, _MONGO_URL_IN_USE
    if _COLL_TASKS is not None and _COLL_LATEST is not None:
        return _COLL_TASKS, _COLL_LATEST

    url_candidates: list[str] = []
    custom_url = os.getenv("LICENSE_API_MONGODB_URL", "").strip()
    if custom_url:
        url_candidates.append(custom_url)
    url_candidates.extend(
        [
            local_mongodb_url,
            f"mongodb://{local_mongodb_host}:{local_mongodb_port}/{local_mongodb_db}",
            f"mongodb://{local_mongodb_host}:{local_mongodb_port}",
        ]
    )

    last_exc: Exception | None = None
    selected_client: MongoClient | None = None
    for url in dict.fromkeys(url_candidates):
        try:
            client = MongoClient(url, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            selected_client = client
            _MONGO_URL_IN_USE = url
            break
        except Exception as exc:
            last_exc = exc
            continue

    if selected_client is None:
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("no mongodb url candidates available")

    _MONGO_CLIENT = selected_client
    db = _MONGO_CLIENT[local_mongodb_db]
    _COLL_TASKS = db[TASK_COLL_NAME]
    _COLL_LATEST = db[LATEST_COLL_NAME]

    _COLL_TASKS.create_index([("task_id", ASCENDING)], unique=True)
    _COLL_TASKS.create_index([("repo_url", ASCENDING)])
    _COLL_TASKS.create_index([("updated_at", ASCENDING)])

    _COLL_LATEST.create_index([("repo_url", ASCENDING)], unique=True)
    _COLL_LATEST.create_index([("repo_name", ASCENDING)])
    _COLL_LATEST.create_index([("timestamp", ASCENDING)])
    return _COLL_TASKS, _COLL_LATEST


def _collections_or_503():
    try:
        return _mongo_collections()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"mongodb unavailable: {exc}") from exc


def _run_license_task(task_id: str) -> None:
    prepare_env()
    try:
        coll_tasks, coll_latest = _mongo_collections()
    except Exception:
        return

    task = coll_tasks.find_one({"task_id": task_id}, {"_id": 0})
    if not task:
        return

    coll_tasks.update_one(
        {"task_id": task_id},
        {
            "$set": {
                "task_status": "running",
                "updated_at": _ts(),
                "error": None,
            }
        },
    )

    repo_url = task["repo_url"]
    repo_name = task["repo_name"]
    repo_id = task["repo_id"]
    repo_dir = WORK_DIR / task_id / "repo"

    try:
        _clone_repo(repo_url, repo_dir)

        from http_license_api import license_check

        success, results, message = license_check(str(repo_dir))
        if not success:
            raise RuntimeError(message)

        final_results = results or {}
        now = _ts()
        coll_tasks.update_one(
            {"task_id": task_id},
            {
                "$set": {
                    "task_status": "success",
                    "result": final_results,
                    "updated_at": now,
                    "error": None,
                }
            },
        )
        coll_latest.update_one(
            {"repo_url": repo_url},
            {
                "$set": {
                    "task_id": task_id,
                    "repo_url": repo_url,
                    "repo_name": repo_name,
                    "repo_id": repo_id,
                    "results": final_results,
                    "timestamp": now,
                }
            },
            upsert=True,
        )
    except Exception as exc:
        now = _ts()
        err = str(exc).strip() or traceback.format_exc()
        coll_tasks.update_one(
            {"task_id": task_id},
            {
                "$set": {
                    "task_status": "failed",
                    "updated_at": now,
                    "error": err,
                }
            },
        )
    finally:
        shutil.rmtree(repo_dir.parent, ignore_errors=True)


class RepoUrlRequest(BaseModel):
    repo_url: str = Field(..., description="github 仓库地址")


app = FastAPI(
    title="License Analysis API",
    version="1.0.0",
    description="许可证分析接口服务",
)


@app.on_event("startup")
def on_startup() -> None:
    prepare_env()
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    try:
        coll_tasks, _ = _mongo_collections()
        coll_tasks.update_many(
            {"task_status": {"$in": ["submit", "running"]}},
            {
                "$set": {
                    "task_status": "failed",
                    "error": "service restarted before task completion",
                    "updated_at": _ts(),
                }
            },
        )
    except Exception:
        # Mongo connectivity is checked per request to avoid blocking service startup.
        pass


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "success",
        "service": "license-analysis",
        "port": DEFAULT_PORT,
        "timestamp": _ts(),
    }


@app.post("/api/license/v1/metrics/generate")
def create_generate_task(
    payload: RepoUrlRequest, background_tasks: BackgroundTasks
) -> dict[str, Any]:
    repo_url = payload.repo_url.strip()
    try:
        repo_name, repo_id = _repo_meta(repo_url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    coll_tasks, _ = _collections_or_503()
    task_id = f"task_{hashlib.sha1((repo_url + _ts()).encode('utf-8')).hexdigest()[:8]}"
    now = _ts()
    task = {
        "task_id": task_id,
        "repo_url": repo_url,
        "repo_name": repo_name,
        "repo_id": repo_id,
        "task_status": "submit",
        "created_at": now,
        "updated_at": now,
        "result": None,
        "error": None,
    }
    coll_tasks.update_one({"task_id": task_id}, {"$set": task}, upsert=True)
    background_tasks.add_task(_run_license_task, task_id)

    return {
        "status": "submitted",
        "message": "License profiling task submitted successfully",
        "task_id": task_id,
        "repo_name": repo_name,
        "repo_id": repo_id,
        "upstream_status": 200,
        "timestamp": now,
    }


@app.get("/api/license/v1/task/{task_id}")
def query_task_status(task_id: str) -> dict[str, Any]:
    coll_tasks, _ = _collections_or_503()
    task = coll_tasks.find_one({"task_id": task_id}, {"_id": 0})
    if not task:
        return {
            "status": "success",
            "task_id": task_id,
            "task_status": "not_found",
            "timestamp": _ts(),
        }

    resp = {
        "status": "success",
        "task_id": task_id,
        "task_status": _task_public_status(task),
        "timestamp": task.get("updated_at", _ts()),
    }
    if task.get("task_status") == "failed" and task.get("error"):
        resp["error"] = task["error"]
    return resp


@app.get("/api/license/v1/result/{task_id}")
def get_task_result(task_id: str) -> dict[str, Any]:
    coll_tasks, _ = _collections_or_503()
    task = coll_tasks.find_one({"task_id": task_id}, {"_id": 0})
    if not task:
        return {
            "status": "success",
            "task_id": task_id,
            "task_status": "not_found",
            "timestamp": _ts(),
        }

    status = _task_public_status(task)
    if status == "submit":
        return {
            "status": "success",
            "task_id": task_id,
            "task_status": "submit",
            "timestamp": task.get("updated_at", _ts()),
        }
    if status == "failed":
        return {
            "status": "success",
            "task_id": task_id,
            "task_status": "failed",
            "error": task.get("error", ""),
            "timestamp": task.get("updated_at", _ts()),
        }

    return {
        "status": "success",
        "task_id": task_id,
        "task_status": "success",
        "repo_name": task.get("repo_name"),
        "repo_id": task.get("repo_id"),
        "repo_url": task.get("repo_url"),
        "results": task.get("result") or {},
        "timestamp": task.get("updated_at", _ts()),
    }


@app.post("/api/license/v1/metrics/query")
def query_metrics(payload: RepoUrlRequest) -> dict[str, Any]:
    repo_url = payload.repo_url.strip()
    try:
        _repo_meta(repo_url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    _, coll_latest = _collections_or_503()
    rec = coll_latest.find_one({"repo_url": repo_url}, {"_id": 0})
    if not rec:
        return {
            "status": "success",
            "repo_url": repo_url,
            "result_status": "not_found",
            "timestamp": _ts(),
        }
    return {
        "status": "success",
        "repo_url": rec.get("repo_url"),
        "task_id": rec.get("task_id"),
        "task_status": "success",
        "repo_name": rec.get("repo_name"),
        "repo_id": rec.get("repo_id"),
        "results": rec.get("results") or {},
        "timestamp": rec.get("timestamp", _ts()),
    }


@app.get("/api/license/v1/metrics/repos/count")
def query_repo_count() -> dict[str, Any]:
    _, coll_latest = _collections_or_503()
    repo_count = coll_latest.count_documents({})
    return {
        "status": "success",
        "repo_count": repo_count,
        "timestamp": _ts(),
    }


@app.get("/api/license/v1/metrics/repos/search")
def search_repos(keyword: str = Query(..., description="仓库名称关键词")) -> dict[str, Any]:
    _, coll_latest = _collections_or_503()
    key = keyword.strip()
    regex = re.escape(key)
    cursor = (
        coll_latest.find(
            {"repo_name": {"$regex": regex, "$options": "i"}},
            {"_id": 0, "repo_id": 1, "repo_name": 1, "repo_url": 1},
        )
        .sort("repo_name", ASCENDING)
        .limit(MAX_SEARCH_RESULTS)
    )
    rows = [
        {
            "repo_id": item.get("repo_id"),
            "repo_name": item.get("repo_name"),
            "url": item.get("repo_url"),
        }
        for item in cursor
    ]
    return {
        "status": "success",
        "keyword": keyword,
        "count": len(rows),
        "repos": rows,
        "timestamp": _ts(),
    }
