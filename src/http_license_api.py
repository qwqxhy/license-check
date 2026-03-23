# -*- coding: utf-8 -*-
import json
import os
import shutil
import subprocess
import sys
import tempfile
import traceback

from pymongo import MongoClient

import licensedb
from light_scan import append_scan_error, run_fast_scan
from localconfig import local_mongodb_db, local_mongodb_host, local_mongodb_port, local_mongodb_url
from ltree import LTree


ignores_pattern = (
    '.git/', '.github/', '.idea/',
    '*.pdf', '*.jpg', '*.jpeg', '*.png', '*.gif', '*.bmp',
    '*.mp3', '*.mp4', '*.avi', '*.WAV', '*.MOV', '*.mid', '*.cda', '*.rmvb',
)


def _run_scancode(path: str):
    local_scancode = os.path.join(os.path.dirname(sys.executable), "scancode")
    scancode_cmd = local_scancode if os.path.isfile(local_scancode) else (shutil.which("scancode") or "scancode")

    with tempfile.TemporaryDirectory(prefix="scancode-out-") as tmp_dir:
        out_json = os.path.join(tmp_dir, "scan-result.json")
        cmd = [
            scancode_cmd,
            "--license",
            "--info",
            "--classify",
            "--strip-root",
            "--json",
            out_json,
        ]
        for pattern in ignores_pattern:
            cmd.extend(["--ignore", pattern])
        cmd.append(path)

        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=7200,
        )

        if not os.path.exists(out_json):
            stderr = (proc.stderr or "").strip()
            stdout = (proc.stdout or "").strip()
            detail = stderr or stdout or f"scancode failed with code {proc.returncode}"
            raise RuntimeError(detail)

        with open(out_json, "r", encoding="utf-8") as fp:
            results = json.load(fp)

        return proc.returncode, results


def _scan_backend() -> str:
    backend = os.getenv("LICENSE_SCAN_BACKEND", "auto").strip().lower()
    if backend in {"fast", "scancode", "auto"}:
        return backend
    return "auto"


def _run_scan(path: str):
    backend = _scan_backend()
    if backend == "scancode":
        rc, results = _run_scancode(path)
        results["scan_backend"] = "scancode"
        return rc, results

    _rc, fast_results, fast_meta = run_fast_scan(path, ignores_pattern)
    if backend == "fast" or not fast_meta["needs_fallback"]:
        return 0, fast_results

    try:
        rc, results = _run_scancode(path)
        results["scan_backend"] = "scancode"
        results["scan_meta"] = {"fallback_from": "fast", "fallback_reasons": fast_meta["fallback_reasons"]}
        return rc, results
    except Exception:
        append_scan_error(
            fast_results,
            "fast scan requested scancode fallback but scancode command is unavailable; using fast result only",
        )
        return 0, fast_results


def _collect_scan_errors(results):
    scan_errors = []
    headers = results.get("headers", []) if isinstance(results, dict) else []
    for header in headers:
        scan_errors.extend(header.get("errors") or [])
    return scan_errors


def _mongo_url_for_http() -> str:
    url_candidates = []
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

    for url in dict.fromkeys(url_candidates):
        try:
            client = MongoClient(url, serverSelectionTimeoutMS=3000)
            client.admin.command("ping")
            return url
        except Exception:
            continue
    return local_mongodb_url


def _prepare_licensedb_for_http() -> None:
    # Keep original source code unchanged, override runtime connection
    # only inside HTTP service process.
    licensedb.local_mongodb_url = _mongo_url_for_http()
    licensedb.MongoClintSingleton._instance = None


def license_check(codebase):
    success = True
    result = {}
    message = '0'
    path = codebase

    if not os.path.exists(codebase):
        message = 'path: {}: not exists'.format(codebase)
        success = False
        return success, result, message

    file_num = len(os.listdir(codebase))
    if file_num == 0:
        message = 'path: {}: is an empty directory'.format(codebase)
        success = False
        return success, result, message

    if file_num == 1:
        file = os.listdir(codebase)[0]
        if os.path.isdir(os.path.join(codebase, file)):
            path = os.path.join(codebase, file)

    try:
        _, results = _run_scan(path)
    except Exception:
        success = False
        message = 'scancode cli error: {}: \nException: {}'.format(codebase, traceback.format_exc())
        return success, result, message

    files = results.get("files", [])
    if not files:
        success = False
        message = 'scancode cli error: no file scan results'
        return success, result, message

    scan_errors = _collect_scan_errors(results)

    _prepare_licensedb_for_http()
    ltree = LTree()
    build_success, message = ltree.build(files)
    success = success and build_success
    if not build_success:
        return success, result, message

    detect_success, message = ltree.detect()
    success = success and detect_success
    if detect_success:
        result = ltree.get_result()
        result["scan_backend"] = results.get("scan_backend", _scan_backend())
        if scan_errors:
            result["scan_errors"] = scan_errors
            result["scan_error_count"] = len(scan_errors)
            message = '0 (with scan errors)'
        else:
            message = '0'
    else:
        success = False

    return success, result, message
