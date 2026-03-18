#!/usr/bin/env python3
"""
Fetch public GitHub repositories and persist to repos file with resume support.

This script uses GET /repositories?since=<id> (cursor pagination), which avoids
Search API 1000-result limits and supports stable resume.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import tomllib


GITHUB_API = "https://api.github.com/repositories"


def read_toml(path: Path) -> Dict:
    with path.open("rb") as f:
        return tomllib.load(f)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch GitHub repo list for batch scanner.")
    parser.add_argument("--config", default="config/batch_scan.toml", help="Config path.")
    parser.add_argument("--target", type=int, default=None, help="Override target count.")
    return parser.parse_args()


def load_state(state_file: Path, start_since: int) -> Dict:
    if not state_file.exists():
        return {"since": start_since, "fetched_count": 0, "updated_at": None}
    with state_file.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state_file: Path, state: Dict) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with state_file.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def load_existing_urls(output_file: Path) -> Tuple[List[str], set[str]]:
    if not output_file.exists():
        return [], set()
    lines: List[str] = []
    seen: set[str] = set()
    with output_file.open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            url = stripped.split(",", 1)[0].split(maxsplit=1)[0]
            if url in seen:
                continue
            seen.add(url)
            lines.append(stripped)
    return lines, seen


def github_list_repos(since: int, per_page: int, token: str, timeout_sec: int) -> List[Dict]:
    query = urlencode({"since": since, "per_page": per_page})
    req = Request(
        f"{GITHUB_API}?{query}",
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "license-check-batch-fetcher",
            **({"Authorization": f"Bearer {token}"} if token else {}),
        },
    )
    with urlopen(req, timeout=timeout_sec) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
        return json.loads(body)


def main() -> int:
    args = parse_args()
    cfg_path = Path(args.config).resolve()
    if not cfg_path.exists():
        print(f"config not found: {cfg_path}")
        return 2

    cfg = read_toml(cfg_path)
    fetch_cfg = {}
    fetch_cfg.update(cfg.get("github_fetch", {}))
    fetch_cfg.update(cfg.get("GITHUB_FETCH", {}))

    output_file = Path(fetch_cfg.get("output_file", "config/repos.top1w.txt")).resolve()
    state_file = Path(fetch_cfg.get("state_file", "config/github_fetch_state.json")).resolve()
    target_count = int(args.target) if args.target is not None else int(fetch_cfg.get("target_count", 10000))
    per_page = int(fetch_cfg.get("per_page", 100))
    timeout_sec = int(fetch_cfg.get("request_timeout_sec", 30))
    sleep_sec = float(fetch_cfg.get("sleep_sec", 0.2))
    token_env = str(fetch_cfg.get("token_env", "GITHUB_TOKEN"))
    token = os.getenv(token_env, "") or str(fetch_cfg.get("token", ""))

    start_since = int(fetch_cfg.get("start_since", 0))
    state = load_state(state_file, start_since=start_since)
    since = int(state.get("since", start_since))

    existing_lines, seen_urls = load_existing_urls(output_file)
    print(f"config: {cfg_path}", flush=True)
    print(f"output_file: {output_file}", flush=True)
    print(f"state_file: {state_file}", flush=True)
    print(f"target_count: {target_count}, existing: {len(existing_lines)}, since: {since}", flush=True)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("a", encoding="utf-8") as out:
        while len(seen_urls) < target_count:
            try:
                repos = github_list_repos(
                    since=since,
                    per_page=per_page,
                    token=token,
                    timeout_sec=timeout_sec,
                )
            except HTTPError as e:
                body = e.read().decode("utf-8", errors="ignore")
                print(f"http_error: {e.code} {body[:300]}", flush=True)
                return 1
            except URLError as e:
                print(f"url_error: {e}", flush=True)
                return 1
            except Exception as e:  # noqa: BLE001
                print(f"unexpected_error: {type(e).__name__}: {e}", flush=True)
                return 1

            if not repos:
                print("no more repositories from API.", flush=True)
                break

            new_added = 0
            max_id = since
            for repo in repos:
                repo_id = int(repo.get("id", 0))
                if repo_id > max_id:
                    max_id = repo_id
                clone_url = repo.get("clone_url")
                if not clone_url:
                    full_name = repo.get("full_name")
                    if full_name:
                        clone_url = f"https://github.com/{full_name}.git"
                default_branch = repo.get("default_branch") or ""
                if not clone_url or clone_url in seen_urls:
                    continue
                line = clone_url if not default_branch else f"{clone_url},{default_branch}"
                out.write(line + "\n")
                seen_urls.add(clone_url)
                new_added += 1
                if len(seen_urls) >= target_count:
                    break

            since = max_id
            state = {
                "since": since,
                "fetched_count": len(seen_urls),
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            save_state(state_file, state)
            out.flush()

            print(
                f"progress: total={len(seen_urls)}/{target_count}, "
                f"added={new_added}, since={since}"
            , flush=True)
            time.sleep(sleep_sec)

    print(f"done: wrote {len(seen_urls)} unique repos to {output_file}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
