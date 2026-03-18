#!/usr/bin/env python3
import os
import sys
from ctypes import CDLL
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def _icu_candidates() -> list[Path]:
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
    return candidates


def _has_icu_runtime() -> bool:
    try:
        CDLL("libicui18n.so.78")
        return True
    except OSError:
        return False


def bootstrap_icu_runtime() -> None:
    if _has_icu_runtime():
        return
    if os.getenv("ICU_BOOTSTRAP_DONE") == "1":
        return

    current = os.getenv("LD_LIBRARY_PATH", "")
    parts = [p for p in current.split(":") if p]

    for cand in _icu_candidates():
        if not cand.exists() or not any(cand.glob("libicui18n.so*")):
            continue
        if str(cand) not in parts:
            parts.insert(0, str(cand))
        env = os.environ.copy()
        env["LD_LIBRARY_PATH"] = ":".join(parts)
        env["ICU_BOOTSTRAP_DONE"] = "1"
        os.execvpe(sys.executable, [sys.executable, *sys.argv], env)


def prepare_env() -> None:
    os.environ.setdefault("LANG", "C.UTF-8")
    os.environ.setdefault("LC_ALL", "C.UTF-8")
    os.environ.setdefault("SCANCODE_CACHE", "/tmp/scancode-cache")
    os.environ.setdefault("SCANCODE_TEMP", "/tmp/scancode-tmp")


def main() -> int:
    bootstrap_icu_runtime()
    prepare_env()

    import uvicorn
    from http_service import DEFAULT_HOST, DEFAULT_PORT

    host = os.getenv("LICENSE_API_HOST", DEFAULT_HOST)
    port = int(os.getenv("LICENSE_API_PORT", str(DEFAULT_PORT)))
    uvicorn.run("http_service:app", host=host, port=port, reload=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
