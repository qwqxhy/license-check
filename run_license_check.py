#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def prepare_env() -> None:
    # Safe defaults for constrained/containerized environments.
    os.environ.setdefault("LANG", "C.UTF-8")
    os.environ.setdefault("LC_ALL", "C.UTF-8")
    os.environ.setdefault("SCANCODE_CACHE", "/tmp/scancode-cache")
    os.environ.setdefault("SCANCODE_TEMP", "/tmp/scancode-tmp")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run license_check for a target codebase."
    )
    parser.add_argument(
        "target",
        nargs="?",
        default="/root/lanyun-tmp/fastapi",
        help="Absolute path to the codebase to scan.",
    )
    parser.add_argument(
        "-o",
        "--output-json",
        help="Optional output JSON file path.",
    )
    args = parser.parse_args()

    prepare_env()

    from license_api import license_check

    success, results, message = license_check(args.target)
    print("success:", success)
    print("message:", message)
    if results:
        print("license_total:", results.get("license_total:"))
        print("license_kind:", results.get("license_kind:"))
        print("conflicts:", len(results.get("license_conflict:", [])))
    else:
        print("license_total:", None)
        print("license_kind:", None)
        print("conflicts:", None)

    if args.output_json:
        output_path = Path(args.output_json).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "success": success,
                    "message": message,
                    "results": results,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        print("json_saved:", str(output_path))

    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
