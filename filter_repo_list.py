#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


EXCLUDE_FULL_NAMES = {
    "angular/angular",
    "ant-design/ant-design",
    "appwrite/appwrite",
    "bitcoin/bitcoin",
    "comfyanonymous/ComfyUI",
    "django/django",
    "facebook/react",
    "freeCodeCamp/freeCodeCamp",
    "gin-gonic/gin",
    "go-gitea/gitea",
    "gohugoio/hugo",
    "home-assistant/core",
    "immich-app/immich",
    "kamranahmedse/developer-roadmap",
    "mui/material-ui",
    "opencv/opencv",
    "openai/whisper",
    "oven-sh/bun",
    "puppeteer/puppeteer",
    "pytorch/pytorch",
    "storybookjs/storybook",
    "supabase/supabase",
    "tensorflow/tensorflow",
    "TheAlgorithms/Python",
    "torvalds/linux",
}

EXCLUDE_SUBSTRINGS = (
    "awesome-llm",
    "ComfyUI",
    "material-ui",
    "material-design",
    "ollama",
    "OpenWebUI",
    "stable-diffusion",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Filter oversized repositories from a JSONL repo list.")
    parser.add_argument("--input", required=True, help="Source JSONL path.")
    parser.add_argument("--output", required=True, help="Filtered repo txt output path.")
    parser.add_argument("--excluded-output", required=True, help="Excluded repo report output path.")
    parser.add_argument("--star-threshold", type=int, default=50000, help="Exclude repos with stars >= threshold.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)
    excluded_path = Path(args.excluded_output)

    kept = 0
    excluded = 0
    output_path.parent.mkdir(parents=True, exist_ok=True)
    excluded_path.parent.mkdir(parents=True, exist_ok=True)

    with (
        input_path.open("r", encoding="utf-8") as src,
        output_path.open("w", encoding="utf-8") as out,
        excluded_path.open("w", encoding="utf-8") as excluded_out,
    ):
        for line in src:
            line = line.strip()
            if not line:
                continue
            item = json.loads(line)
            full_name = str(item.get("full_name") or "").strip()
            if not full_name:
                continue
            stars = int(item.get("stargazers_count") or 0)
            html_url = str(item.get("html_url") or "").strip()
            clone_url = f"{html_url}.git" if html_url else f"https://github.com/{full_name}.git"

            reason = None
            if full_name in EXCLUDE_FULL_NAMES:
                reason = "denylist"
            elif any(token in full_name for token in EXCLUDE_SUBSTRINGS):
                reason = "substring_denylist"
            elif stars >= args.star_threshold:
                reason = f"stars>={args.star_threshold}"

            if reason:
                excluded += 1
                excluded_out.write(
                    json.dumps(
                        {
                            "full_name": full_name,
                            "clone_url": clone_url,
                            "stargazers_count": stars,
                            "reason": reason,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                continue

            out.write(clone_url + "\n")
            kept += 1

    print(
        json.dumps(
            {
                "input": str(input_path),
                "output": str(output_path),
                "excluded_output": str(excluded_path),
                "star_threshold": args.star_threshold,
                "kept": kept,
                "excluded": excluded,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
