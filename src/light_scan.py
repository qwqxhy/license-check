import configparser
import fnmatch
import json
import os
import re
import tomllib
import xml.etree.ElementTree as et
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


LEGAL_BASE_NAMES = {
    "license",
    "licence",
    "copying",
    "copyright",
    "notice",
    "notices",
    "patents",
    "unlicense",
}

README_BASE_NAMES = {
    "readme",
    "readme.md",
    "readme.rst",
    "readme.txt",
    "readme.adoc",
}

MANIFEST_NAMES = {
    "package.json",
    "composer.json",
    "cargo.toml",
    "pyproject.toml",
    "setup.cfg",
    "setup.py",
    "pom.xml",
    "pubspec.yaml",
}

SPDX_SCAN_EXTENSIONS = {
    "",
    ".c",
    ".cc",
    ".cpp",
    ".cxx",
    ".h",
    ".hh",
    ".hpp",
    ".hxx",
    ".m",
    ".mm",
    ".java",
    ".kt",
    ".kts",
    ".groovy",
    ".scala",
    ".go",
    ".rs",
    ".py",
    ".pyi",
    ".rb",
    ".php",
    ".js",
    ".jsx",
    ".ts",
    ".tsx",
    ".vue",
    ".sh",
    ".bash",
    ".zsh",
    ".fish",
    ".ps1",
    ".bat",
    ".cmd",
    ".swift",
    ".cs",
    ".fs",
    ".lua",
    ".r",
    ".jl",
    ".pl",
    ".pm",
    ".dart",
    ".sql",
    ".html",
    ".htm",
    ".xml",
    ".xsd",
    ".xsl",
    ".yml",
    ".yaml",
    ".toml",
    ".ini",
    ".cfg",
    ".conf",
    ".properties",
    ".gradle",
    ".cmake",
    ".mk",
}

SPDX_SCAN_BASENAMES = {
    "dockerfile",
    "makefile",
    "cmakelists.txt",
    "jenkinsfile",
    "gemfile",
    "rakefile",
    "vagrantfile",
}

TEXT_SIGNATURES: Sequence[Tuple[str, Tuple[str, ...]]] = (
    ("mit", ("permission is hereby granted, free of charge, to any person obtaining a copy",)),
    ("apache-2.0", ("apache license", "version 2.0")),
    ("bsd-3-clause", ("redistribution and use in source and binary forms", "neither the name of")),
    ("bsd-2-clause", ("redistribution and use in source and binary forms", "this list of conditions and the following disclaimer")),
    ("mpl-2.0", ("mozilla public license", "version 2.0")),
    ("epl-2.0", ("eclipse public license", "version 2.0")),
    ("isc", ("permission to use, copy, modify, and/or distribute this software for any purpose",)),
    ("unlicense", ("this is free and unencumbered software released into the public domain",)),
    ("agpl-3.0", ("gnu affero general public license", "version 3")),
    ("lgpl-3.0", ("gnu lesser general public license", "version 3")),
    ("lgpl-2.1", ("gnu lesser general public license", "version 2.1")),
    ("gpl-3.0", ("gnu general public license", "version 3")),
    ("gpl-2.0", ("gnu general public license", "version 2")),
)

URL_LICENSE_MAP = {
    "apache.org/licenses/license-2.0": "apache-2.0",
    "opensource.org/licenses/mit": "mit",
    "opensource.org/license/mit": "mit",
    "opensource.org/licenses/bsd-2-clause": "bsd-2-clause",
    "opensource.org/licenses/bsd-3-clause": "bsd-3-clause",
    "mozilla.org/en-us/mpl/2.0/": "mpl-2.0",
    "gnu.org/licenses/gpl-3.0": "gpl-3.0",
    "gnu.org/licenses/gpl-2.0": "gpl-2.0",
    "gnu.org/licenses/lgpl-3.0": "lgpl-3.0",
    "gnu.org/licenses/lgpl-2.1": "lgpl-2.1",
}

NAME_LICENSE_MAP = {
    "mit": "mit",
    "mit license": "mit",
    "apache-2.0": "apache-2.0",
    "apache 2.0": "apache-2.0",
    "apache license 2.0": "apache-2.0",
    "apache license, version 2.0": "apache-2.0",
    "bsd-2-clause": "bsd-2-clause",
    "bsd 2-clause": "bsd-2-clause",
    "bsd-3-clause": "bsd-3-clause",
    "bsd 3-clause": "bsd-3-clause",
    "isc": "isc",
    "isc license": "isc",
    "mpl-2.0": "mpl-2.0",
    "mozilla public license 2.0": "mpl-2.0",
    "epl-2.0": "epl-2.0",
    "eclipse public license 2.0": "epl-2.0",
    "unlicense": "unlicense",
    "agpl-3.0": "agpl-3.0",
    "gpl-3.0": "gpl-3.0",
    "gpl-2.0": "gpl-2.0",
    "lgpl-3.0": "lgpl-3.0",
    "lgpl-2.1": "lgpl-2.1",
}

SPDX_RE = re.compile(r"spdx-license-identifier\s*:\s*([^\r\n]+)", re.IGNORECASE)
LICENSE_TOKEN_RE = re.compile(r"[A-Za-z0-9.+-]+")

FAST_TEXT_LIMIT = 16 * 1024
LEGAL_TEXT_LIMIT = 128 * 1024


def run_fast_scan(path: str, ignore_patterns: Sequence[str]) -> Tuple[int, Dict[str, Any], Dict[str, Any]]:
    files: List[Dict[str, Any]] = []
    meta: Dict[str, Any] = {
        "backend": "fast",
        "license_hits": 0,
        "candidate_files": 0,
        "top_level_legal_files": 0,
        "unmatched_candidate_files": [],
        "fallback_reasons": [],
    }

    for root, dirnames, filenames in os.walk(path, topdown=True):
        dirnames[:] = sorted(
            d for d in dirnames if not _is_ignored(_relative_path(path, os.path.join(root, d), directory=True), ignore_patterns)
        )
        filenames = sorted(filenames)

        rel_dir = _relative_path(path, root, directory=False)
        if rel_dir:
            files.append(_directory_record(rel_dir))

        for filename in filenames:
            full_path = os.path.join(root, filename)
            rel_path = _relative_path(path, full_path, directory=False)
            if _is_ignored(rel_path, ignore_patterns):
                continue
            record = _file_record(rel_path)
            licenses = _detect_licenses(full_path, record)
            record["licenses"] = licenses
            files.append(record)

            if record["is_legal"] or record["is_manifest"] or record["is_readme"]:
                meta["candidate_files"] += 1
                if record["is_top_level"] and record["is_legal"]:
                    meta["top_level_legal_files"] += 1
                if not licenses:
                    meta["unmatched_candidate_files"].append(record["path"])
            if licenses:
                meta["license_hits"] += len(licenses)

    if meta["license_hits"] == 0:
        meta["fallback_reasons"].append("no_license_detected")
    if meta["top_level_legal_files"] and not _has_top_level_legal_license(files):
        meta["fallback_reasons"].append("unmatched_top_level_legal")

    meta["needs_fallback"] = bool(meta["fallback_reasons"])
    results = {
        "files": files,
        "headers": [{"errors": []}],
        "scan_backend": "fast",
        "scan_meta": meta,
    }
    return 0, results, meta


def append_scan_error(results: Dict[str, Any], message: str) -> None:
    headers = results.setdefault("headers", [{"errors": []}])
    if not headers:
        headers.append({"errors": []})
    headers[0].setdefault("errors", []).append(message)


def _relative_path(base_dir: str, full_path: str, directory: bool) -> str:
    rel_path = os.path.relpath(full_path, base_dir)
    if rel_path == ".":
        return ""
    rel_path = rel_path.replace(os.sep, "/")
    if directory:
        return f"{rel_path}/"
    return rel_path


def _is_ignored(rel_path: str, ignore_patterns: Sequence[str]) -> bool:
    if not rel_path:
        return False
    normalized = rel_path.replace(os.sep, "/")
    for pattern in ignore_patterns:
        if pattern.endswith("/") and normalized.startswith(pattern):
            return True
        if fnmatch.fnmatch(normalized, pattern):
            return True
    return False


def _directory_record(path: str) -> Dict[str, Any]:
    name = os.path.basename(path.rstrip("/"))
    return {
        "path": path.rstrip("/"),
        "type": "directory",
        "name": name,
        "base_name": name,
        "extension": "",
        "is_top_level": "/" not in path.rstrip("/"),
        "is_legal": False,
        "is_readme": False,
        "is_manifest": False,
        "licenses": [],
    }


def _file_record(path: str) -> Dict[str, Any]:
    name = os.path.basename(path)
    suffix = Path(name).suffix
    return {
        "path": path,
        "type": "file",
        "name": name,
        "base_name": os.path.splitext(name)[0],
        "extension": suffix.lower(),
        "is_top_level": "/" not in path,
        "is_legal": _is_legal_file(name),
        "is_readme": _is_readme_file(name),
        "is_manifest": _is_manifest_file(name),
    }


def _is_legal_file(name: str) -> bool:
    lower_name = name.lower()
    base_name = os.path.splitext(lower_name)[0]
    return base_name in LEGAL_BASE_NAMES or lower_name.endswith(".license")


def _is_readme_file(name: str) -> bool:
    lower_name = name.lower()
    return lower_name in README_BASE_NAMES or lower_name.startswith("readme.")


def _is_manifest_file(name: str) -> bool:
    lower_name = name.lower()
    return lower_name in MANIFEST_NAMES or lower_name.endswith(".gemspec")


def _should_scan_spdx(record: Dict[str, Any]) -> bool:
    return record["extension"] in SPDX_SCAN_EXTENSIONS or record["name"].lower() in SPDX_SCAN_BASENAMES


def _detect_licenses(full_path: str, record: Dict[str, Any]) -> List[Dict[str, Any]]:
    expressions: List[str] = []

    if record["is_manifest"]:
        expressions.extend(_extract_manifest_expressions(full_path, record["name"].lower()))

    if not (record["is_legal"] or record["is_readme"] or record["is_manifest"]) and not _should_scan_spdx(record):
        return _build_license_entries(expressions)

    text = _read_text_file(
        full_path,
        LEGAL_TEXT_LIMIT if (record["is_legal"] or record["is_readme"] or record["is_manifest"]) else FAST_TEXT_LIMIT,
    )
    if text:
        expressions.extend(match.group(1).strip() for match in SPDX_RE.finditer(text))
        if (record["is_legal"] or record["is_readme"]) and not expressions:
            expressions.extend(_match_license_text(text))

    return _build_license_entries(expressions)


def _read_text_file(full_path: str, limit: int) -> Optional[str]:
    try:
        with open(full_path, "rb") as fh:
            data = fh.read(limit)
    except OSError:
        return None
    if not data:
        return ""
    if b"\x00" in data:
        return None
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        try:
            return data.decode("latin-1")
        except UnicodeDecodeError:
            return None


def _extract_manifest_expressions(full_path: str, lower_name: str) -> List[str]:
    try:
        if lower_name == "package.json":
            return _package_json_expressions(full_path)
        if lower_name == "composer.json":
            return _composer_json_expressions(full_path)
        if lower_name == "cargo.toml":
            return _cargo_toml_expressions(full_path)
        if lower_name == "pyproject.toml":
            return _pyproject_expressions(full_path)
        if lower_name == "setup.cfg":
            return _setup_cfg_expressions(full_path)
        if lower_name == "setup.py":
            return _setup_py_expressions(full_path)
        if lower_name == "pom.xml":
            return _pom_xml_expressions(full_path)
        if lower_name.endswith(".gemspec"):
            return _gemspec_expressions(full_path)
    except Exception:
        return []
    return []


def _package_json_expressions(full_path: str) -> List[str]:
    with open(full_path, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    values: List[str] = []
    license_value = payload.get("license")
    if isinstance(license_value, str):
        values.append(license_value)
    elif isinstance(license_value, dict):
        value = license_value.get("type")
        if isinstance(value, str):
            values.append(value)
    for item in payload.get("licenses", []):
        if isinstance(item, str):
            values.append(item)
        elif isinstance(item, dict) and isinstance(item.get("type"), str):
            values.append(item["type"])
    return values


def _composer_json_expressions(full_path: str) -> List[str]:
    with open(full_path, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    license_value = payload.get("license")
    if isinstance(license_value, str):
        return [license_value]
    if isinstance(license_value, list):
        return [value for value in license_value if isinstance(value, str)]
    return []


def _cargo_toml_expressions(full_path: str) -> List[str]:
    with open(full_path, "rb") as fh:
        payload = tomllib.load(fh)
    package_cfg = payload.get("package") or {}
    expressions: List[str] = []
    if isinstance(package_cfg.get("license"), str):
        expressions.append(package_cfg["license"])
    return expressions


def _pyproject_expressions(full_path: str) -> List[str]:
    with open(full_path, "rb") as fh:
        payload = tomllib.load(fh)
    project_cfg = payload.get("project") or {}
    expressions: List[str] = []
    license_value = project_cfg.get("license")
    if isinstance(license_value, str):
        expressions.append(license_value)
    elif isinstance(license_value, dict):
        text_value = license_value.get("text")
        if isinstance(text_value, str):
            expressions.append(text_value)
    for classifier in project_cfg.get("classifiers", []):
        if isinstance(classifier, str) and classifier.startswith("License ::"):
            mapped = _map_license_name(classifier.split("::")[-1].strip())
            if mapped:
                expressions.append(mapped)
    return expressions


def _setup_cfg_expressions(full_path: str) -> List[str]:
    parser = configparser.ConfigParser()
    parser.read(full_path, encoding="utf-8")
    expressions: List[str] = []
    if parser.has_option("metadata", "license"):
        expressions.append(parser.get("metadata", "license"))
    return expressions


def _setup_py_expressions(full_path: str) -> List[str]:
    text = _read_text_file(full_path, LEGAL_TEXT_LIMIT)
    if not text:
        return []
    matches = re.findall(r"license\s*=\s*['\"]([^'\"]+)['\"]", text, re.IGNORECASE)
    return matches


def _pom_xml_expressions(full_path: str) -> List[str]:
    root = et.parse(full_path).getroot()
    expressions: List[str] = []
    for license_node in root.findall(".//{*}license"):
        name = license_node.findtext("{*}name") or ""
        url = license_node.findtext("{*}url") or ""
        mapped = _map_license_name(name) or _map_license_url(url)
        if mapped:
            expressions.append(mapped)
    return expressions


def _gemspec_expressions(full_path: str) -> List[str]:
    text = _read_text_file(full_path, LEGAL_TEXT_LIMIT)
    if not text:
        return []
    matches = re.findall(r"\.license\s*=\s*['\"]([^'\"]+)['\"]", text, re.IGNORECASE)
    matches.extend(re.findall(r"\.licenses\s*=\s*\[([^\]]+)\]", text, re.IGNORECASE))
    expressions: List[str] = []
    for match in matches:
        if "," in match:
            expressions.extend(re.findall(r"['\"]([^'\"]+)['\"]", match))
        else:
            expressions.append(match)
    return expressions


def _match_license_text(text: str) -> List[str]:
    lower_text = text.lower()
    matches: List[str] = []
    for key, required_phrases in TEXT_SIGNATURES:
        if all(phrase in lower_text for phrase in required_phrases):
            matches.append(key)
    return matches


def _build_license_entries(expressions: Sequence[str]) -> List[Dict[str, Any]]:
    seen = set()
    items: List[Dict[str, Any]] = []
    for expression in expressions:
        cleaned = expression.strip()
        if not cleaned:
            continue
        keys = _expression_keys(cleaned)
        if not keys:
            mapped = _map_license_name(cleaned) or _map_license_url(cleaned)
            if mapped:
                keys = [mapped]
        for key in keys:
            entry_key = (key, cleaned)
            if entry_key in seen:
                continue
            seen.add(entry_key)
            items.append(
                {
                    "key": key,
                    "matched_rule": {"license_expression": cleaned},
                }
            )
    return items


def _expression_keys(expression: str) -> List[str]:
    tokens = LICENSE_TOKEN_RE.findall(expression.replace("(", " ").replace(")", " ").replace("/", " "))
    keys: List[str] = []
    skip_next = False
    for token in tokens:
        upper = token.upper()
        if upper in {"AND", "OR"}:
            continue
        if upper == "WITH":
            skip_next = True
            continue
        if skip_next:
            skip_next = False
            continue
        normalized = _normalize_license_token(token)
        if normalized:
            keys.append(normalized)
    return list(dict.fromkeys(keys))


def _normalize_license_token(token: str) -> str:
    value = token.strip()
    if not value:
        return ""
    lower = value.lower()
    if lower.startswith("licenseref-"):
        return lower
    if lower.endswith("-or-later"):
        lower = lower[: -len("-or-later")] + "-plus"
    elif lower.endswith("-only"):
        lower = lower[: -len("-only")]
    elif lower.endswith("+"):
        lower = lower[:-1] + "-plus"
    if lower in {"documentref", "none"}:
        return ""
    return lower


def _map_license_name(name: str) -> str:
    return NAME_LICENSE_MAP.get(name.strip().lower(), "")


def _map_license_url(url: str) -> str:
    lower_url = url.strip().lower()
    for pattern, key in URL_LICENSE_MAP.items():
        if pattern in lower_url:
            return key
    return ""


def _has_top_level_legal_license(files: Sequence[Dict[str, Any]]) -> bool:
    for item in files:
        if item.get("type") != "file":
            continue
        if item.get("is_top_level") and item.get("is_legal") and item.get("licenses"):
            return True
    return False
