# license-check-master Quick Start

This document is for environments where you often reconnect via SSH and need a
repeatable startup process.

## 1) One-time initialization

Run in project root:

```bash
cd /root/lanyun-tmp/license-check-master

python -m venv .venv
source .venv/bin/activate

python -m pip install -U pip
pip install -r requirements.txt
pip install ./scancode_toolkit-31.1.1rc0-py3-none-any.whl

# Compatibility for SPDX plugin used by this ScanCode version
pip install --upgrade spdx-tools==0.6.1
pip install --force-reinstall "setuptools<81"
```

## 2) MongoDB one-time data import

If MongoDB is already configured with user `sca/sca123` and DB `sca`, import:

```bash
cd /root/lanyun-tmp/license-check-master

mongoimport --port 27037 -u sca -p sca123 --authenticationDatabase sca \
  --db sca --collection license_info \
  --file database/data/license_info.json

mongoimport --port 27037 -u sca -p sca123 --authenticationDatabase sca \
  --db sca --collection license_term \
  --file database/data/license_term.json
```

## 3) Per-SSH-session startup

Run these every time after reconnect:

```bash
cd /root/lanyun-tmp/license-check-master
source .venv/bin/activate

# Avoid locale/cache issues in restricted/containerized environments
export LANG=C.UTF-8
export LC_ALL=C.UTF-8
export SCANCODE_CACHE=/tmp/scancode-cache
export SCANCODE_TEMP=/tmp/scancode-tmp
export LICENSE_SCAN_BACKEND=auto
```

`LICENSE_SCAN_BACKEND` supports `auto`, `fast`, and `scancode`.
`auto` runs the lightweight scanner first and falls back to ScanCode only when needed.

Start MongoDB if needed:

```bash
mongod --auth --dbpath /var/lib/mongo \
  --logpath /var/log/mongodb/mongod.log \
  --fork --bind_ip 127.0.0.1 --port 27037
```

Check MongoDB:

```bash
mongosh --port 27037 -u sca -p sca123 --authenticationDatabase sca \
  --eval 'db.runCommand({ping:1})'
```

## 4) Project DB config

Set local DB config in `src/localconfig.py`:

```python
local_mongodb_host = "127.0.0.1"
local_mongodb_port = 27037
local_mongodb_user = "sca"
local_mongodb_password = "sca123"
```

## 5) Smoke test

```bash
cd /root/lanyun-tmp/license-check-master
source .venv/bin/activate
export LANG=C.UTF-8
export LC_ALL=C.UTF-8
export SCANCODE_CACHE=/tmp/scancode-cache
export SCANCODE_TEMP=/tmp/scancode-tmp

python -c "from scancode import cli; print('scancode ok:', cli.__file__)"
```

Then run project API:

```bash
PYTHONPATH=src python - <<'PY'
from license_api import license_check
target = "/absolute/path/to/codebase"
success, results, message = license_check(target)
print("success:", success)
print("message:", message)
print("license_total:", results.get("license_total:") if results else None)
PY
```

## 6) Known issue in constrained containers

`src/license_api.py` currently calls ScanCode with `processes=2`.
In some restricted containers this can fail with multiprocessing semaphore
permissions (`SemLock` permission error).

If that happens, change `processes=2` to `processes=0` in `src/license_api.py`.
