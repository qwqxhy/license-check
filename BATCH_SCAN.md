# Batch GitHub License Scan

This guide is for large-scale scanning (e.g., 400k repositories) with:

- batch clone
- parallel scan
- batch cleanup
- OSS upload (S3-compatible)

## 1) Install dependencies

```bash
cd /root/lanyun-tmp/license-check-master
source .venv/bin/activate
pip install -r requirements.batch.txt
```

## 2) Prepare config

Create runtime config from example:

```bash
cp config/batch_scan.example.toml config/batch_scan.toml
```

Edit:

- `run.repos_file`
- `run.batch_size`, `run.clone_workers`, `run.scan_workers`
- MongoDB section (`mongodb.*`)
- OSS section (`oss.*`)
- REMOTE section (`remote.*`)

For Aliyun OSS credentials, prefer env vars:

```bash
export OSS_ACCESS_KEY_ID='...'
export OSS_ACCESS_KEY_SECRET='...'
```

## 3) Prepare repository list

Put repo URLs in the file defined by `run.repos_file`.

Examples:

```text
https://github.com/fastapi/fastapi.git
https://github.com/pallets/flask.git,main
https://github.com/django/django.git main
```

Or auto-fetch 10k public repositories (resume-safe):

```bash
export GITHUB_TOKEN='...'
python fetch_github_repos.py --config config/batch_scan.toml --target 10000
```

Then set in config:

- `run.repos_file = "config/repos.top1w.txt"`

### Optional: Use S3/MinIO mirror instead of git clone

If your repos are mirrored to S3/MinIO as ZIP snapshots:

```
github/<owner>/<repo>-master.zip
github/<owner>/<repo>-main.zip
```

Enable mirror download in config:

```
[MIRROR]
enabled = true
endpoint = "public-s3.isrc.ac.cn"
bucket = "yuantu"
prefix = "github"
access_key_env = "MINIO_ACCESS_KEY"
secret_key_env = "MINIO_SECRET_KEY"
```

Then export credentials:

```
export MINIO_ACCESS_KEY="..."
export MINIO_SECRET_KEY="..."
```

When `MIRROR.enabled=true`, the scanner now falls back to `git clone` if the ZIP snapshot
does not exist in mirror storage. Disable this only if you want missing mirror objects to
count as hard failures:

```toml
[MIRROR]
fallback_to_git_on_miss = true
```

### Optional: Sync results to remote storage via SSH/rsync

Enable remote sync in config:

```
[REMOTE]
enabled = true
host = "10.211.152.12"
port = 22
user = "huayuan"
base_dir = "/home/huayuan/workspace/storage"
```

For fastest, passwordless sync, use an SSH key:

```
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519
ssh-copy-id -i ~/.ssh/id_ed25519.pub huayuan@10.211.152.12
```

Then set in config:

```
ssh_key = "/home/huayuan/.ssh/id_ed25519"
```

If you must use a password (requires `sshpass`):

```
export REMOTE_PASSWORD="your-password"
```

## 4) Dry run

```bash
python batch_scan_github.py --config config/batch_scan.toml --dry-run
```

## 5) Run batch scan

```bash
python batch_scan_github.py --config config/batch_scan.toml
```

## 6) Output structure

Under `run.work_dir/run_YYYYMMDD_HHMMSS`:

- `results/batch_xxxxxx/*.json` per-repo scan results
- `manifests/batch_xxxxxx.json` batch summary
- `run_summary.json` global summary

`clone_failed` repositories also produce a per-repo JSON record under `results/`, so remote
storage can contain one result file per input repository rather than only scanned repositories.

If `OSS.enabled=true`, each batch uploads:

- batch manifest
- all per-repo result JSON files

to:

`s3://<bucket>/<prefix>/<run_id>/<batch_name>/...`

## 7) Notes for large-scale run

- Start with small test:
  - `batch_size=20`
  - `clone_workers=4`
  - `scan_workers=4`
- Increase scan workers gradually based on CPU/IO.
- Keep `cleanup_clone_after_batch=true` for disk control.
- Keep OSS upload enabled to avoid local accumulation.
- Keep `[state].enabled=true` so successful repos are skipped after restart.
