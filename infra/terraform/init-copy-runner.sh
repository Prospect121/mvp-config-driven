#!/bin/bash
set -euo pipefail
echo "[init] Copy runner to /local_disk0" 1>&2
cp /dbfs/FileStore/runner_cli.py /local_disk0/runner_cli.py
chmod 644 /local_disk0/runner_cli.py