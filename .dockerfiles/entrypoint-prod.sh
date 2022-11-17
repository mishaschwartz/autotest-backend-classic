#!/usr/bin/env bash

set -e

python3 /app/manage.py install

exec "$@"
