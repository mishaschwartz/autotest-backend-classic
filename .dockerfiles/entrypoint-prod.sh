#!/usr/bin/env bash

python3 /app/manage.py install

exec "$@"
