#!/usr/bin/env bash

set -e

python3 /app/manage.py install

for tester in /testers/*; do
  python3 /app/manage.py tester install "$tester"
done

for plugin in /plugins/*; do
  python3 /app/manage.py plugin install "$plugin"
done

exec "$@"
