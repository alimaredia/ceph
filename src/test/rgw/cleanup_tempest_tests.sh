#!/bin/bash
set -x

KEYSTONE_PUBLIC_PID=$(pgrep -f keystone-wsgi-public)
KEYSTONE_ADMIN_PID=$(pgrep -f keystone-wsgi-admin)
rm -rf tempest
../src/stop.sh
# Stoping Keyston Admin Instance
kill $KEYSTONE_ADMIN_PID
## Stoping Keyston Public Instance
kill $KEYSTONE_PUBLIC_PID

rm -rf archive
rm -rf keystone
rm -rf tox-venv
rm tempest_run.out
