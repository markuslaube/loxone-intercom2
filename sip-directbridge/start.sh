#!/bin/sh
set -e

echo "Starting sip-directbridge..."

exec python sip_directbridge.py "$@"
