#!/bin/bash
# Edge Research Platform
# Usage: ./dashboard/start.sh [port]
PORT=${1:-7477}
cd "$(dirname "$0")/.."
exec python3 dashboard/server.py "$PORT"
