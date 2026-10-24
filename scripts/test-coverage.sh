#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
cabal build lib:rdkafka-hs rdkafka-hs-integration -f integration
python3 scripts/test-coverage-tools.py
redpanda-local-up
python3 scripts/collect-coverage.py
