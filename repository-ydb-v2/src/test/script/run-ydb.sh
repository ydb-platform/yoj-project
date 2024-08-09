#!/usr/bin/env bash

# Script to run before starting YdbRepositoryIntegrationTest.
# Starts a local instance of YDB inside a Docker container.

set -e

# Same as in https://ydb.tech/docs/en/quickstart, but with:
# - TLS port=2136 and non-TLS port=2135
# - Explicit row limit = 10_000
# - Auto kill container and data after you press [Enter]
# - No volumes (= no persistence)
docker run -d --rm --name ydb-local -h localhost \
  --platform linux/amd64 \
  -p 2135:2135 -p 2136:2136 -p 8765:8765 \
  -e GRPC_TLS_PORT=2136 -e GRPC_PORT=2135 -e MON_PORT=8765 \
  -e YDB_USE_IN_MEMORY_PDISKS=true \
  -e YDB_KQP_RESULT_ROWS_LIMIT=10000 \
  cr.yandex/yc/yandex-docker-local-ydb:latest &
read -r -p "Press [Enter] key to exit..."
docker kill ydb-local || true
