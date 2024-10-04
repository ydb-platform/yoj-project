#!/usr/bin/env bash

# Script to run before starting YdbRepositoryIntegrationTest.
# Starts a local instance of YDB inside a Docker container.

set -e

# Same as in https://ydb.tech/docs/en/quickstart, but with:
# - TLS port=2136 and non-TLS port=2135
# - Explicit row limit = 10_000
# - Auto kill container and data after you press [Enter]
# - No volumes (= no persistence)
# - UNIQUE INDEX constraint support forcefully enabled: it's not enabled by default, at least not in yandex-docker-local-ydb@sha256:f39237e6bab018c2635af765faa893061fc359892e53f7b51d40ab503b90846f
#   @see container entrypoint: https://github.com/ydb-platform/ydb/blob/d7773b7d314987d7563ba246476b506f60ef6c68/.github/docker/files/initialize_local_ydb#L12
#   @see calls local_ydb script: https://github.com/ydb-platform/ydb/blob/d7773b7d314987d7563ba246476b506f60ef6c68/ydb/public/tools/local_ydb/__main__.py#L99
#   @see EnableUniqConstraint in https://github.com/ydb-platform/ydb/blob/d7773b7d314987d7563ba246476b506f60ef6c68/ydb/core/protos/feature_flags.proto, needs to be in lower_snake_case to be passed as argument to /local_ydb
#   TODO(nvamelichev): Remove all these shenanigans when yandex-docker-local-ydb:latest will enable UNIQUE INDEXes by default
docker run -d -it --rm --name ydb-local -h localhost \
  --platform linux/amd64 \
  -p 2135:2135 -p 2136:2136 -p 8765:8765 \
  -e GRPC_TLS_PORT=2136 -e GRPC_PORT=2135 -e MON_PORT=8765 \
  -e YDB_USE_IN_MEMORY_PDISKS=true \
  -e YDB_KQP_RESULT_ROWS_LIMIT=10000 \
  --entrypoint=/bin/sh \
  cr.yandex/yc/yandex-docker-local-ydb@sha256:f39237e6bab018c2635af765faa893061fc359892e53f7b51d40ab503b90846f \
  -c 'export YDB_LOCAL_SURVIVE_RESTART="true" ; export YDB_GRPC_ENABLE_TLS="true" ; export GRPC_TLS_PORT=${GRPC_TLS_PORT:-2135} ; export GRPC_PORT=${GRPC_PORT:-2136} ; export YDB_GRPC_TLS_DATA_PATH="/ydb_certs" ; /local_ydb deploy --ydb-working-dir /ydb_data --ydb-binary-path /ydbd --fixed-ports --enable-feature-flag enable_uniq_constraint ; tail -f /dev/null' \
  &

read -r -p "Press [Enter] key to exit..."
docker kill ydb-local || true
