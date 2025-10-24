#!/bin/sh

set -e
set pipefail

die() {
  echo "$@" >&2
  exit 1
}

SCRIPT_NAME="$(basename "$0")"
SCRIPT_DIR="$(dirname "$0")"

[ -z "$YOJ_VERSION" ] && die "Please set YOJ_VERSION before calling ${SCRIPT_NAME}!"
! (echo "$YOJ_VERSION" | grep -- '-SNAPSHOT' >/dev/null) && die "Please set YOJ_VERSION to a snapshot version (x.y.z-SNAPSHOT)!"

if [ -z "$MVN" ]; then
  MVN="$SCRIPT_DIR/mvnw"
fi

echo "[**] Using Maven executable: $MVN"

echo "[**] Updating YOJ artifact version to ${YOJ_VERSION}..."
"$MVN" versions:set -DnewVersion="${YOJ_VERSION}" -DprocessAllModules -DgenerateBackupPoms=false

echo
echo "[**] Checking that YOJ ${YOJ_VERSION} builds successfully:"
"$MVN" clean install

echo
echo "[**] Committing changes to VCS:"
git add -A
git commit -m "Prepare for development of YOJ ${YOJ_VERSION}"

echo
echo "[**] Pushing changes:"
git push origin

echo
echo "[**] Done!"
