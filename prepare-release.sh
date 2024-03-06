#!/bin/sh

set -e
set pipefail

die() {
  echo "$@" >&2
  exit 1
}

[ -z "$YOJ_VERSION" ] && die "Please set YOJ_VERSION before calling prepare-release.sh"
[ -z "$MVN" ] && MVN='mvn'

echo "[**] Updating YOJ artifact version to ${YOJ_VERSION}..."
"$MVN" versions:set -DnewVersion="${YOJ_VERSION}" -DprocessAllModules -DgenerateBackupPoms=false

echo
echo "[**] Checking that YOJ ${YOJ_VERSION} builds successfully:"
"$MVN" clean verify

echo
echo "[**] Updating README.md:"
sed "/<artifactId>yoj-bom<\/artifactId>\$/ {
  N
  s|<version>.*</version>|<version>${YOJ_VERSION}</version>|
}" README.md > README.md.new
mv -- README.md.new README.md

echo
echo "[**] Committing changes to VCS:"
git add -A
git commit -m "Release YOJ ${YOJ_VERSION}"
git tag "v${YOJ_VERSION}"

echo
echo "[**] Pushing changes:"
#git push origin && git push origin --tags

echo
echo "[**] Done!"
echo "     See https://github.com/ydb-platform/yoj-project/actions"
