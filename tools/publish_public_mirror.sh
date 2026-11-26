#!/usr/bin/env bash
# Publish the code-and-data mirror used as the paper's data availability address.
#
#   tools/publish_public_mirror.sh [remote-url]
#
# The mirror is the same history as this repository with the manuscript and its
# figure-generation script removed from every commit. History is rewritten in a
# throwaway clone, so nothing local changes; the mirror is then force-pushed,
# because a filtered history has different commit IDs by construction.
#
# Usage: run from the repository root after committing to master.
set -euo pipefail

REMOTE="${1:-https://github.com/tyrone1979/stream-viz.git}"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
ROOT="$(git rev-parse --show-toplevel)"
EXCLUDE="paper examples/make_figures.py"

if [ -n "$(git status --porcelain)" ]; then
    echo "working tree is dirty; commit first so the mirror matches a known state" >&2
    exit 1
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
echo "building filtered mirror of $BRANCH in $tmp ..."
git clone -q --no-hardlinks "$ROOT" "$tmp/repo"
cd "$tmp/repo"
git checkout -q "$BRANCH"

FILTER_BRANCH_SQUELCH_WARNING=1 git filter-branch -f \
    --index-filter "git rm -r --cached --ignore-unmatch --quiet $EXCLUDE" \
    --prune-empty "$BRANCH" >/dev/null

if git ls-files | grep -qE '^(paper/|examples/make_figures\.py$)'; then
    echo "manuscript files survived the filter; refusing to publish" >&2
    exit 1
fi
if ! git ls-files | grep -q '^measurements/'; then
    echo "archived measurements missing from the mirror" >&2
    exit 1
fi

echo "files: $(git ls-files | wc -l) | commits: $(git rev-list --count HEAD)"
git push -f "$REMOTE" "$BRANCH:main"
echo "published $(git rev-parse --short HEAD) to $REMOTE (main)"
