#!/usr/bin/env bash
# Fresh-clone reproducibility check.
#
# Clones the published tag into a temporary directory, installs only the two
# declared dependencies into a throwaway virtual environment, runs the test
# suite and audits the archived measurements.  Nothing from the author's
# working tree is used, so a pass is evidence that the release is
# self-contained -- not that it works on this particular machine's
# site-packages.
#
# This is the check a prospective adopter runs before trusting the numbers, and
# the check a reviewer runs before trusting the paper.  It is deliberately
# blunt: if it fails, something in the release is missing.
#
#   tools/reproducibility_check.sh                     # from GitHub
#   tools/reproducibility_check.sh --repo . --ref HEAD # from a local clone
#   tools/reproducibility_check.sh --keep              # leave the sandbox behind
#
# Exit code 0 means every stage passed.  Network access is required unless
# --repo points at a local path.

set -uo pipefail

REPO="https://github.com/tyrone1979/stream-viz.git"
REF="v1.0.0"
KEEP=0
PYTHON="${PYTHON:-python3}"

while [ $# -gt 0 ]; do
  case "$1" in
    --repo) REPO="$2"; shift 2 ;;
    --ref)  REF="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    -h|--help) sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

WORK="$(mktemp -d)" || { echo "cannot create a temporary directory" >&2; exit 1; }
cleanup() {
  if [ "$KEEP" -eq 1 ]; then
    echo "sandbox kept at $WORK"
    return 0
  fi
  # Use shutil rather than rm: on Git Bash for Windows $WORK comes back as a
  # drive-prefixed path that the shell's rm refuses to touch.
  "$PYTHON" - "$WORK" <<'PY' 2>/dev/null || echo "note: could not remove $WORK"
import shutil, sys
shutil.rmtree(sys.argv[1], ignore_errors=True)
PY
}
trap cleanup EXIT

STAGES=0
FAILED=0

stage() {
  STAGES=$((STAGES + 1))
  printf '\n[%d] %s\n' "$STAGES" "$1"
}

fail() {
  FAILED=$((FAILED + 1))
  printf '    FAILED: %s\n' "$1"
}

LOCAL_REPO=""
if [ -d "$REPO" ]; then LOCAL_REPO="$REPO"; fi

stage "clone $REF"
if [ -n "$LOCAL_REPO" ]; then
  # Local checkout: clone the working tree and check the ref out separately,
  # because --branch does not accept "HEAD" on a local remote.
  if ! git clone --quiet "$REPO" "$WORK/src" 2>"$WORK/clone.err"; then
    cat "$WORK/clone.err" >&2
    exit 1
  fi
  git -C "$WORK/src" checkout --quiet "$REF" || {
    echo "cannot check out $REF" >&2; exit 1; }
else
  if ! git clone --quiet --depth 1 --branch "$REF" "$REPO" "$WORK/src" 2>"$WORK/clone.err"; then
    cat "$WORK/clone.err" >&2
    echo "cannot clone $REF from $REPO" >&2
    exit 1
  fi
fi
echo "    $(git -C "$WORK/src" rev-parse --short HEAD)  $(git -C "$WORK/src" log -1 --format=%s)"

stage "create virtual environment"
if ! "$PYTHON" -m venv "$WORK/venv"; then
  echo "cannot create a venv with $PYTHON" >&2
  exit 1
fi
# POSIX venvs put the activation script under bin/, Windows under Scripts/.
if [ -f "$WORK/venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  source "$WORK/venv/bin/activate"
elif [ -f "$WORK/venv/Scripts/activate" ]; then
  # shellcheck disable=SC1091
  source "$WORK/venv/Scripts/activate"
else
  echo "    no activation script; using $PYTHON directly"
fi
python -m pip install --quiet --upgrade pip
if ! python -m pip install --quiet -r "$WORK/src/requirements.txt"; then
  fail "dependency installation"
fi

stage "run the test suite (data-dependent tests are skipped)"
if ! (cd "$WORK/src" && python -m unittest discover -s tests 2>&1 | tail -5); then
  fail "unit tests"
fi

stage "audit the archived measurements"
if ! (cd "$WORK/src" && python tools/verify_measurements.py); then
  fail "measurement audit"
fi

stage "confirm the release contains no manuscript artefacts"
if [ -d "$LOCAL_REPO" ]; then
  echo "    skipped (a local checkout legitimately carries paper/)"
elif [ -d "$WORK/src/paper" ] || [ -f "$WORK/src/examples/make_figures.py" ]; then
  fail "the code-and-data mirror still contains manuscript files"
else
  echo "    clean"
fi

printf '\n──────────────────────────────────────────────\n'
if [ "$FAILED" -eq 0 ]; then
  echo "PASS: $STAGES stages, release $REF is self-contained"
  exit 0
fi
echo "FAIL: $FAILED of $STAGES stages failed"
exit 1
