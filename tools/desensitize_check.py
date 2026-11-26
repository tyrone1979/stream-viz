"""Pre-publication desensitisation check.

    python tools/desensitize_check.py

Scans repository text files and paths for the public terms in
``tools/sensitive_terms.txt`` plus, when present, the local-only terms in
``.workbuddy/sensitive_terms.local.txt`` (excluded from version control).
Returns a non-zero exit code on a hit, so this can be wired into CI before
any release or tag.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TERMS_FILE = Path(__file__).with_name("sensitive_terms.txt")
LOCAL_TERMS_FILE = ROOT / ".workbuddy" / "sensitive_terms.local.txt"
SKIP_DIRS = {
    ".git", ".workbuddy", "output", "data", "__pycache__",
    ".pytest_cache", "venv", ".venv",
}
SKIP_FILES = {TERMS_FILE.name, LOCAL_TERMS_FILE.name}
TEXT_SUFFIXES = {
    ".py", ".md", ".txt", ".json", ".yml", ".yaml", ".toml", ".cfg", ".ini",
    ".tex", ".bib", ".c", ".h", ".cpp", ".hpp", ".sh", ".bat", ".html",
    ".js", ".css", ".svg",
}
MAX_BYTES = 2 * 1024 * 1024


def load_terms() -> list[str]:
    paths = [TERMS_FILE]
    if LOCAL_TERMS_FILE.is_file():
        paths.append(LOCAL_TERMS_FILE)
    terms = []
    for path in paths:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                terms.append(line)
    return sorted(set(terms))


_NUMBER_CHARS = set("0123456789.eE+-")


def is_json_number_value(line: str, start: int, end: int) -> bool:
    """True when ``line[start:end]`` is a number literal in a JSON value slot.

    A purely numeric blocked term (an institute number, a hull number) is a
    substring of ordinary measurements as soon as the repository ships a
    results file: ``"min": 4711.0`` and ``"frame_dropped": 4711`` both contain
    ``4711`` without naming anything.  Treating those as hits makes the scan
    unusable exactly when it starts to matter, so a match is ignored only when
    it sits in a well-formed number in value position.  A quoted string
    (``"4711"``) or a term followed by Chinese text (``4711所``) is still
    reported, because those *are* identifiers.
    """
    token = line[start:end]
    if not token.isdigit():
        return False
    i, j = start, end
    while i > 0 and line[i - 1] in _NUMBER_CHARS:
        i -= 1
    while j < len(line) and line[j] in _NUMBER_CHARS:
        j += 1
    try:
        float(line[i:j])
    except ValueError:
        return False
    prefix = line[:i].rstrip()
    if not prefix or prefix[-1] not in ":[,":
        return False
    suffix = line[j:].lstrip()
    return not suffix or suffix[0] in ",}]"


def find_term(line: str, term: str) -> int | None:
    """First index of ``term`` in ``line`` that is not a JSON number literal."""
    start = 0
    lowered = line.lower()
    needle = term.lower()
    while True:
        at = lowered.find(needle, start)
        if at < 0:
            return None
        if not (needle.isdigit() and is_json_number_value(line, at, at + len(needle))):
            return at
        start = at + len(needle)


def iter_files():
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file():
            continue
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if path.name in SKIP_FILES:
            continue
        yield path


def main() -> int:
    terms = load_terms()
    findings: list[str] = []
    scanned = 0

    for path in iter_files():
        rel = path.relative_to(ROOT).as_posix()
        for term in terms:
            if term.lower() in rel.lower():
                findings.append(f"PATH  {rel}  ->  {term!r}")
        if path.suffix.lower() not in TEXT_SUFFIXES and path.name != "LICENSE":
            continue
        if path.stat().st_size > MAX_BYTES:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        scanned += 1
        for lineno, line in enumerate(text.splitlines(), 1):
            for term in terms:
                if find_term(line, term) is not None:
                    findings.append(f"TEXT  {rel}:{lineno}  ->  {term!r}")
                    break

    print(f"Scanned {scanned} text files; {len(terms)} blocked terms.")
    if findings:
        print("FAIL - sensitive terms found:")
        for item in findings:
            print("  " + item)
        return 1
    print("PASS - no blocked terms in repository content or paths.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
