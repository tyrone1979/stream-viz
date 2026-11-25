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
            lowered = line.lower()
            for term in terms:
                if term.lower() in lowered:
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
