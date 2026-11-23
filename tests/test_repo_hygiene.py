"""Amendment A2 guard rails: no personal data or secrets tracked in git."""

import fnmatch
import re
import subprocess

import pytest

from tests.conftest import REPO_ROOT

FORBIDDEN_PATTERNS = [
    "data/*",
    "personal/*",
    "snapshots/*",
    "*.db",
    "*.sqlite",
    "*.sqlite3",
    ".env",
    ".env.*",
    "config/careeros.yaml",
    "*.pem",
    "*.key",
]
ALLOWED_EMAIL_DOMAINS = {"example.com", "example.org", "example.net"}
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@([A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+)")


def _tracked_files() -> list[str]:
    try:
        out = subprocess.run(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("git not available")
    return [f for f in out.stdout.splitlines() if f]


def test_gitignore_covers_personal_data() -> None:
    ignore = (REPO_ROOT / ".gitignore").read_text()
    for pattern in ("data/", "personal/", "*.db", ".env", "config/careeros.yaml"):
        assert pattern in ignore


def test_no_forbidden_files_tracked() -> None:
    bad = [
        f
        for f in _tracked_files()
        if any(fnmatch.fnmatch(f, p) for p in FORBIDDEN_PATTERNS) and f != ".env.example"
    ]
    assert bad == []


def test_only_example_email_domains_in_repo() -> None:
    offenders = []
    for f in _tracked_files():
        path = REPO_ROOT / f
        if not path.is_file() or path.suffix in {".png", ".jpg", ".pdf"}:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for m in EMAIL_RE.finditer(text):
            domain = m.group(1).lower()
            if domain.endswith((".py", ".md", ".yaml", ".txt")):
                continue
            if domain not in ALLOWED_EMAIL_DOMAINS:
                offenders.append(f"{f}: {m.group(0)}")
    assert offenders == []
