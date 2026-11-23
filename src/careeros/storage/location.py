"""Resolve where data lives. Data directories may never be inside a git working tree."""

from __future__ import annotations

from pathlib import Path

from careeros.core.config import StorageConfig
from careeros.core.errors import StorageLocationError


def default_data_root() -> Path:
    return Path.home() / ".local" / "share" / "careeros"


def find_git_root(path: Path) -> Path | None:
    """Return the enclosing git working tree root of ``path``, if any."""
    p = path.resolve()
    for candidate in (p, *p.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def resolve_data_dir(cfg: StorageConfig) -> Path:
    """Return the absolute data directory for ``cfg`` (not created).

    Raises ``StorageLocationError`` if it would be inside a git repository, which
    could lead to personal data being committed (amendment A2).
    """
    base = cfg.data_dir.expanduser() if cfg.data_dir else default_data_root() / cfg.dataset_kind
    base = base.resolve()
    git_root = find_git_root(base)
    if git_root is not None:
        raise StorageLocationError(
            f"Refusing data directory {base}: it is inside the git repository at {git_root}. "
            "Personal and working data must live outside version control."
        )
    return base
