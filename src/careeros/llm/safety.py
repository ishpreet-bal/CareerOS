"""The real-execution gate. Checked on every call, independently of config validation.

M2 has no code path that can reach a real provider: the only registered adapters are
offline simulators, and these checks refuse anything else even if configuration
validation were bypassed.
"""

from __future__ import annotations

from careeros.core.config import OFFLINE_PROVIDERS, AppConfig, ModelProfile
from careeros.llm.base import ModelAdapter
from careeros.llm.contracts import RealExecutionBlockedError

ALLOWED_MODES = frozenset({"mock", "scripted"})


def check_call_allowed(config: AppConfig, profile: ModelProfile, adapter: ModelAdapter) -> None:
    if config.execution.mode not in ALLOWED_MODES or config.execution.allow_real_models:
        raise RealExecutionBlockedError(
            f"Execution mode {config.execution.mode!r} is not permitted; only mock execution."
        )
    if profile.provider not in OFFLINE_PROVIDERS:
        raise RealExecutionBlockedError(
            f"Model {profile.id!r} uses provider {profile.provider!r}; real providers are "
            "disabled until explicitly approved."
        )
    if not getattr(adapter, "offline", False) or adapter.provider not in OFFLINE_PROVIDERS:
        raise RealExecutionBlockedError(
            f"Adapter {type(adapter).__name__} is not an offline simulator."
        )
    if not profile.enabled:
        raise RealExecutionBlockedError(f"Model {profile.id!r} is disabled.")
