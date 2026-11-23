"""Maps provider names to adapter instances. Only offline adapters can be registered."""

from __future__ import annotations

from careeros.core.config import OFFLINE_PROVIDERS, ModelProfile
from careeros.llm.base import ModelAdapter
from careeros.llm.contracts import RealExecutionBlockedError


class AdapterRegistry:
    def __init__(self) -> None:
        self._adapters: dict[str, ModelAdapter] = {}

    def register(self, adapter: ModelAdapter) -> None:
        if not getattr(adapter, "offline", False) or adapter.provider not in OFFLINE_PROVIDERS:
            raise RealExecutionBlockedError(
                f"Refusing to register {type(adapter).__name__} for provider "
                f"{adapter.provider!r}: only offline simulators are allowed in M2."
            )
        self._adapters[adapter.provider] = adapter

    def for_profile(self, profile: ModelProfile) -> ModelAdapter:
        try:
            return self._adapters[profile.provider]
        except KeyError:
            raise RealExecutionBlockedError(
                f"No adapter is available for provider {profile.provider!r}."
            ) from None

    @property
    def providers(self) -> frozenset[str]:
        return frozenset(self._adapters)
