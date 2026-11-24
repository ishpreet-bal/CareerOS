"""The adapter interface every model provider implements.

To connect a real provider later (M10, only with explicit approval), write one class that
satisfies :class:`ModelAdapter`, translate ``ModelRequest.effort`` into that provider's
reasoning controls in ``map_effort``, and add profiles for its models to ``models.yaml``.
The router, gateway, manager and workers do not change.
"""

from __future__ import annotations

from typing import Any, ClassVar, Protocol, runtime_checkable

from careeros.core.config import Effort, ModelProfile
from careeros.llm.contracts import ModelRequest, ModelResponse


@runtime_checkable
class ModelAdapter(Protocol):
    #: Provider name matched against ``ModelProfile.provider``.
    provider: ClassVar[str]
    #: True only for adapters that never touch the network and never cost money.
    offline: ClassVar[bool]
    # Optional (read with getattr, default False): whether the provider de-duplicates
    # requests by ``ModelRequest.client_request_id``. Provider-side idempotency is an
    # extra safety net only; CareerOS's correctness never depends on it.
    #     supports_provider_idempotency: ClassVar[bool]

    def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
        """Execute one request. Raises ``ModelError`` subclasses on failure."""
        ...

    def map_effort(self, effort: Effort) -> dict[str, Any]:
        """Provider-specific parameters for an abstract effort level."""
        ...
