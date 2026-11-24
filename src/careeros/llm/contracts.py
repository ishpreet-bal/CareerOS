"""Provider-neutral request/response types shared by all adapters."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from careeros.core.config import Effort
from careeros.core.errors import CareerOSError


@dataclass(frozen=True)
class OutputSchema:
    """Minimal structural contract for a model's JSON ``output`` object.

    ``kind`` lets an offline simulator produce a plausible shape (e.g. ``plan``); real
    adapters pass ``required``/``types`` to the provider as a JSON schema.
    """

    required: tuple[str, ...] = ()
    types: dict[str, str] = field(default_factory=dict)  # field -> "string"|"list"|"number"
    kind: str = "generic"

    def to_dict(self) -> dict[str, Any]:
        return {"required": list(self.required), "types": dict(self.types), "kind": self.kind}


@dataclass(frozen=True)
class ModelRequest:
    task_type: str
    prompt: str
    system: str | None = None
    output_schema: OutputSchema = OutputSchema()
    effort: Effort = "none"
    max_output_tokens: int = 2_000
    #: Varies per attempt so a retry is a fresh sample rather than a replay.
    seed_salt: str = ""
    #: Hints for offline simulators only (e.g. true task difficulty). Real adapters
    #: must ignore this field.
    simulation: dict[str, Any] = field(default_factory=dict)
    #: CareerOS's *local* idempotency key for this logical call. Adapters may forward it
    #: to a provider that supports request de-duplication (see
    #: ``ModelAdapter.supports_provider_idempotency``); CareerOS never relies on that.
    client_request_id: str | None = None


@dataclass(frozen=True)
class ModelResponse:
    text: str
    input_tokens: int
    output_tokens: int
    latency_ms: int
    finish_reason: str = "stop"
    #: Ground truth recorded by simulators (e.g. hidden quality); empty for real models.
    simulation: dict[str, Any] = field(default_factory=dict)
    #: Identifier the provider assigned to this request, when it reports one.
    provider_request_id: str | None = None


# ------------------------------------------------------------------------------ errors
class ModelError(CareerOSError):
    """Base for model-call failures. Carries usage so failed calls are still accounted."""

    kind = "error"

    def __init__(
        self,
        message: str,
        *,
        input_tokens: int = 0,
        output_tokens: int = 0,
        latency_ms: int = 0,
        response: ModelResponse | None = None,
    ) -> None:
        super().__init__(message)
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens
        self.latency_ms = latency_ms
        self.response = response


class ModelTimeoutError(ModelError):
    kind = "timeout"


class ModelRateLimitError(ModelError):
    kind = "rate_limit"


class ModelMalformedResponseError(ModelError):
    """The model returned text that is not the requested JSON."""

    kind = "malformed"


class IdempotencyMismatchError(CareerOSError):
    """A stored call with this idempotency key was made with a different model, effort or
    request: replaying it would attribute a reply to the wrong call (audit L1)."""


class RealExecutionBlockedError(CareerOSError):
    """Raised whenever anything attempts to reach a real (non-offline) provider."""


class BudgetExceededError(CareerOSError):
    def __init__(
        self,
        message: str,
        *,
        scope: str,
        needed_usd: float,
        available_usd: float,
        reserved_usd: float = 0.0,
        uncommitted_usd: float | None = None,
    ):
        super().__init__(message)
        self.scope = scope
        self.needed_usd = needed_usd
        self.available_usd = available_usd
        self.reserved_usd = reserved_usd
        self.uncommitted_usd = available_usd if uncommitted_usd is None else uncommitted_usd

    @property
    def is_contention(self) -> bool:
        """The call would fit if concurrent in-flight reservations were settled: wait and
        retry rather than ask the user."""
        return self.reserved_usd > 1e-12 and self.needed_usd <= self.uncommitted_usd + 1e-12
