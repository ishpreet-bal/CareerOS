"""Typed configuration loading and validation.

Configuration lives in YAML files in a config directory (default ``./config``):

* ``careeros.yaml`` (local, gitignored) or ``careeros.example.yaml`` (committed template)
* ``models.yaml``  — provider-neutral model capability profiles
* ``routing.yaml`` — task-type routing policy

Safety rules enforced here (M1): only mock execution is allowed, no real provider may be
enabled, and the real-money budget must be zero. These rules are relaxed only by a later,
explicitly approved milestone (M10).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from careeros.core.errors import ConfigError

ExecutionMode = Literal["mock", "scripted", "manual", "real"]
DatasetKind = Literal["synthetic", "personal"]
Priority = Literal["low", "normal", "high", "critical"]
Effort = Literal["none", "low", "medium", "high", "max"]
#: Reasoning-effort levels from least to most expensive.
EFFORT_ORDER: tuple[Effort, ...] = ("none", "low", "medium", "high", "max")

ENV_CONFIG_DIR = "CAREEROS_CONFIG_DIR"
ENV_DATA_DIR = "CAREEROS_DATA_DIR"
ENV_DATASET_KIND = "CAREEROS_DATASET_KIND"
ENV_EXECUTION_MODE = "CAREEROS_EXECUTION_MODE"

#: Providers that never contact a network service or cost money.
OFFLINE_PROVIDERS = frozenset({"mock", "scripted"})


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ExecutionConfig(_Strict):
    mode: ExecutionMode = "mock"
    allow_real_models: bool = False
    #: Upper bound on concurrently executing tasks (worker threads) per run.
    max_concurrency: int = Field(default=3, ge=1, le=16)

    @model_validator(mode="after")
    def _no_real_execution(self) -> ExecutionConfig:
        if self.mode == "real" or self.allow_real_models:
            raise ValueError(
                "Real model execution is not available: it requires explicit approval "
                "and the M10 safety gate. Use mode 'mock'."
            )
        if self.mode == "manual":
            raise ValueError(
                "The manual AI bridge is an optional future capability and is not implemented."
            )
        return self


class StorageConfig(_Strict):
    #: None means "use the default location outside the repository".
    data_dir: Path | None = None
    dataset_kind: DatasetKind = "synthetic"
    backend: Literal["sqlite"] = "sqlite"


class BudgetConfig(_Strict):
    #: Real-money budget. Must stay 0.0 until real execution is explicitly approved.
    real_monthly_usd: float = 0.0
    simulated_monthly_usd: float = Field(default=5.0, ge=0)
    default_objective_usd: float = Field(default=1.00, ge=0)
    default_task_usd: float = Field(default=0.25, ge=0)
    #: Cap on simulated spend by one execution session (one `run`/`resume` invocation).
    session_simulated_usd: float = Field(default=2.0, ge=0)
    #: Input-token estimates are multiplied by this when reserving, to cover tokenizer
    #: differences (estimates are ~4 chars/token; real tokenizers vary).
    input_token_margin: float = Field(default=1.10, ge=1.0, le=3.0)

    @field_validator("real_monthly_usd")
    @classmethod
    def _real_budget_zero(cls, v: float) -> float:
        if v != 0.0:
            raise ValueError("real_monthly_usd must be 0.0 until real spending is approved.")
        return v


class ResearchLimits(_Strict):
    max_sources: int = Field(default=5, ge=1)
    max_depth: int = Field(default=2, ge=1)
    max_queries: int = Field(default=8, ge=1)
    max_tokens_per_company: int = Field(default=20_000, ge=1)


class LimitsConfig(_Strict):
    max_attempts: int = Field(default=3, ge=1, le=10)
    max_escalations: int = Field(default=2, ge=0, le=5)
    max_retries_transient: int = Field(default=2, ge=0, le=5)
    max_tasks_per_objective: int = Field(default=100, ge=1)
    task_timeout_seconds: int = Field(default=300, ge=1)
    #: Lease duration for a claimed task (seconds). Small values are for tests only.
    lease_seconds: float = Field(default=120, ge=1)
    #: How often a live session renews its task leases (default: lease / 6, i.e. 20 s).
    heartbeat_seconds: float | None = Field(default=None, gt=0)
    #: A session halts its own workers once ``halt_after = lease - safety_margin`` seconds
    #: pass without a successful renewal (default margin: 2 heartbeat intervals = 40 s, so
    #: halt_after = 80 s), well before any lease it holds can expire.
    lease_safety_margin_seconds: float | None = Field(default=None, gt=0)
    #: Immediate retries of a failed lease renewal (with short backoff) before waiting for
    #: the next heartbeat interval.
    heartbeat_retries: int = Field(default=2, ge=0, le=10)
    #: After a shutdown request, how long workers still inside a provider call may take
    #: to reach a safe point before their tasks are released (calls abandoned, replies
    #: recorded as late). The heartbeat keeps their leases alive meanwhile.
    shutdown_grace_seconds: float = Field(default=30.0, ge=0)
    research: ResearchLimits = ResearchLimits()

    def heartbeat_interval(self) -> float:
        return self.heartbeat_seconds or self.lease_seconds / 6

    def safety_margin(self) -> float:
        return self.lease_safety_margin_seconds or 2 * self.heartbeat_interval()

    def halt_after(self) -> float:
        return self.lease_seconds - self.safety_margin()

    @model_validator(mode="after")
    def _lease_timing(self) -> LimitsConfig:
        if self.halt_after() <= self.heartbeat_interval():
            raise ValueError(
                "lease timing: halt_after (lease - safety margin) must exceed the heartbeat "
                f"interval; got lease={self.lease_seconds}, interval="
                f"{self.heartbeat_interval()}, margin={self.safety_margin()}"
            )
        return self


class EligibilityRule(_Strict):
    id: str
    description: str
    requirement: Literal[
        "work_authorization", "graduation_window", "degree_level", "location", "term"
    ]
    enabled: bool = True
    #: Amendment A3: an unknown candidate input never resolves to a guess.
    on_unknown: Literal["needs_user_confirmation"] = "needs_user_confirmation"


class EligibilityConfig(_Strict):
    #: ISO 3166-1 alpha-2 codes for the job search, not statements about the candidate.
    target_countries: tuple[str, ...] = ("CA", "US")
    include_remote_international: bool = True
    rules: tuple[EligibilityRule, ...] = ()

    @field_validator("target_countries")
    @classmethod
    def _iso_codes(cls, v: tuple[str, ...]) -> tuple[str, ...]:
        for code in v:
            if len(code) != 2 or not code.isalpha() or not code.isupper():
                raise ValueError(f"Country code {code!r} must be ISO 3166-1 alpha-2, e.g. 'CA'.")
        return v


class Pricing(_Strict):
    input_per_mtok: float | None = None
    output_per_mtok: float | None = None
    #: True for placeholder prices used only to exercise cost logic.
    simulated: bool = True
    verified_at: str | None = None


class SimulationProfile(_Strict):
    """Behaviour of an offline mock model. Only valid for offline providers."""

    #: Complexity (0..1) this model handles reliably at effort 'none'.
    skill: float = Field(ge=0, le=1)
    base_latency_ms: int = Field(default=300, ge=0)
    ms_per_output_token: float = Field(default=5.0, ge=0)
    malformed_rate: float = Field(default=0.0, ge=0, le=1)
    invalid_rate: float = Field(default=0.0, ge=0, le=1)
    timeout_rate: float = Field(default=0.0, ge=0, le=1)
    rate_limit_rate: float = Field(default=0.0, ge=0, le=1)
    timeout_ms: int = Field(default=60_000, ge=1)


class ModelProfile(_Strict):
    """A provider-neutral capability profile.

    ``id`` is CareerOS's own name for the profile. ``provider_model`` is the identifier the
    provider's API expects; it is opaque to the router and manager and is only read by the
    provider adapter.
    """

    id: str
    provider: str
    provider_model: str | None = None
    tier: int = Field(ge=1, le=3)
    capabilities: tuple[str, ...] = ()
    context_window: int = Field(default=8_000, ge=1)
    effort_levels: tuple[Effort, ...] = ("none",)
    pricing: Pricing = Pricing()
    simulation: SimulationProfile | None = None
    enabled: bool = False

    @property
    def is_offline(self) -> bool:
        return self.provider in OFFLINE_PROVIDERS

    def max_effort(self) -> Effort:
        return max(self.effort_levels, key=EFFORT_ORDER.index)

    @model_validator(mode="after")
    def _offline_only(self) -> ModelProfile:
        if self.simulation is not None and not self.is_offline:
            raise ValueError(f"Model {self.id!r}: simulation settings are only valid for mocks.")
        if self.enabled and self.provider not in OFFLINE_PROVIDERS:
            raise ValueError(
                f"Model {self.id!r}: provider {self.provider!r} cannot be enabled. Only "
                f"offline providers {sorted(OFFLINE_PROVIDERS)} are allowed before M10 approval."
            )
        if self.enabled and (
            self.pricing.input_per_mtok is None or self.pricing.output_per_mtok is None
        ):
            raise ValueError(f"Model {self.id!r}: an enabled model needs pricing.")
        return self


class TaskTypePolicy(_Strict):
    base_complexity: float = Field(default=0.3, ge=0, le=1)
    #: Lowest-capability tier allowed (tier 3 is the cheapest, tier 1 the strongest):
    #: a task may use any model whose tier number is <= min_tier.
    min_tier: int = Field(default=3, ge=1, le=3)
    expected_input_tokens: int = Field(default=800, ge=1)
    expected_output_tokens: int = Field(default=400, ge=1)
    #: Hard minimum acceptance probability for this task type regardless of priority or
    #: downgrade (e.g. factual deliverables such as resumes).
    min_quality: float = Field(default=0.0, ge=0, le=1)
    #: Capabilities a model profile must declare to be considered.
    required_capabilities: tuple[str, ...] = ()
    default_effort: Effort = "none"
    important: bool = False
    deterministic_first: bool = True


_DEFAULT_FLOORS: dict[Priority, float] = {
    "low": 0.6,
    "normal": 0.75,
    "high": 0.85,
    "critical": 0.92,
}


def _default_tier_skill() -> dict[int, float]:
    return {1: 0.9, 2: 0.7, 3: 0.45}


def _default_effort_bonus() -> dict[Effort, float]:
    return {"none": 0.0, "low": 0.03, "medium": 0.06, "high": 0.09, "max": 0.11}


def _default_degrade_limits() -> dict[Priority, float]:
    return {"normal": 0.10, "low": 0.20}


def _default_effort_multiplier() -> dict[Effort, float]:
    return {"none": 1.0, "low": 1.5, "medium": 2.0, "high": 3.0, "max": 4.0}


class RoutingConfig(_Strict):
    quality_floors: dict[Priority, float] = Field(default_factory=lambda: dict(_DEFAULT_FLOORS))
    #: Not used in M2: routing is deterministic so results are reproducible.
    exploration_rate: float = Field(default=0.05, ge=0, le=0.5)
    prior_strength: float = Field(default=4.0, gt=0)
    #: Prior belief about the complexity each tier handles reliably (before any history).
    tier_skill: dict[int, float] = Field(default_factory=_default_tier_skill)
    #: How much each effort level adds to a model's effective skill (prior only).
    effort_bonus: dict[Effort, float] = Field(default_factory=_default_effort_bonus)
    #: Output-token multiplier for reasoning effort (thinking tokens are billed as output).
    effort_output_multiplier: dict[Effort, float] = Field(
        default_factory=_default_effort_multiplier
    )
    #: Priorities whose quality floor is never lowered automatically; the user is asked.
    #: Must include critical and high.
    no_degrade_priorities: tuple[Priority, ...] = ("critical", "high")
    #: How far below its floor a task of this priority may be downgraded automatically.
    #: Priorities not listed here cannot be downgraded automatically.
    degrade_limits: dict[Priority, float] = Field(default_factory=_default_degrade_limits)
    #: No automatic or approved choice may fall below this acceptance probability.
    absolute_min_quality: float = Field(default=0.5, ge=0, le=1)
    #: Effective confidence below this is treated as a low-confidence result.
    confidence_threshold: float = Field(default=0.7, ge=0, le=1)
    #: Hard output cap per call = expected output tokens x effort multiplier x this factor.
    #: The cap is sent to the provider and the worst case at the cap is what gets reserved.
    output_cap_factor: float = Field(default=1.5, ge=1.0, le=10.0)
    #: Minimum reviewer score to accept an important deliverable.
    review_accept_score: float = Field(default=0.75, ge=0, le=1)
    task_types: dict[str, TaskTypePolicy] = Field(default_factory=dict)

    def policy(self, task_type: str) -> TaskTypePolicy:
        return self.task_types.get(task_type, TaskTypePolicy())

    @model_validator(mode="after")
    def _priority_policy(self) -> RoutingConfig:
        missing = {"critical", "high"} - set(self.no_degrade_priorities)
        if missing:
            raise ValueError(
                f"no_degrade_priorities must include {sorted(missing)}: critical and high "
                "priority tasks are never downgraded automatically."
            )
        bad = set(self.degrade_limits) & set(self.no_degrade_priorities)
        if bad:
            raise ValueError(
                f"degrade_limits cannot cover never-downgrade priorities {sorted(bad)}"
            )
        for k, v in self.degrade_limits.items():
            if not 0 <= v <= 0.5:
                raise ValueError(f"degrade limit for {k!r} must be in [0, 0.5]")
        return self

    @field_validator("quality_floors")
    @classmethod
    def _floors_in_range(cls, v: dict[Priority, float]) -> dict[Priority, float]:
        for k, f in v.items():
            if not 0 <= f <= 1:
                raise ValueError(f"quality floor for {k!r} must be in [0, 1]")
        return v


class AppConfig(_Strict):
    execution: ExecutionConfig = ExecutionConfig()
    storage: StorageConfig = StorageConfig()
    budgets: BudgetConfig = BudgetConfig()
    limits: LimitsConfig = LimitsConfig()
    eligibility: EligibilityConfig = EligibilityConfig()
    models: tuple[ModelProfile, ...] = ()
    routing: RoutingConfig = RoutingConfig()
    config_dir: Path | None = None

    @model_validator(mode="after")
    def _unique_model_ids(self) -> AppConfig:
        ids = [m.id for m in self.models]
        dupes = {i for i in ids if ids.count(i) > 1}
        if dupes:
            raise ValueError(f"Duplicate model ids: {sorted(dupes)}")
        return self

    def enabled_models(self) -> tuple[ModelProfile, ...]:
        return tuple(m for m in self.models if m.enabled)


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {path}: {exc}") from exc
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path} must contain a YAML mapping at the top level.")
    return data


def default_config_dir() -> Path:
    env = os.environ.get(ENV_CONFIG_DIR)
    return Path(env) if env else Path.cwd() / "config"


def load_config(config_dir: Path | None = None) -> AppConfig:
    """Load and validate configuration from ``config_dir``.

    Environment overrides: ``CAREEROS_DATA_DIR``, ``CAREEROS_DATASET_KIND``,
    ``CAREEROS_EXECUTION_MODE``.
    """
    cdir = config_dir or default_config_dir()
    raw: dict[str, Any] = {}
    if cdir.is_dir():
        main = cdir / "careeros.yaml"
        if not main.exists():
            main = cdir / "careeros.example.yaml"
        if main.exists():
            raw.update(_read_yaml(main))
        models_file = cdir / "models.yaml"
        if models_file.exists():
            raw["models"] = _read_yaml(models_file).get("models", [])
        routing_file = cdir / "routing.yaml"
        if routing_file.exists():
            raw["routing"] = _read_yaml(routing_file)
    elif config_dir is not None:
        raise ConfigError(f"Config directory {cdir} does not exist.")

    storage = dict(raw.get("storage") or {})
    if env_dir := os.environ.get(ENV_DATA_DIR):
        storage["data_dir"] = env_dir
    if env_kind := os.environ.get(ENV_DATASET_KIND):
        storage["dataset_kind"] = env_kind
    raw["storage"] = storage
    if env_mode := os.environ.get(ENV_EXECUTION_MODE):
        raw["execution"] = {**(raw.get("execution") or {}), "mode": env_mode}

    raw["config_dir"] = cdir if cdir.is_dir() else None
    try:
        return AppConfig.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"Invalid configuration:\n{exc}") from exc
