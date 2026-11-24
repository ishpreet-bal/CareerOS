"""`careeros` command-line interface (M1: init, status, config)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from careeros import __version__
from careeros.context import open_context
from careeros.core.config import AppConfig, load_config
from careeros.core.errors import CareerOSError
from careeros.cost.report import build_report, format_report
from careeros.manager.manager import CareerManager
from careeros.orchestration.shutdown import graceful_shutdown
from careeros.simulation import SCENARIOS, run_scenario
from careeros.storage.location import resolve_data_dir
from careeros.synthetic import seed_synthetic

app = typer.Typer(no_args_is_help=True, add_completion=False, help="CareerOS (mock mode only).")
config_app = typer.Typer(no_args_is_help=True, help="Inspect configuration.")
app.add_typer(config_app, name="config")

ConfigDirOpt = Annotated[
    Path | None, typer.Option("--config-dir", help="Directory containing careeros YAML files.")
]


def _load(config_dir: Path | None) -> AppConfig:
    try:
        return load_config(config_dir)
    except CareerOSError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc


@app.command()
def version() -> None:
    """Print the CareerOS version."""
    typer.echo(__version__)


@app.command()
def init(
    config_dir: ConfigDirOpt = None,
    synthetic: Annotated[
        bool, typer.Option("--synthetic", help="Seed the fictional development dataset.")
    ] = False,
) -> None:
    """Create the data directory and database (applies migrations)."""
    cfg = _load(config_dir)
    try:
        with open_context(cfg) as ctx:
            typer.echo(
                f"Database: {ctx.db.path}  (dataset: {ctx.db.dataset_kind}, "
                f"schema v{ctx.db.schema_version()})"
            )
            if synthetic:
                s = seed_synthetic(ctx)
                typer.echo(
                    f"Seeded synthetic data: {s.facts} facts, {s.skills} skills, "
                    f"{s.companies} companies, {s.jobs} jobs, {s.applications} application."
                )
    except CareerOSError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc


@app.command()
def status(config_dir: ConfigDirOpt = None) -> None:
    """Show database state and outstanding (resumable) work."""
    cfg = _load(config_dir)
    try:
        with open_context(cfg) as ctx:
            profile = ctx.personal.get_profile()
            unknown_auth: list[str] = []
            if profile is not None and profile.id is not None:
                for country in cfg.eligibility.target_countries:
                    auth = ctx.personal.get_work_authorization(profile.id, country)
                    if auth.needs_user_confirmation:
                        unknown_auth.append(country)
            outstanding = ctx.orchestration.outstanding_tasks()
            typer.echo(f"Execution mode : {cfg.execution.mode} (real model calls disabled)")
            typer.echo(f"Data directory : {ctx.data_dir}")
            typer.echo(f"Dataset        : {ctx.db.dataset_kind}, schema v{ctx.db.schema_version()}")
            typer.echo(f"Profile        : {'present' if profile else 'none'}")
            if unknown_auth:
                typer.echo(
                    "Work auth      : needs your confirmation for " + ", ".join(unknown_auth)
                )
            typer.echo(f"Companies/jobs : {ctx.companies.count()} / {ctx.jobs.count()}")
            typer.echo(f"Applications   : {len(ctx.personal.list_applications())}")
            typer.echo(f"Objectives     : {len(ctx.orchestration.list_objectives())}")
            typer.echo(f"Outstanding    : {len(outstanding)} task(s)")
            for t in outstanding:
                typer.echo(f"  - task {t.id} [{t.status.value}] {t.task_type}: {t.title or ''}")
    except CareerOSError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc


@app.command()
def demo(
    scenario: Annotated[str, typer.Option(help=f"One of: {', '.join(SCENARIOS)}")] = "standard",
    config_dir: ConfigDirOpt = None,
) -> None:
    """Run an offline demonstration objective (synthetic data, mock models only)."""
    cfg = _load(config_dir)
    try:
        with open_context(cfg) as ctx:
            result = run_scenario(ctx, scenario)
            typer.echo(f"Scenario '{scenario}' (all model calls simulated)\n")
            typer.echo("\n".join(result.events))
            typer.echo("")
            typer.echo(format_report(build_report(ctx.db, result.objective_id)))
    except CareerOSError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc


@app.command()
def resume(
    objective_id: int,
    force_recover: Annotated[
        bool,
        typer.Option(
            "--force-recover",
            help="Treat all other sessions as dead (only if no other CareerOS process runs).",
        ),
    ] = False,
    config_dir: ConfigDirOpt = None,
) -> None:
    """Resume an interrupted or approved objective from its persisted state."""
    cfg = _load(config_dir)
    try:
        with open_context(cfg) as ctx:
            manager = CareerManager(ctx)
            with graceful_shutdown(manager):  # SIGINT/SIGTERM: stop safely; twice: exit
                summary = manager.run(objective_id, force_recover=force_recover)
            typer.echo("\n".join(summary.events))
            typer.echo(f"Objective {objective_id}: {summary.status.value} ({summary.stop_reason})")
    except CareerOSError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc


@app.command()
def report(objective_id: int, config_dir: ConfigDirOpt = None) -> None:
    """Show the simulated cost and quality report for an objective."""
    cfg = _load(config_dir)
    try:
        with open_context(cfg) as ctx:
            typer.echo(format_report(build_report(ctx.db, objective_id)))
    except CareerOSError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc


@app.command()
def approvals(config_dir: ConfigDirOpt = None) -> None:
    """List requests waiting for your decision."""
    cfg = _load(config_dir)
    with open_context(cfg) as ctx:
        pending = CareerManager(ctx).pending_requests()
        if not pending:
            typer.echo("No pending requests.")
        for r in pending:
            typer.echo(
                f"#{r['id']} objective {r['objective_id']} task {r['task_id']} "
                f"[{r['kind']}]: {r['details'].get('reason') or r['details']}"
            )


@app.command()
def approve(
    request_id: int,
    usd: Annotated[float, typer.Option(help="Extra SIMULATED budget to allow.")] = 0.0,
    accept_downgrade: Annotated[
        bool,
        typer.Option(
            "--accept-downgrade",
            help="Instead of more budget, accept the lower-quality option the request offers.",
        ),
    ] = False,
    config_dir: ConfigDirOpt = None,
) -> None:
    """Approve a pending request (budget amounts are simulated; nothing is charged)."""
    cfg = _load(config_dir)
    try:
        with open_context(cfg) as ctx:
            CareerManager(ctx).approve(request_id, extra_usd=usd, accept_downgrade=accept_downgrade)
            typer.echo(f"Request {request_id} approved. Run `careeros resume <objective>`.")
    except CareerOSError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc


@config_app.command("validate")
def config_validate(config_dir: ConfigDirOpt = None) -> None:
    """Validate configuration files and safety rules."""
    cfg = _load(config_dir)
    try:
        data_dir = resolve_data_dir(cfg.storage)
    except CareerOSError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    enabled = ", ".join(m.id for m in cfg.enabled_models()) or "none"
    typer.echo("Configuration OK")
    typer.echo(f"  mode={cfg.execution.mode} real_budget=${cfg.budgets.real_monthly_usd:.2f}")
    typer.echo(f"  enabled models: {enabled}")
    typer.echo(f"  data dir: {data_dir}")


@config_app.command("show")
def config_show(config_dir: ConfigDirOpt = None) -> None:
    """Print the effective configuration as JSON."""
    cfg = _load(config_dir)
    typer.echo(json.dumps(cfg.model_dump(mode="json"), indent=2))


if __name__ == "__main__":  # pragma: no cover
    app()
