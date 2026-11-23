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
