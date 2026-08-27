"""``python -m scripts.keys`` — issue, list and revoke public /v1/ API keys.

The ledger secret is the only record of who holds a key, so every write here
is compare-and-swap (see :mod:`scripts.keys.store`) and every issuance is
verified before the plaintext is shown. Ordering matters: the key is printed
*last*, once the record is known to be stored and live, because a key printed
before a failed write is a key that was handed out and never worked.
"""

from __future__ import annotations

import functools
import json
from typing import TYPE_CHECKING, Any

import click
from src.public_keys_admin import (
    LedgerError,
    build_record,
    format_ledger,
    merge_record,
    mint,
    revoke_record,
)

from scripts.keys.store import (
    DEFAULT_LEDGER_ID,
    DEFAULT_REGION,
    ConcurrentWriteError,
    LedgerStore,
    MalformedLedgerError,
)
from scripts.keys.verify import wait_until_live

if TYPE_CHECKING:
    from collections.abc import Callable

LEDGER_ERRORS = (LedgerError, ConcurrentWriteError, MalformedLedgerError)


def _store_options(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Shared --ledger-id/--region/--profile options, newest decorator last."""

    @click.option(
        "--ledger-id",
        default=DEFAULT_LEDGER_ID,
        show_default=True,
        help="Secrets Manager secret holding the key ledger.",
    )
    @click.option(
        "--region",
        default=DEFAULT_REGION,
        show_default=True,
        help="AWS region holding the secret.",
    )
    @click.option(
        "--profile", default=None, help="AWS profile (e.g. foodatlas-prod-admin)."
    )
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return fn(*args, **kwargs)

    return wrapper


def _store(ledger_id: str, region: str, profile: str | None) -> LedgerStore:
    return LedgerStore(ledger_id=ledger_id, region=region, profile=profile)


@click.group()
def cli() -> None:
    """Manage the public /v1/ API key ledger."""


@cli.command("list")
@_store_options
@click.option("--active-only", is_flag=True, help="Hide revoked records.")
@click.option("--as-json", is_flag=True, help="Emit JSON instead of a table.")
def list_keys(
    ledger_id: str,
    region: str,
    profile: str | None,
    active_only: bool,
    as_json: bool,
) -> None:
    """Show who holds a key, when it was issued, and whether it still works."""
    payload, _ = _read(_store(ledger_id, region, profile))
    rows = format_ledger(payload, include_revoked=not active_only)
    if as_json:
        click.echo(json.dumps([row.__dict__ for row in rows], indent=2))
        return
    if not rows:
        click.echo("Ledger is empty.")
        return
    click.echo(f"{'EMAIL':<34}{'PREFIX':<11}{'CREATED':<12}{'STATUS':<9}NOTES")
    for row in rows:
        prefix = row.prefix or "—"
        note = row.notes or row.org
        if row.status != "active" and row.revoked_at:
            note = f"revoked {row.revoked_at}. {note}".strip()
        click.echo(f"{row.email:<34}{prefix:<11}{row.created:<12}{row.status:<9}{note}")
    click.echo(f"\n{len(rows)} record(s).")


@cli.command()
@_store_options
@click.option(
    "--email",
    prompt="Researcher email",
    help="Who the key is for; stored in the ledger.",
)
@click.option(
    "--notes",
    prompt="Notes (optional, e.g. project name)",
    default="",
    help="Free-form notes attached to the record.",
)
@click.option("--org", default="", help="Affiliation, if given on the request.")
@click.option(
    "--issued-by", default="", envvar="USER", help="Who issued it (defaults to $USER)."
)
@click.option(
    "--api-url",
    default=None,
    envvar="FOODATLAS_API_URL",
    help="API base URL to verify against, e.g. the prod ALB DNS.",
)
@click.option(
    "--no-wait",
    is_flag=True,
    help="Skip the convergence check (the key may 401 intermittently).",
)
def issue(
    ledger_id: str,
    region: str,
    profile: str | None,
    email: str,
    notes: str,
    org: str,
    issued_by: str,
    api_url: str | None,
    no_wait: bool,
) -> None:
    """Mint a key, record it, confirm it is live, then print it."""
    store = _store(ledger_id, region, profile)
    payload, version_id = _read(store)
    minted = mint()
    record = build_record(
        email=email, notes=notes, org=org, issued_by=issued_by, prefix=minted.prefix
    )
    try:
        merged = merge_record(payload, minted.key_hash, record)
        _, warning = store.write(merged, expected_version_id=version_id)
    except LEDGER_ERRORS as exc:
        raise click.ClickException(str(exc)) from exc
    if warning:
        click.secho(f"note: {warning}", fg="yellow")

    # Read back rather than trusting the write: this is the whole point of the
    # command existing instead of a pasted one-liner.
    stored, _ = _read(store)
    if minted.key_hash not in stored:
        raise click.ClickException(
            "write reported success but the record is not in the ledger — "
            "the key was NOT issued; inspect the secret before retrying"
        )
    click.secho(f"Recorded {email} (prefix {minted.prefix}).", fg="green")

    live = _await_convergence(api_url, minted.plaintext, no_wait=no_wait)
    click.echo("")
    click.secho("API key — email this to the recipient:", fg="green", bold=True)
    click.echo(minted.plaintext)
    if not live:
        click.echo("")
        click.secho(
            "⚠ Not confirmed live. Sending it now risks intermittent 401s for the\n"
            f"  recipient. Re-check with: probe --api-url ... (prefix {minted.prefix})",
            fg="red",
        )


@cli.command()
@_store_options
@click.argument("selector")
@click.option("--yes", is_flag=True, help="Skip the confirmation prompt.")
def revoke(
    ledger_id: str, region: str, profile: str | None, selector: str, yes: bool
) -> None:
    """Revoke a key by prefix, email, or full hash. The record is kept."""
    store = _store(ledger_id, region, profile)
    payload, version_id = _read(store)
    try:
        revised, row = revoke_record(payload, selector)
    except LEDGER_ERRORS as exc:
        raise click.ClickException(str(exc)) from exc
    if not yes:
        click.confirm(
            f"Revoke {row.email}'s key (prefix {row.prefix or '—'}, "
            f"issued {row.created})?",
            abort=True,
        )
    try:
        _, warning = store.write(revised, expected_version_id=version_id)
    except LEDGER_ERRORS as exc:
        raise click.ClickException(str(exc)) from exc
    if warning:
        click.secho(f"note: {warning}", fg="yellow")
    click.secho(
        f"Revoked {row.email} (prefix {row.prefix or '—'}). The record stays in "
        "the ledger; the key stops working within one refresh interval.",
        fg="green",
    )


@cli.command()
@click.argument("plaintext")
@click.option(
    "--api-url",
    required=True,
    envvar="FOODATLAS_API_URL",
    help="API base URL to probe.",
)
def probe(plaintext: str, api_url: str) -> None:
    """Check whether a key is live on every task yet."""
    if not _await_convergence(api_url, plaintext, no_wait=False):
        raise click.ClickException("key is not consistently live yet")


def _await_convergence(api_url: str | None, plaintext: str, *, no_wait: bool) -> bool:
    if no_wait:
        click.secho("Skipping the convergence check (--no-wait).", fg="yellow")
        return False
    if not api_url:
        click.secho(
            "No --api-url given, so the key was not verified against a running "
            "API. Pass one (or set FOODATLAS_API_URL) to confirm it is live.",
            fg="yellow",
        )
        return False

    def _tick(attempt: int, run: int, status: int) -> None:
        click.echo(f"  probe {attempt}: HTTP {status or 'error'} (run of {run})")

    click.echo(f"Waiting for every task to pick the key up ({api_url})…")
    result = wait_until_live(api_url, plaintext, on_attempt=_tick)
    if result.live:
        click.secho(
            f"Live: {result.streak} consecutive 200s in {result.elapsed_s:.0f}s.",
            fg="green",
        )
    return result.live


def _read(store: LedgerStore) -> tuple[dict[str, dict[str, str]], str]:
    try:
        return store.read()
    except MalformedLedgerError as exc:
        raise click.ClickException(str(exc)) from exc
