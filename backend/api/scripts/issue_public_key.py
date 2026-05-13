"""Issue a new /v1/ public API key.

Usage::

    cd backend/api && uv run python scripts/issue_public_key.py

The script never touches AWS itself — it prints the plaintext key (email it
to the recipient) and the ``aws secretsmanager`` command you can paste into
the terminal to merge the new entry into ``foodatlas/public-api-keys``.
Plaintext keys are not written to disk.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import secrets

import click

DEFAULT_SECRET_NAME = "foodatlas/public-api-keys"


@click.command()
@click.option(
    "--email",
    prompt="Researcher email",
    help="Email of the person receiving the key (stored in the secret).",
)
@click.option(
    "--notes",
    prompt="Notes (optional, e.g. project name)",
    default="",
    help="Free-form notes attached to the key record.",
)
@click.option(
    "--secret-name",
    default=DEFAULT_SECRET_NAME,
    show_default=True,
    help="AWS Secrets Manager secret name to update.",
)
@click.option(
    "--region",
    default="us-west-1",
    show_default=True,
    help="AWS region holding the secret.",
)
def main(email: str, notes: str, secret_name: str, region: str) -> None:
    """Generate a key and emit the plaintext + AWS CLI merge command."""
    plaintext = secrets.token_urlsafe(32)
    key_hash = hashlib.sha256(plaintext.encode("utf-8")).hexdigest()
    created = dt.date.today().isoformat()

    record = {
        key_hash: {
            "email": email,
            "created": created,
            "notes": notes,
        }
    }

    click.echo("")
    click.secho("New API key (email this to the recipient):", fg="green", bold=True)
    click.echo(plaintext)
    click.echo("")
    click.secho("Add to Secrets Manager:", fg="cyan", bold=True)
    click.echo(_aws_merge_command(secret_name, region, record))
    click.echo("")
    click.secho(
        "The running API picks up new keys on its next refresh tick "
        "(default 5 minutes) — no redeploy needed.",
        fg="yellow",
    )


def _aws_merge_command(secret_name: str, region: str, new_entry: dict) -> str:
    """Build a one-liner that merges ``new_entry`` into the existing secret."""
    new_json = json.dumps(new_entry)
    # Reads current secret, merges in the new entry via jq, writes back.
    return (
        "EXISTING=$("
        f"aws secretsmanager get-secret-value --region {region} "
        f"--secret-id {secret_name} --query SecretString --output text "
        "2>/dev/null || echo '{}'"
        ") && \\\n"
        f"  echo \"$EXISTING\" | jq -c '. + {new_json}' | \\\n"
        f"  xargs -0 aws secretsmanager update-secret --region {region} "
        f"--secret-id {secret_name} --secret-string"
    )


if __name__ == "__main__":
    main()
