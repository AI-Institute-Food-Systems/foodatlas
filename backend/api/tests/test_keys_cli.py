"""Tests for the `python -m scripts.keys` admin CLI."""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner
from scripts.keys import cli as cli_module
from scripts.keys.store import LedgerStore
from scripts.keys.verify import ProbeResult
from test_keys_store import FakeSecrets

ACTIVE_LEDGER = {
    "h" * 64: {
        "email": "alice@u.edu",
        "created": "2026-01-05",
        "notes": "DMD figures",
        "prefix": "aaaa1111",
        "org": "UC Davis",
        "status": "active",
        "revoked_at": "",
        "issued_by": "lukas",
    },
    "b" * 64: {
        "email": "bob@u.edu",
        "created": "2026-02-09",
        "notes": "",
        "prefix": "bbbb2222",
        "org": "",
        "status": "revoked",
        "revoked_at": "2026-03-01",
        "issued_by": "lukas",
    },
}


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _install(monkeypatch: pytest.MonkeyPatch, fake: FakeSecrets) -> FakeSecrets:
    monkeypatch.setattr(
        cli_module, "_store", lambda *_a, **_k: LedgerStore(client_factory=lambda: fake)
    )
    return fake


def _ledger(fake: FakeSecrets) -> dict[str, dict[str, str]]:
    return json.loads(fake.bodies[fake.current])


class TestIssue:
    def test_records_the_key_and_prints_it(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake = _install(monkeypatch, FakeSecrets(json.dumps({})))
        result = runner.invoke(
            cli_module.cli,
            ["issue", "--email", "carol@u.edu", "--notes", "thesis", "--no-wait"],
        )
        assert result.exit_code == 0, result.output
        ledger = _ledger(fake)
        assert len(ledger) == 1
        record = next(iter(ledger.values()))
        assert record["email"] == "carol@u.edu"
        assert record["status"] == "active"
        assert record["prefix"]
        # The printed key must be the one whose prefix we recorded.
        assert record["prefix"] in result.output

    def test_key_is_printed_after_the_record_is_confirmed(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Ordering is the whole point: a key printed before a failed write is
        # a key that was handed out and never worked.
        _install(monkeypatch, FakeSecrets(json.dumps({})))
        result = runner.invoke(
            cli_module.cli,
            ["issue", "--email", "carol@u.edu", "--notes", "", "--no-wait"],
        )
        assert result.output.index("Recorded") < result.output.index("email this")

    def test_existing_records_are_preserved(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake = _install(monkeypatch, FakeSecrets(json.dumps(ACTIVE_LEDGER)))
        runner.invoke(
            cli_module.cli,
            ["issue", "--email", "carol@u.edu", "--notes", "", "--no-wait"],
        )
        ledger = _ledger(fake)
        assert len(ledger) == 3
        assert ledger["h" * 64]["email"] == "alice@u.edu"

    def test_concurrent_write_aborts_without_printing_a_key(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake = _install(monkeypatch, FakeSecrets(json.dumps({})))
        fake.fail_promote = True
        result = runner.invoke(
            cli_module.cli,
            ["issue", "--email", "carol@u.edu", "--notes", "", "--no-wait"],
        )
        assert result.exit_code == 1
        assert "email this" not in result.output

    def test_read_back_mismatch_is_fatal(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class AmnesiacSecrets(FakeSecrets):
            """Accepts the write, then reads back as if it never happened."""

            def get_secret_value(self, SecretId: str) -> dict[str, str]:
                return {"SecretString": json.dumps({}), "VersionId": self.current}

        _install(monkeypatch, AmnesiacSecrets(json.dumps({})))
        result = runner.invoke(
            cli_module.cli,
            ["issue", "--email", "carol@u.edu", "--notes", "", "--no-wait"],
        )
        assert result.exit_code == 1
        assert "was NOT issued" in result.output
        assert "email this" not in result.output

    def test_unverified_key_is_flagged(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(monkeypatch, FakeSecrets(json.dumps({})))
        result = runner.invoke(
            cli_module.cli,
            ["issue", "--email", "carol@u.edu", "--notes", "", "--no-wait"],
        )
        assert "Not confirmed live" in result.output

    def test_verified_key_is_not_flagged(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(monkeypatch, FakeSecrets(json.dumps({})))
        monkeypatch.setattr(
            cli_module,
            "wait_until_live",
            lambda *_a, **_k: ProbeResult(True, 12, 12, 200, 55.0),
        )
        result = runner.invoke(
            cli_module.cli,
            [
                "issue",
                "--email",
                "carol@u.edu",
                "--notes",
                "",
                "--api-url",
                "http://api",
            ],
        )
        assert "Live: 12 consecutive 200s" in result.output
        assert "Not confirmed live" not in result.output


class TestList:
    def test_shows_holders_and_status(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(monkeypatch, FakeSecrets(json.dumps(ACTIVE_LEDGER)))
        result = runner.invoke(cli_module.cli, ["list"])
        assert "alice@u.edu" in result.output
        assert "aaaa1111" in result.output
        assert "revoked 2026-03-01" in result.output
        assert "2 record(s)" in result.output

    def test_active_only_hides_revoked(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(monkeypatch, FakeSecrets(json.dumps(ACTIVE_LEDGER)))
        result = runner.invoke(cli_module.cli, ["list", "--active-only"])
        assert "bob@u.edu" not in result.output

    def test_json_output_is_machine_readable(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(monkeypatch, FakeSecrets(json.dumps(ACTIVE_LEDGER)))
        result = runner.invoke(cli_module.cli, ["list", "--as-json"])
        rows = json.loads(result.output)
        assert {r["email"] for r in rows} == {"alice@u.edu", "bob@u.edu"}

    def test_empty_ledger(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(monkeypatch, FakeSecrets(json.dumps({})))
        assert "Ledger is empty" in runner.invoke(cli_module.cli, ["list"]).output

    def test_malformed_secret_is_reported_cleanly(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(monkeypatch, FakeSecrets("placeholder!"))
        result = runner.invoke(cli_module.cli, ["list"])
        assert result.exit_code == 1
        assert "not JSON" in result.output


class TestRevoke:
    def test_keeps_the_record_and_flips_status(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake = _install(monkeypatch, FakeSecrets(json.dumps(ACTIVE_LEDGER)))
        result = runner.invoke(cli_module.cli, ["revoke", "aaaa1111", "--yes"])
        assert result.exit_code == 0, result.output
        ledger = _ledger(fake)
        assert len(ledger) == 2
        assert ledger["h" * 64]["status"] == "revoked"
        assert ledger["h" * 64]["revoked_at"]

    def test_by_email(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _install(monkeypatch, FakeSecrets(json.dumps(ACTIVE_LEDGER)))
        runner.invoke(cli_module.cli, ["revoke", "alice@u.edu", "--yes"])
        assert _ledger(fake)["h" * 64]["status"] == "revoked"

    def test_prompts_before_writing(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake = _install(monkeypatch, FakeSecrets(json.dumps(ACTIVE_LEDGER)))
        result = runner.invoke(cli_module.cli, ["revoke", "aaaa1111"], input="n\n")
        assert result.exit_code == 1
        assert _ledger(fake)["h" * 64]["status"] == "active"

    def test_unknown_selector_is_reported(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(monkeypatch, FakeSecrets(json.dumps(ACTIVE_LEDGER)))
        result = runner.invoke(cli_module.cli, ["revoke", "nope", "--yes"])
        assert result.exit_code == 1
        assert "no ledger record" in result.output

    def test_double_revoke_is_reported(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install(monkeypatch, FakeSecrets(json.dumps(ACTIVE_LEDGER)))
        result = runner.invoke(cli_module.cli, ["revoke", "bbbb2222", "--yes"])
        assert result.exit_code == 1
        assert "already revoked" in result.output


class TestProbe:
    def test_reports_failure(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            cli_module,
            "wait_until_live",
            lambda *_a, **_k: ProbeResult(False, 3, 0, 401, 30.0),
        )
        result = runner.invoke(
            cli_module.cli, ["probe", "somekey", "--api-url", "http://api"]
        )
        assert result.exit_code == 1
        assert "not consistently live" in result.output
