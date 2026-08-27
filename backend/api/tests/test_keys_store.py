"""Tests for the compare-and-swap ledger store and the convergence probe."""

from __future__ import annotations

import json
from typing import Any

import pytest
from botocore.exceptions import ClientError
from scripts.keys.store import (
    ConcurrentWriteError,
    LedgerStore,
    MalformedLedgerError,
)
from scripts.keys.verify import wait_until_live


def _client_error(op: str) -> ClientError:
    return ClientError({"Error": {"Code": "InvalidRequest", "Message": "no"}}, op)


class FakeSecrets:
    """Minimal Secrets Manager stand-in with staging-label semantics."""

    def __init__(self, payload: str = "{}", version: str = "v0") -> None:
        self.current = version
        self.bodies = {version: payload}
        self.stages: dict[str, list[str]] = {version: ["AWSCURRENT"]}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.fail_promote = False
        self.fail_cleanup = False
        self._next = 0

    def get_secret_value(self, SecretId: str) -> dict[str, str]:
        self.calls.append(("get", {"SecretId": SecretId}))
        return {"SecretString": self.bodies[self.current], "VersionId": self.current}

    def put_secret_value(
        self, SecretId: str, SecretString: str, VersionStages: list[str]
    ) -> dict[str, str]:
        self._next += 1
        version = f"v{self._next}"
        self.bodies[version] = SecretString
        self.stages[version] = list(VersionStages)
        self.calls.append(("put", {"SecretId": SecretId, "VersionId": version}))
        return {"VersionId": version}

    def update_secret_version_stage(
        self,
        SecretId: str,
        VersionStage: str,
        MoveToVersionId: str | None = None,
        RemoveFromVersionId: str | None = None,
    ) -> dict[str, str]:
        self.calls.append(("stage", {"stage": VersionStage, "to": MoveToVersionId}))
        if VersionStage == "AWSCURRENT":
            if self.fail_promote or RemoveFromVersionId != self.current:
                raise _client_error("UpdateSecretVersionStage")
            self.current = str(MoveToVersionId)
        elif self.fail_cleanup:
            raise _client_error("UpdateSecretVersionStage")
        else:
            self.stages[str(RemoveFromVersionId)].remove(VersionStage)
        return {"VersionId": self.current}


def _store(fake: FakeSecrets) -> LedgerStore:
    return LedgerStore(client_factory=lambda: fake)


class TestRead:
    def test_parses_payload_and_returns_version(self) -> None:
        fake = FakeSecrets(json.dumps({"h1": {"email": "a@x"}}), version="abc")
        payload, version = _store(fake).read()
        assert payload == {"h1": {"email": "a@x"}}
        assert version == "abc"

    def test_blank_payload_is_an_empty_ledger(self) -> None:
        assert _store(FakeSecrets("   ")).read()[0] == {}

    def test_placeholder_is_refused_not_overwritten(self) -> None:
        # A fresh CDK secret holds a random placeholder string. Silently
        # replacing it with a one-entry ledger would be indistinguishable
        # from clobbering a corrupted-but-real ledger.
        with pytest.raises(MalformedLedgerError, match="not JSON"):
            _store(FakeSecrets("Xy8!plac3holder")).read()

    def test_json_array_is_refused(self) -> None:
        with pytest.raises(MalformedLedgerError, match="JSON object"):
            _store(FakeSecrets("[]")).read()


class TestWrite:
    def test_promotes_the_new_version(self) -> None:
        fake = FakeSecrets(json.dumps({}), version="v0")
        version, warning = _store(fake).write({"h1": {}}, expected_version_id="v0")
        assert warning == ""
        assert fake.current == version
        assert json.loads(fake.bodies[version]) == {"h1": {}}

    def test_clears_the_pending_label(self) -> None:
        fake = FakeSecrets(json.dumps({}), version="v0")
        version, _ = _store(fake).write({"h1": {}}, expected_version_id="v0")
        assert fake.stages[version] == []

    def test_stale_version_is_refused(self) -> None:
        # Someone else wrote between our read and our write.
        fake = FakeSecrets(json.dumps({}), version="v0")
        with pytest.raises(ConcurrentWriteError, match="re-run it"):
            _store(fake).write({"h1": {}}, expected_version_id="stale")
        assert fake.current == "v0"

    def test_promotion_failure_leaves_current_untouched(self) -> None:
        fake = FakeSecrets(json.dumps({"h0": {}}), version="v0")
        fake.fail_promote = True
        with pytest.raises(ConcurrentWriteError):
            _store(fake).write({"h1": {}}, expected_version_id="v0")
        assert json.loads(fake.bodies[fake.current]) == {"h0": {}}

    def test_cleanup_failure_is_a_warning_not_an_error(self) -> None:
        fake = FakeSecrets(json.dumps({}), version="v0")
        fake.fail_cleanup = True
        version, warning = _store(fake).write({"h1": {}}, expected_version_id="v0")
        assert fake.current == version
        assert "AWSPENDING" in warning


class TestWaitUntilLive:
    def test_requires_a_run_of_successes(self) -> None:
        statuses = iter([200, 200, 200])
        result = wait_until_live(
            "http://api",
            "k",
            streak=3,
            sleep=lambda _: None,
            probe=lambda *_: next(statuses),
        )
        assert result.live
        assert result.attempts == 3

    def test_a_stale_task_resets_the_run(self) -> None:
        # The failure mode this exists for: one Fargate task has refreshed and
        # another has not, so the key 401s at random rather than cleanly.
        statuses = iter([200, 200, 401, 200, 200, 200])
        result = wait_until_live(
            "http://api",
            "k",
            streak=3,
            sleep=lambda _: None,
            probe=lambda *_: next(statuses),
        )
        assert result.live
        assert result.attempts == 6

    def test_gives_up_at_the_deadline(self) -> None:
        result = wait_until_live(
            "http://api",
            "k",
            streak=3,
            deadline_s=0.0,
            sleep=lambda _: None,
            probe=lambda *_: 200,
        )
        assert not result.live
        assert result.attempts == 0

    def test_probe_receives_the_stats_url(self) -> None:
        seen: list[str] = []

        def _probe(url: str, _key: str) -> int:
            seen.append(url)
            return 200

        wait_until_live(
            "http://api/", "k", streak=1, sleep=lambda _: None, probe=_probe
        )
        assert seen == ["http://api/v1/stats"]

    def test_reports_the_last_status_on_failure(self) -> None:
        result = wait_until_live(
            "http://api",
            "k",
            streak=2,
            deadline_s=0.2,
            sleep=lambda _: None,
            probe=lambda *_: 401,
        )
        assert not result.live
        assert result.last_status == 401
