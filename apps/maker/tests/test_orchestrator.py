"""Tests for the orchestrator's ordered shutdown."""

import subprocess
from typing import Any

import pytest

from apps.maker.src.orchestrator import (
    PROCESS_LIST,
    SHUTDOWN_PHASES,
    check_identifiers,
    module_of,
    shutdown,
    shutdown_phases,
    stop_processes,
)


class FakeProc:
    """A process handle that exits on SIGTERM only if told to."""

    def __init__(self, exits_on_term: bool = True, running: bool = True) -> None:
        """Set whether the process honours SIGTERM and whether it is running."""
        self.exits_on_term = exits_on_term
        self.running = running
        self.signals: list[str] = []
        self.pid = id(self)

    def poll(self) -> int | None:
        """Return None while running."""
        return None if self.running else 0

    def wait(self, timeout: float | None = None) -> int:
        """Exit if SIGTERM was honoured, otherwise time out."""
        if "term" in self.signals and self.exits_on_term:
            self.running = False
            return 0
        raise subprocess.TimeoutExpired("fake", timeout or 0)


def cmd(module: str, *args: str) -> list[str]:
    """Return a launch command for a module."""
    return ["uv", "run", "-m", f"apps.maker.src.{module}", *args]


def terminate(proc: Any) -> None:
    """Record a SIGTERM."""
    proc.signals.append("term")


def kill(proc: Any) -> None:
    """Record a SIGKILL and stop the process."""
    proc.signals.append("kill")
    proc.running = False


def test_module_of_reads_the_module_after_dash_m():
    """The short module name comes from the ``-m`` argument."""
    assert module_of(cmd("broker")) == "broker"
    assert module_of(cmd("launcher", "0")) == "launcher"
    assert module_of(["python", "script.py"]) == ""


def test_every_launched_module_has_a_phase():
    """Nothing the orchestrator starts falls through to the default by accident."""
    named = set().union(*(modules for _, modules, _ in SHUTDOWN_PHASES))
    for command in PROCESS_LIST:
        assert module_of(command) in named, command
    assert "launcher" in named


def test_shutdown_phases_order_strategies_oms_feeds_recorder():
    """Strategies stop first, the order manager second, the recorder last."""
    processes = [(c, FakeProc()) for c in PROCESS_LIST] + [
        (cmd("launcher", "0"), FakeProc()),
        (cmd("launcher", "1"), FakeProc()),
        (cmd("unknown_tool"), FakeProc()),
    ]
    phases = shutdown_phases(processes)
    by_name = {name: sorted(module_of(c) for c, _ in group) for name, group, _ in phases}
    assert [name for name, _, _ in phases] == [
        "strategies",
        "order manager",
        "feeds and broker",
        "recorder",
    ]
    assert by_name["strategies"] == ["launcher", "launcher"]
    assert by_name["order manager"] == ["message_processor"]
    assert by_name["feeds and broker"] == [
        "balance",
        "broker",
        "matcher",
        "order_watcher",
        "unknown_tool",
        "watcher",
    ]
    assert by_name["recorder"] == ["recorder"]


def test_the_order_manager_gets_the_longest_grace():
    """Cancelling every resting order takes a broker round trip each."""
    grace = {name: g for name, _, g in SHUTDOWN_PHASES}
    assert grace["order manager"] == max(grace.values())


def test_stop_processes_terminates_then_kills_the_stubborn():
    """Everything gets SIGTERM; only what ignores it gets SIGKILL."""
    polite = FakeProc(exits_on_term=True)
    stubborn = FakeProc(exits_on_term=False)
    gone = FakeProc(running=False)
    processes = [(cmd("watcher"), polite), (cmd("broker"), stubborn), (cmd("matcher"), gone)]

    killed = stop_processes(processes, grace_s=0.01, terminate=terminate, kill=kill)

    assert polite.signals == ["term"]
    assert stubborn.signals == ["term", "kill"]
    assert gone.signals == []
    assert [module_of(c) for c, _ in killed] == ["broker"]
    assert not polite.running and not stubborn.running


def test_shutdown_stops_phases_in_order(monkeypatch):
    """Each phase is fully stopped before the next one is signalled."""
    order: list[str] = []
    procs = {module_of(c): FakeProc() for c in PROCESS_LIST}
    procs["launcher"] = FakeProc()
    processes = [(cmd(m), p) for m, p in procs.items()]

    def fake_stop(group, grace_s, terminate=None, kill=None):
        for c, p in group:
            order.append(module_of(c))
            p.running = False
        return []

    monkeypatch.setattr("apps.maker.src.orchestrator.stop_processes", fake_stop)
    shutdown(processes)

    assert order[0] == "launcher"
    assert order[1] == "message_processor"
    assert order[-1] == "recorder"
    assert set(order[2:-1]) == {"broker", "watcher", "order_watcher", "matcher", "balance"}


def test_check_identifiers_rejects_missing_and_duplicate():
    """Two strategies cannot share an identifier and none may lack one."""
    check_identifiers([{"identifier": "a"}, {"identifier": "b"}])
    with pytest.raises(Exception, match="Duplicate"):
        check_identifiers([{"identifier": "a"}, {"identifier": "a"}])
    with pytest.raises(Exception, match="Missing"):
        check_identifiers([{"type": "x"}])
