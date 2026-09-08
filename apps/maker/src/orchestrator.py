"""Module for managing multiple trading processes.

The orchestrator starts every process of the system, restarts nothing, and
winds everything down when one of them dies or when it is told to. The
wind-down is ordered (``SHUTDOWN_PHASES``): strategies first so no new
intents arrive; the order manager next, so it cancels what rests while the
broker is still there to execute the cancellations, the order watcher to
confirm them and the recorder to record them; then the feed handlers,
matcher and broker; the recorder last, so everything published on the way
down is on disk. Stopping all of them at once, as this used to, lost the
final cancellations from the recording and left them unconfirmed.
"""

import logging
import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from typing import Any

import apps.shared.src.logging_config as logging_config
from apps.shared.src.utils import strategies

# Setup logging
logging_config.setup_logging()
logger = logging.getLogger(__name__)


# Unique processes (start immediately)
PROCESS_LIST = [
    ["uv", "run", "-m", "apps.maker.src.broker"],
    ["uv", "run", "-m", "apps.maker.src.message_processor"],
    ["uv", "run", "-m", "apps.maker.src.watcher"],
    ["uv", "run", "-m", "apps.maker.src.order_watcher"],
    ["uv", "run", "-m", "apps.maker.src.matcher"],
    ["uv", "run", "-m", "apps.maker.src.balance"],
    ["uv", "run", "-m", "apps.maker.src.recorder"],
]

# Seconds between starting two processes, and the wait before strategies
# start, which gives the feeds time to publish their first snapshots.
PROCESS_START_DELAY_S = 1.5
STRATEGY_START_DELAY_S = 60

# Shutdown order: phase name, the modules it stops, and how long each gets
# to exit on SIGTERM before it is killed. A module named in no phase stops
# with the feeds. The order manager gets the longest grace: it cancels every
# resting order over the broker before it exits, one round trip each.
SHUTDOWN_PHASES: tuple[tuple[str, frozenset[str], float], ...] = (
    ("strategies", frozenset({"launcher"}), 5.0),
    ("order manager", frozenset({"message_processor"}), 20.0),
    (
        "feeds and broker",
        frozenset({"watcher", "order_watcher", "balance", "matcher", "broker"}),
        10.0,
    ),
    ("recorder", frozenset({"recorder"}), 10.0),
)
DEFAULT_PHASE = "feeds and broker"

Process = tuple[list[str], Any]


def module_of(cmd: Sequence[str]) -> str:
    """
    Return the short module name a launch command runs.

    Parameters
    ----------
    cmd : Sequence[str]
        The command, e.g. ``["uv", "run", "-m", "apps.maker.src.broker"]``.

    Returns
    -------
    str
        The last dotted component after ``-m``, e.g. ``broker``, or an empty
        string for a command that does not run a module.
    """
    cmd = list(cmd)
    if "-m" not in cmd:
        return ""
    return cmd[cmd.index("-m") + 1].rsplit(".", 1)[-1]


def shutdown_phases(processes: Sequence[Process]) -> list[tuple[str, list[Process], float]]:
    """
    Group processes into the order they are stopped in.

    Parameters
    ----------
    processes : Sequence[Process]
        Launch command and handle of every process, running or not.

    Returns
    -------
    list[tuple[str, list[Process], float]]
        One entry per phase in ``SHUTDOWN_PHASES`` order: name, the
        processes in it, and the grace period. Empty phases are included.
    """
    groups: dict[str, list[Process]] = {name: [] for name, _, _ in SHUTDOWN_PHASES}
    for process in processes:
        module = module_of(process[0])
        phase = next(
            (name for name, modules, _ in SHUTDOWN_PHASES if module in modules),
            DEFAULT_PHASE,
        )
        groups[phase].append(process)
    return [(name, groups[name], grace) for name, _, grace in SHUTDOWN_PHASES]


def launch_process(cmd: list[str]) -> subprocess.Popen:
    """
    Launch a process in its own process group.

    Parameters
    ----------
    cmd : list[str]
        The command to execute.

    Returns
    -------
    subprocess.Popen
        The process handle.
    """
    return subprocess.Popen(cmd, preexec_fn=os.setpgrp)


def signal_group(proc: Any, sig: int) -> None:
    """
    Send a signal to a process's whole group, ignoring one already gone.

    Parameters
    ----------
    proc : Any
        The process handle.
    sig : int
        The signal.
    """
    try:
        os.killpg(os.getpgid(proc.pid), sig)
    except ProcessLookupError:
        pass


def stop_processes(
    processes: Sequence[Process],
    grace_s: float,
    terminate: Callable[[Any], None] | None = None,
    kill: Callable[[Any], None] | None = None,
) -> list[Process]:
    """
    Stop a group of processes together: SIGTERM all, wait, SIGKILL the rest.

    Parameters
    ----------
    processes : Sequence[Process]
        The processes to stop.
    grace_s : float
        Seconds the group as a whole gets to exit after SIGTERM.
    terminate : Callable[[Any], None] | None
        Sends the termination signal; defaults to SIGTERM to the group.
    kill : Callable[[Any], None] | None
        Sends the kill signal; defaults to SIGKILL to the group.

    Returns
    -------
    list[Process]
        The processes that had to be killed.
    """
    terminate = terminate or (lambda proc: signal_group(proc, signal.SIGTERM))
    kill = kill or (lambda proc: signal_group(proc, signal.SIGKILL))
    running = [process for process in processes if process[1].poll() is None]
    for _cmd, proc in running:
        terminate(proc)
    deadline = time.monotonic() + grace_s
    killed: list[Process] = []
    for process in running:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            process[1].wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            logger.error(f"{module_of(process[0])} ignored SIGTERM for {grace_s}s, killing")
            kill(process[1])
            killed.append(process)
    return killed


def shutdown(processes: Sequence[Process]) -> None:
    """
    Stop every process in ``SHUTDOWN_PHASES`` order.

    Parameters
    ----------
    processes : Sequence[Process]
        Launch command and handle of every process.
    """
    for name, group, grace in shutdown_phases(processes):
        running = [p for p in group if p[1].poll() is None]
        if not running:
            continue
        logger.info(f"Stopping {name}: {[module_of(cmd) for cmd, _ in running]}")
        stop_processes(running, grace)


def check_identifiers(configured: list[dict[str, Any]]) -> None:
    """
    Refuse to start with missing or duplicate strategy identifiers.

    Parameters
    ----------
    configured : list[dict[str, Any]]
        The active strategies.

    Raises
    ------
    Exception
        If a strategy has no identifier or two share one.
    """
    identified: set[str] = set()
    for strategy in configured:
        identifier = strategy.get("identifier")
        if identifier is None:
            raise Exception("Missing strategy identifier")
        if identifier in identified:
            raise Exception(f"Duplicate strategy identifier: {identifier}")
        identified.add(identifier)


def main() -> None:
    """Start every process, then wind down on a signal or a crash."""
    if strategies is None:
        raise Exception("No strategy could be loaded")
    check_identifiers(strategies)

    processes: list[Process] = []

    def cleanup_and_exit(exit_code: int = 1) -> None:
        print("Cleaning up processes...")
        shutdown(processes)
        sys.exit(exit_code)

    def handle_exit(_sig: int, _frame: Any) -> None:
        cleanup_and_exit(0)

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    for cmd in PROCESS_LIST:
        processes.append((cmd, launch_process(cmd)))
        time.sleep(PROCESS_START_DELAY_S)

    time.sleep(STRATEGY_START_DELAY_S)
    logger.info("Starting strategy processes after delay...")
    for i, _strategy in enumerate(strategies):
        cmd = ["uv", "run", "-m", "apps.maker.src.launcher", str(i)]
        processes.append((cmd, launch_process(cmd)))

    while True:
        for cmd, proc in processes:
            if proc.poll() is not None:
                logger.error(f"Process {module_of(cmd)} crashed unexpectedly, winding down.")
                cleanup_and_exit(1)
        time.sleep(2)


if __name__ == "__main__":
    main()
