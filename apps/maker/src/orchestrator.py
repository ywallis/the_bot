import logging
import os
import signal
import subprocess
import sys
import time

from dotenv import load_dotenv

import apps.shared.src.logging_config as logging_config
from apps.shared.src.utils import load_config

# Setup logging
logging_config.setup_logging()
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
production = os.getenv("PRODUCTION", False)

# Load configuration
config = load_config()
strategies = config.get("strategies")

# Check for duplicate strategy identifiers
identified_strategies: list[str] = []
if strategies is None:
    raise Exception("No strategy could be loaded")
for strategy in strategies:
    identifier = strategy.get("identifier")
    if identifier in identified_strategies:
        raise Exception(f"Duplicate strategy identifier: {identifier}")
    else:
        identified_strategies.append(identifier)


# Unique processes (start immediately)
PROCESS_LIST = [
    ["uv", "run", "-m", "apps.maker.src.broker"],
    ["uv", "run", "-m", "apps.maker.src.message_processor"],
    ["uv", "run", "-m", "apps.maker.src.watcher"],
    ["uv", "run", "-m", "apps.maker.src.matcher"],
    ["uv", "run", "-m", "apps.maker.src.balance"],
]


def launch_process(cmd):
    """Launch a process in its own process group."""
    return subprocess.Popen(cmd, preexec_fn=os.setpgrp)


def cleanup_and_exit(exit_code=1):
    """Terminate all running processes and exit."""
    print("Cleaning up processes...")
    for _, proc in processes:
        if proc.poll() is None:  # Only kill if the process is still running
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)  # Graceful termination
                proc.wait(timeout=3)  # Wait for process to exit
            except ProcessLookupError:
                pass  # Process already exited, nothing to do
            except Exception:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)  # Force kill if needed
    sys.exit(exit_code)


# Register signal handlers for manual termination (Ctrl+C)
def handle_exit(_sig, _frame):
    cleanup_and_exit(0)  # Normal exit on SIGINT/SIGTERM


signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

if __name__ == "__main__":
    # Start unique processes immediately
    # processes = [(cmd, launch_process(cmd)) for cmd in PROCESS_LIST]
    processes = []
    for cmd in PROCESS_LIST:
        proc = launch_process(cmd)
        processes.append((cmd, proc))
        time.sleep(1.5)  # Delay of 1 second between each process launch

    # Wait 1 min before launching strategy processes
    time.sleep(60)
    logger.info("Starting strategy processes after delay...")

    # Start strategy processes after delay
    for i, strategy in enumerate(strategies):
        cmd = ["uv", "run", "-m", "apps.maker.src.launcher", str(i)]
        processes.append((cmd, launch_process(cmd)))

    # Process monitoring loop
    while True:
        for i, (cmd, proc) in enumerate(processes):
            if proc.poll() is not None:  # Process exited
                if production:
                    # Restart process if in production mode
                    print(f"Process {cmd} crashed. Restarting...")
                    processes[i] = (cmd, launch_process(cmd))
                else:
                    logger.error(f"Process {proc} crashed unexpectedly, winding down.")
                    cleanup_and_exit(1)
        time.sleep(2)
