import os
import signal
import subprocess
import sys
import time

PROCESS_LIST = [
    ["uv", "run", "-m", "apps.maker.src.balance"],
    ["uv", "run", "-m", "apps.maker.src.watcher"],
]

processes = []

def launch_process(cmd):
    """Launch a process in its own process group."""
    return subprocess.Popen(cmd, preexec_fn=os.setpgrp)

def cleanup_and_exit(_signum, _frame):
    """Terminate all processes properly."""
    print("Terminating all processes...")

    for _, proc, _ in processes:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)  # Kill process group
            proc.wait(timeout=3)  # Give it time to exit
        except Exception:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)  # Force kill

    sys.exit(0)

# Register signal handlers for graceful shutdown
signal.signal(signal.SIGINT, cleanup_and_exit)
signal.signal(signal.SIGTERM, cleanup_and_exit)

if __name__ == "__main__":
    processes = [(cmd, launch_process(cmd), time.time()) for cmd in PROCESS_LIST]
    while True:
        for i, (cmd, proc, start_time) in enumerate(processes):
            if proc.poll() is not None:  # Process exited
                print(f"Process {cmd} crashed. Restarting...")
                processes[i] = (cmd, launch_process(cmd), time.time())
        time.sleep(2)
