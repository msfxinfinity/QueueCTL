# runner.py
# A unified runner that starts workers and submits jobs automatically.
# This lets you run your entire queue system from ONE file.

import os
import time
import json
import signal
from multiprocessing import Process

from storage import Storage
from worker import start_workers, STOP_FLAG, PIDFILE

storage = Storage()

RUNNER_STOP = False

def handle_sigint(sig, frame):
    global RUNNER_STOP
    print("\n[runner] Ctrl+C detected, stopping...")
    RUNNER_STOP = True
    # create stop flag for workers
    open(STOP_FLAG, "w").close()

signal.signal(signal.SIGINT, handle_sigint)


def wait_for_workers():
    """Wait until workers_meta table has at least one worker registered."""
    print("[runner] Waiting for workers to register...")
    while storage.count_workers() == 0:
        time.sleep(0.2)
    print(f"[runner] {storage.count_workers()} workers active.")


def auto_enqueue_loop():
    """Continuously add example jobs into queue."""
    counter = 1
    while not RUNNER_STOP:
        cmd = f"echo 'job {counter}: processed'"
        job = {
            "command": cmd,
            "max_retries": 3
        }
        jid = storage.add_job(job)
        print(f"[runner] Enqueued job {jid}: {cmd}")
        counter += 1
        time.sleep(3)  # every 3 seconds create a job


def status_loop():
    """Print queue status periodically."""
    while not RUNNER_STOP:
        counts = storage.state_counts()
        workers = storage.count_workers()
        print("\n[runner-status]")
        print("----------------------------")
        print(f"workers: {workers}")
        for s, c in counts.items():
            print(f"{s}: {c}")
        print("----------------------------\n")

        time.sleep(5)


def main():
    print("=== QueueCTL Runner ===")
    print("Starting workers...")

    # Start workers
    start_workers(count=2, daemon=True)

    wait_for_workers()

    # Start background processes
    enqueue_proc = Process(target=auto_enqueue_loop)
    status_proc = Process(target=status_loop)

    enqueue_proc.start()
    status_proc.start()

    print("[runner] System running. Press CTRL + C to stop.")

    try:
        while not RUNNER_STOP:
            time.sleep(0.5)
    finally:
        print("[runner] Cleaning up...")
        enqueue_proc.terminate()
        status_proc.terminate()

        # Remove flags and pidfile
        if os.path.exists(PIDFILE):
            os.remove(PIDFILE)
        if os.path.exists(STOP_FLAG):
            os.remove(STOP_FLAG)

        print("[runner] Shutdown complete.")


if __name__ == "__main__":
    main()
