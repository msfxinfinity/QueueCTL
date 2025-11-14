# worker.py
# Worker process logic. Uses Storage.claim_next_job for atomic claiming.

import os
import signal
import time
import subprocess
from multiprocessing import Process, current_process
from typing import Optional
from storage import Storage
import math

STOP_FLAG = "workers.stop"
PIDFILE = "workers.pid"

storage = Storage()

def run_command(command: str, timeout: Optional[int] = None) -> (int, str):
    try:
        proc = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              timeout=timeout, text=True)
        return proc.returncode, proc.stdout
    except subprocess.TimeoutExpired as te:
        return -1, f"timeout: {te}"
    except Exception as ex:
        return -1, str(ex)

def worker_loop(worker_index: int):
    pid = os.getpid()
    storage.register_worker(pid)
    worker_name = f"{pid}-{worker_index}"
    click_echo = print
    click_echo(f"[worker {worker_name}] started")
    backoff_base = float(storage.get_config("backoff_base", "2") or 2)

    try:
        while True:
            # check stop flag between jobs
            if os.path.exists(STOP_FLAG):
                click_echo(f"[worker {worker_name}] stop flag detected, exiting after current job")
                break

            job = storage.claim_next_job(pid)
            if not job:
                time.sleep(0.5)
                continue

            job_id = job["id"]
            command = job["command"]
            attempts = int(job["attempts"])
            max_retries = int(job["max_retries"])

            click_echo(f"[worker {worker_name}] claimed job {job_id} attempts={attempts}/{max_retries} cmd={command}")

            rc, output = run_command(command, timeout=300)
            if rc == 0:
                storage.update_job_result(job_id, "completed", attempts, None, 0)
                click_echo(f"[worker {worker_name}] job {job_id} completed")
            else:
                attempts += 1
                # compute backoff: base ** attempts (bounded)
                delay = int(math.pow(backoff_base, attempts))
                if delay > 3600:  # cap at 1 hour
                    delay = 3600
                avail = int(time.time()) + delay
                last_err = f"rc={rc} out={(output or '')[:400]}"
                if attempts > max_retries:
                    storage.move_to_dead(job_id, last_err)
                    click_echo(f"[worker {worker_name}] job {job_id} moved to DLQ (attempts={attempts})")
                else:
                    storage.update_job_result(job_id, "failed", attempts, last_err, avail)
                    click_echo(f"[worker {worker_name}] job {job_id} failed -> retry in {delay}s (attempt {attempts})")
            # loop continues
    except KeyboardInterrupt:
        click_echo(f"[worker {worker_name}] interrupted")
    finally:
        storage.unregister_worker(pid)
        click_echo(f"[worker {worker_name}] stopped")


def start_workers(count: int, daemon: bool = False):
    procs = []
    for i in range(count):
        p = Process(target=worker_loop, args=(i,))
        p.daemon = False
        p.start()
        procs.append(p)

    # write PIDs
    with open(PIDFILE, "w") as f:
        for p in procs:
            f.write(str(p.pid) + "\n")

    print(f"Started {len(procs)} workers (PIDs written to {PIDFILE})")
    if not daemon:
        def handle_sigint(sig, frame):
            print("SIGINT received: stop workers gracefully")
            open(STOP_FLAG, "w").close()
        signal.signal(signal.SIGINT, handle_sigint)
        try:
            for p in procs:
                p.join()
        finally:
            if os.path.exists(PIDFILE):
                os.remove(PIDFILE)
            if os.path.exists(STOP_FLAG):
                os.remove(STOP_FLAG)
            print("All workers exited")
