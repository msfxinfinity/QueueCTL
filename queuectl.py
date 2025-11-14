# queuectl.py
import click
import json
import os
import uuid
from storage import Storage
from worker import start_workers, PIDFILE, STOP_FLAG
import sys

storage = Storage()

@click.group()
def cli():
    pass

@cli.command(help="Enqueue a job. Provide JSON with at least 'command'. Optional: id (external), max_retries.")
@click.argument("job_json")
def enqueue(job_json):
    try:
        job = json.loads(job_json)
    except Exception as e:
        raise click.ClickException(f"Invalid JSON: {e}")
    # generate external id if none provided
    if "id" not in job and "external_id" not in job:
        job["external_id"] = str(uuid.uuid4())
    try:
        internal_id = storage.add_job(job)
    except Exception as e:
        raise click.ClickException(f"Failed to add job: {e}")
    click.echo(f"Enqueued job internal_id={internal_id} external_id={job.get('external_id') or job.get('id')}")

@cli.group(help="Worker management")
def worker():
    pass

@worker.command("start", help="Start N workers")
@click.option("--count", "-c", default=1, help="Number of workers")
@click.option("--daemon/--no-daemon", default=False, help="Run in background (no foreground join)")
def start(count, daemon):
    start_workers(count, daemon=daemon)

@worker.command("stop", help="Stop workers gracefully")
def stop():
    # create stop flag and try to signal PIDs in pidfile
    open(STOP_FLAG, "w").close()
    click.echo(f"Created stop flag: {STOP_FLAG}")
    if os.path.exists(PIDFILE):
        with open(PIDFILE, "r") as f:
            for line in f:
                try:
                    pid = int(line.strip())
                    click.echo(f"Sending SIGTERM to {pid}")
                    os.kill(pid, 15)
                except Exception as e:
                    click.echo(f"Could not signal {line.strip()}: {e}")
        try:
            os.remove(PIDFILE)
        except Exception:
            pass

@cli.command("status", help="Show job counts and active workers")
def status():
    counts = storage.state_counts()
    workers = storage.count_workers()
    click.echo("Jobs by state:")
    for s, c in counts.items():
        click.echo(f"  {s}: {c}")
    click.echo(f"Active workers (tracked): {workers}")

@cli.command("list", help="List jobs; use --state to filter")
@click.option("--state", "-s", default=None, help="Filter by state")
@click.option("--limit", "-n", default=100)
def list_jobs(state, limit):
    rows = storage.list_jobs(state, limit)
    for r in rows:
        d = dict(r)
        click.echo(json.dumps(d))

@cli.group("dlq", help="DLQ operations")
def dlq():
    pass

@dlq.command("list", help="List dead jobs")
def dlq_list():
    rows = storage.list_dead()
    for r in rows:
        click.echo(json.dumps(dict(r)))

@dlq.command("retry", help="Retry a dead job (by internal id)")
@click.argument("job_id", type=int)
def dlq_retry(job_id):
    try:
        storage.retry_dead_job(job_id)
        click.echo(f"Retried job {job_id}")
    except KeyError:
        raise click.ClickException("Dead job not found")
    except Exception as e:
        raise click.ClickException(str(e))

@cli.group("config", help="Configuration commands")
def cfg():
    pass

@cfg.command("set", help="Set config key value")
@click.argument("key")
@click.argument("value")
def cfg_set(key, value):
    storage.set_config(key, value)
    click.echo(f"Set {key} = {value}")

@cfg.command("get", help="Get config value")
@click.argument("key")
def cfg_get(key):
    val = storage.get_config(key)
    click.echo(val if val is not None else "")

@cli.command("dropdb", help="Delete DB and reset (dev only)", )
@click.confirmation_option(prompt="Are you sure?")
def dropdb():
    path = storage.db_path
    try:
        storage.conn.close()
    except Exception:
        pass
    if os.path.exists(path):
        os.remove(path)
    if os.path.exists(PIDFILE):
        os.remove(PIDFILE)
    if os.path.exists(STOP_FLAG):
        os.remove(STOP_FLAG)
    click.echo("Removed DB and worker flags")

if __name__ == "__main__":
    cli()
