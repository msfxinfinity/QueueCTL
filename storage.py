# storage.py
# Responsible for DB access, schema, config and atomic job claiming.

import sqlite3
import time
import datetime
from typing import Optional, Dict, Any, Tuple, List

DB_PATH = "jobs.sqlite"

def iso_now() -> str:
    return datetime.datetime.utcnow().isoformat() + "Z"

class Storage:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        # allow multithread/process access
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.init_schema()

    def init_schema(self):
        cur = self.conn.cursor()
        cur.executescript("""
        PRAGMA journal_mode = WAL;
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id TEXT UNIQUE,
            command TEXT NOT NULL,
            state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 3,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            available_at INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            worker_pid INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_state_available ON jobs(state, available_at);
        
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS workers_meta (
            pid INTEGER PRIMARY KEY,
            started_at TEXT
        );
        """)
        # defaults
        cur.execute("INSERT OR IGNORE INTO config(key, value) VALUES(?,?)", ("backoff_base", "2"))
        cur.execute("INSERT OR IGNORE INTO config(key, value) VALUES(?,?)", ("default_max_retries", "3"))
        self.conn.commit()

    # ------------- config helpers -------------
    def get_config(self, key: str, default: Optional[str] = None) -> Optional[str]:
        cur = self.conn.cursor()
        cur.execute("SELECT value FROM config WHERE key = ?", (key,))
        row = cur.fetchone()
        return row["value"] if row else default

    def set_config(self, key: str, value: str):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO config(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value)
        )
        self.conn.commit()

    # ------------- job CRUD -------------
    def add_job(self, job: Dict[str, Any]) -> int:
        """
        job: dictionary possibly containing:
          - external_id (optional): string id provided by user
          - command: required
          - max_retries: optional
          - available_at: optional (unix ts)
        Returns the internal integer id.
        """
        now = iso_now()
        cmd = job.get("command")
        if not cmd:
            raise ValueError("job must include 'command'")

        external_id = job.get("id") or job.get("external_id")  # accept either
        attempts = int(job.get("attempts", 0))
        max_retries = int(job.get("max_retries", self.get_config("default_max_retries", "3")))
        available_at = int(job.get("available_at", 0))

        cur = self.conn.cursor()
        try:
            cur.execute("""
            INSERT INTO jobs(external_id, command, state, attempts, max_retries, created_at, updated_at, available_at)
            VALUES (?, ?, 'pending', ?, ?, ?, ?, ?)
            """, (external_id, cmd, attempts, max_retries, now, now, available_at))
            self.conn.commit()
            return cur.lastrowid
        except sqlite3.IntegrityError as e:
            # likely duplicate external_id
            raise

    def get_job(self, internal_id: int) -> Optional[sqlite3.Row]:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id = ?", (internal_id,))
        return cur.fetchone()

    def list_jobs(self, state: Optional[str] = None, limit: int = 100) -> List[sqlite3.Row]:
        cur = self.conn.cursor()
        if state:
            cur.execute("SELECT * FROM jobs WHERE state = ? ORDER BY created_at DESC LIMIT ?", (state, limit))
        else:
            cur.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,))
        return cur.fetchall()

    # atomic claim
    def claim_next_job(self, worker_pid: int) -> Optional[sqlite3.Row]:
        """
        Atomically claim one pending job whose available_at <= now.
        Returns the claimed job (row) or None.
        """
        cur = self.conn.cursor()
        now_ts = int(time.time())
        try:
            cur.execute("BEGIN IMMEDIATE")
            cur.execute("""
            SELECT * FROM jobs
            WHERE state = 'pending' AND available_at <= ?
            ORDER BY created_at ASC
            LIMIT 1
            """, (now_ts,))
            row = cur.fetchone()
            if not row:
                self.conn.commit()
                return None
            job_id = row["id"]
            cur.execute("UPDATE jobs SET state='processing', worker_pid=?, updated_at=? WHERE id=?",
                        (worker_pid, iso_now(), job_id))
            self.conn.commit()
            cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
            return cur.fetchone()
        except sqlite3.OperationalError:
            try:
                self.conn.rollback()
            except Exception:
                pass
            return None

    def update_job_result(self, job_id: int, state: str, attempts: int,
                          last_error: Optional[str] = None, available_at: int = 0):
        cur = self.conn.cursor()
        cur.execute("""
        UPDATE jobs
        SET state = ?, attempts = ?, last_error = ?, updated_at = ?, available_at = ?, worker_pid = NULL
        WHERE id = ?
        """, (state, attempts, last_error, iso_now(), available_at, job_id))
        self.conn.commit()

    def move_to_dead(self, job_id: int, last_error: Optional[str] = None):
        self.update_job_result(job_id, "dead", self.get_job_attempts(job_id), last_error, 0)

    def get_job_attempts(self, job_id: int) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT attempts FROM jobs WHERE id = ?", (job_id,))
        r = cur.fetchone()
        return r["attempts"] if r else 0

    # worker meta
    def register_worker(self, pid: int):
        cur = self.conn.cursor()
        cur.execute("INSERT OR IGNORE INTO workers_meta(pid, started_at) VALUES(?, ?)", (pid, iso_now()))
        self.conn.commit()

    def unregister_worker(self, pid: int):
        cur = self.conn.cursor()
        cur.execute("DELETE FROM workers_meta WHERE pid = ?", (pid,))
        self.conn.commit()

    def count_workers(self) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) as c FROM workers_meta")
        return cur.fetchone()["c"]

    # status counts
    def state_counts(self):
        cur = self.conn.cursor()
        cur.execute("SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state")
        return {r["state"]: r["cnt"] for r in cur.fetchall()}

    # DLQ helpers
    def list_dead(self, limit: int = 100):
        return self.list_jobs("dead", limit)

    def retry_dead_job(self, job_id: int):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id = ? AND state = 'dead'", (job_id,))
        r = cur.fetchone()
        if not r:
            raise KeyError("dead job not found")
        cur.execute("UPDATE jobs SET state='pending', attempts=0, updated_at=?, available_at=? WHERE id=?",
                    (iso_now(), 0, job_id))
        self.conn.commit()
