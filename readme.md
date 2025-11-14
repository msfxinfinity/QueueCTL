📦 1. Setup Instructions
🔧 Requirements

Python 3.8+

SQLite3 (pre-installed on most systems)

📥 Install Dependencies
pip3 install -r requirements.txt

▶️ Initialize the database
python3 queuectl.py init

▶️ Start workers

Start 2 workers (or any number you configure):

python3 queuectl.py worker start --count 2


Workers will automatically:

Poll for new jobs

Claim jobs atomically (SQLite row-level update)

Execute commands

Retry with exponential backoff

Move permanent failures to DLQ

🧪 Try your first job
python3 queuectl.py queue add "echo 'hello world'"

🖥️ 2. Usage Examples
📌 Add a job
python3 queuectl.py queue add "ls -la"


Output:

Job 14 enqueued

📌 List jobs
python3 queuectl.py queue list


Possible output:

ID | State     | Attempts | Next Run | Command
-----------------------------------------------
14 | pending   | 0        | now      | ls -la
8  | failed    | 3        | +120s    | ping fake.com

📌 Check system status
python3 queuectl.py status


Output Example:

Jobs by state:
  pending:     3
  running:     1
  completed:   8
  failed:      2
Active workers: 2

📌 View Dead Letter Queue (DLQ)
python3 queuectl.py dlq list


Output:

ID | Error | Command
---------------------------------------------
5  | rc=1 out=timeout... | curl http://invalid

📌 Retry job from DLQ
python3 queuectl.py dlq retry 5

📌 Stop all workers gracefully
python3 queuectl.py worker stop


Workers finish the current job, unregister, and exit.

🏗️ 3. Architecture Overview
📌 Core Components
1. Storage Layer (storage.py)

Uses SQLite as persistent storage

Handles:

Enqueuing jobs

Claiming jobs with atomic update:

UPDATE jobs SET claimed_by=?, claimed_at=NOW() 
   WHERE id = ? AND (claimed_by IS NULL OR claim_expired)


Updating job results

DLQ operations

Worker tracking

Configuration table

2. Worker Logic (worker.py)

Each worker process:

Registers its PID

Polls for available jobs

Atomically claims a job

Executes the command

Applies retry logic:

Exponential backoff:

delay = base_backoff ** attempts


Moves permanently failed jobs → DLQ

Supports graceful shutdown via workers.stop flag

3. CLI Interface (queuectl.py)

Commands include:

queue add

queue list

dlq list

dlq retry

worker start

worker stop

status

config set/get

4. Dead Letter Queue (dlq.py)

Jobs that exhaust retries are stored here and can be manually retried.

📌 Job Lifecycle
enqueue → pending → claimed_by worker → running
 → (success → completed)
 → (failure → retry with backoff → failed)
 → (failure > max_retries → DLQ)

⚖️ 4. Assumptions & Trade-offs
✔ Assumptions

Commands executed by workers are shell commands.

SQLite is sufficient for local queueing.

Workers run on the same machine.

⚙️ Trade-offs

SQLite does not scale horizontally, but is ideal for local queues.

Workers poll every 0.5s (configurable); event-driven wakeups would require more complexity.

Exponential backoff is simplified:

delay = base^attempts (capped at 3600s)


No authentication/permissions in CLI for simplicity.

🧪 5. Testing Instructions
1️⃣ Start workers in a separate terminal
python3 queuectl.py worker start --count 2

2️⃣ Add sample jobs
python3 queuectl.py queue add "echo test"
python3 queuectl.py queue add "sleep 2 && echo done"
python3 queuectl.py queue add "exit 1"

3️⃣ Check job processing
python3 queuectl.py queue list

4️⃣ Test retry logic

Add a failing job:

python3 queuectl.py queue add "curl http://invalid-url"


Watch exponential retries in worker logs.

5️⃣ Test DLQ
python3 queuectl.py dlq list
python3 queuectl.py dlq retry <id>

6️⃣ Stop workers gracefully
python3 queuectl.py worker stop

"https://drive.google.com/drive/folders/1vYAPYH2VE322dlHu4uL1jvz3os3m8e9n?usp=sharing" - Link to the CLI Demo File
