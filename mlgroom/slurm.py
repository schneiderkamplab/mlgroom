import click
import yaml
import subprocess
from pathlib import Path
from datetime import datetime
import re

def parse_ranges(ranges):
    result = set()
    for r in ranges or []:
        if isinstance(r, int):
            result.add(r)
        elif '-' in str(r):
            start, end = map(int, str(r).split('-'))
            result.update(range(start, end + 1))
    return result

def chunkify(start, end, size):
    return [(i, min(i + size - 1, end)) for i in range(start, end + 1, size)]

def get_total_jobs(user):
    cmd = ["squeue", "-u", user, "-h", "-o", "%i|%T"]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, text=True)
    lines = result.stdout.strip().split("\n")
    count = 0
    for line in lines:
        parts = line.strip().split("|")
        if len(parts) != 2:
            continue
        jobid_field, state = parts
        if state not in {"PENDING", "RUNNING", "CONFIGURING", "COMPLETING"}:
            continue
        m = re.match(r"(\d+)_\[(.+)\]", jobid_field)
        if m:
            task_ranges = m.group(2)
            for part in task_ranges.split(","):
                if "-" in part:
                    start, end = map(int, part.split("-"))
                    count += end - start + 1
                else:
                    count += 1
        else:
            count += 1
    return count

def get_completed_job_ids():
    """Return a dict of job_id -> state for jobs in terminal states."""
    cmd = ["sacct", "--format=JobID,State", "--parsable2", "--noheader"]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, text=True)
    completed = {}
    for line in result.stdout.strip().split("\n"):
        parts = line.strip().split("|")
        if len(parts) == 2:
            jobid, state = parts
            if re.match(r'COMPLETED|FAILED|CANCELLED|TIMEOUT|OUT_OF_MEMORY', state):
                completed[jobid.split('.')[0]] = state
    return completed

def format_ranges(numbers):
    if not numbers:
        return []
    sorted_nums = sorted(set(numbers))
    ranges = []
    start = prev = sorted_nums[0]
    for n in sorted_nums[1:]:
        if n == prev + 1:
            prev = n
        else:
            ranges.append(f"{start}-{prev}" if start != prev else str(start))
            start = prev = n
    ranges.append(f"{start}-{prev}" if start != prev else str(start))
    return ranges

def log_message(log_file, level, message):
    if not log_file:
        return
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as f:
        f.write(f"[{timestamp}] {level.upper()} {message}\n")

def update_completed_jobs(jobs, completed_jobs, log_file=None):
    for job in jobs:
        new_completed = set()
        job_ids = job.get("job_ids", {})
        for chunk, jid in job_ids.items():
            if str(jid) in completed_jobs:
                # e.g. chunk = "1-10"
                if "-" in chunk:
                    start, end = map(int, chunk.split("-"))
                    new_completed.update(range(start, end + 1))
                else:
                    new_completed.add(int(chunk))
        current = parse_ranges(job.get("completed", []))
        updated = sorted(current.union(new_completed))
        if new_completed:
            log_message(log_file, "info", f"Marked chunks as completed for {job['name']}: {format_ranges(new_completed)}")
        job["completed"] = format_ranges(updated)

def submit_array(script_path, name, start, end, dry_run=False, log_file=None):
    array_spec = f"--array={start}-{end}"
    cmd = ["sbatch", array_spec, "--job-name", name, script_path]
    if dry_run:
        msg = f"Would submit {name} {start}-{end}"
        click.echo(f"[DRY-RUN] {msg}")
        log_message(log_file, "dryrun", msg)
        return True, 0
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        msg = f"Submit failed for {name} {start}-{end}: {result.stderr.strip()}"
        click.echo(f"[ERROR] {msg}")
        log_message(log_file, "error", msg)
        return False, None
    match = re.search(r'Submitted batch job (\d+)', result.stdout)
    if match:
        job_id = int(match.group(1))
        msg = f"Submitted {name} {start}-{end} as job ID {job_id}"
        click.echo(f"[OK] {msg}")
        log_message(log_file, "info", msg)
        return True, job_id
    else:
        msg = f"Submission for {name} {start}-{end} did not return a job ID."
        click.echo(f"[WARN] {msg}")
        log_message(log_file, "warn", msg)
        return False, None

@click.command()
@click.option("--queue-file", default="groom.yml", type=click.Path(exists=True), help="Queue file to groom (default: groom.yml)")
@click.option("--user", default=None, help="Username for SLURM squeue/sacct")
@click.option("--dry-run", is_flag=True, help="Do not submit jobs, only print what would happen")
@click.option("--log-file", default="groom.log", type=click.Path(), help="Log file to append job activity to (default: groom.log)")
def groom(queue_file, user, dry_run, log_file):
    path = Path(queue_file)
    with open(path) as f:
        data = yaml.safe_load(f)
    with open(path) as f:
        original_data = yaml.safe_load(f)

    user = user or subprocess.getoutput("whoami")
    chunk_size = data.get("chunk_size", 10)
    max_jobs = data.get("max_jobs", 200)
    current_jobs = get_total_jobs(user)
    remaining_slots = max_jobs - current_jobs

    if remaining_slots <= 0:
        msg = f"No slots available ({current_jobs}/{max_jobs} jobs running)."
        click.echo(f"[INFO] {msg}")
        log_message(log_file, "info", msg)
        return

    completed_ids = get_completed_job_ids()
    update_completed_jobs(data["jobs"], completed_ids, log_file=log_file)

    for job in data["jobs"]:
        name = job["name"]
        total = job["total"]
        submitted = parse_ranges(job.get("submitted", []))
        completed = parse_ranges(job.get("completed", []))
        job.setdefault("job_ids", {})
        handled = submitted.union(completed)

        all_chunks = chunkify(1, total, chunk_size)
        available_chunks = [
            (start, end) for start, end in all_chunks
            if set(range(start, end + 1)).isdisjoint(handled)
        ]

        for start, end in available_chunks:
            size = end - start + 1
            if size > remaining_slots:
                continue
            success, job_id = submit_array(job["script"], name, start, end, dry_run=dry_run, log_file=log_file)
            if success:
                chunk_str = f"{start}-{end}" if start != end else str(start)
                job.setdefault("submitted", []).append(chunk_str)
                job["job_ids"][chunk_str] = job_id
                remaining_slots -= size
            if remaining_slots <= 0:
                break

    if not dry_run:
        if data != original_data:
            with open(path, "w") as f:
                yaml.safe_dump(data, f)
            click.echo("[DONE] YAML updated.")
            log_message(log_file, "info", "YAML updated and written to disk.")
        else:
            log_message(log_file, "info", "No updates.")
    else:
        click.echo("[DRY-RUN] YAML not written to disk.")
        log_message(log_file, "info", "Dry-run: nothing written to disk.")
