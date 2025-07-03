import click
import copy
from pathlib import Path
import re
import subprocess
import yaml

from .utils import *

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

def get_completed_failed_job_ids():
    cmd = ["sacct", "--format=JobID,State", "--parsable2", "--noheader", "--starttime=now-7days"]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, text=True)
    completed = {}
    failed = {}
    for line in result.stdout.strip().split("\n"):
        jobid, state = line.strip().split("|")
        if jobid.endswith(".batch") and state == "COMPLETED":
            completed[jobid] = state
        if jobid.endswith(".batch") and state in {"FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY"}:
            failed[jobid] = state
    return completed, failed

def update_completed_failed_jobs(jobs, completed_jobs, failed_jobs, log_file=None, cleanup_job_ids=False):
    for job in jobs:
        new_completed = set()
        new_failed = set()
        job_ids = job.get("job_ids", {})
        for chunk, jid in job_ids.items():
            task_ids = []
            if "-" in chunk:
                start, end = map(int, chunk.split("-"))
                task_ids = range(start, end + 1)
            else:
                task_ids = [int(chunk)]
            for task_id in task_ids:
                step_id = f"{jid}_{task_id}.batch"
                if step_id in completed_jobs:
                    new_completed.add(task_id)
                if step_id in failed_jobs:
                    new_failed.add(task_id)
        current_completed = parse_ranges(job.get("completed", []))
        current_failed = parse_ranges(job.get("failed", []))
        updated_completed = sorted(current_completed.union(new_completed))
        updated_failed = sorted(current_failed.union(new_failed))
        if new_completed:
            log_message(log_file, "info", f"Marked chunks as completed for {job['name']}: {format_ranges(new_completed)}")
        if new_failed:
            log_message(log_file, "info", f"Marked chunks as failed for {job['name']}: {format_ranges(new_failed)}")
        job["completed"] = format_ranges(updated_completed)
        job["failed"] = format_ranges(updated_failed)
        if cleanup_job_ids:
            completed_set = set(updated_completed)
            to_delete = []
            for chunk in job_ids:
                if "-" in chunk:
                    start, end = map(int, chunk.split("-"))
                    task_ids = set(range(start, end + 1))
                else:
                    task_ids = {int(chunk)}
                if task_ids.issubset(completed_set):
                    to_delete.append(chunk)
            for chunk in to_delete:
                del job_ids[chunk]
                log_message(log_file, "info", f"Removed completed job ID mapping for chunk {chunk} in job {job['name']}")

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

def chunk_task_ids(task_ids, chunk_size):
    if not task_ids:
        return []
    task_ids = sorted(task_ids)
    chunks = []
    current_chunk = [task_ids[0]]

    for tid in task_ids[1:]:
        if len(current_chunk) < chunk_size and tid == current_chunk[-1] + 1:
            current_chunk.append(tid)
        else:
            chunks.append((current_chunk[0], current_chunk[-1]))
            current_chunk = [tid]
    chunks.append((current_chunk[0], current_chunk[-1]))
    return chunks

def split_chunk_on_failures(start, end, failed_set):
    remaining = [i for i in range(start, end + 1) if i not in failed_set]
    if not remaining:
        return []
    result = []
    run_start = remaining[0]
    prev = remaining[0]
    for i in remaining[1:]:
        if i == prev + 1:
            prev = i
        else:
            result.append((run_start, prev))
            run_start = i
            prev = i
    result.append((run_start, prev))
    return [f"{s}-{e}" if s != e else str(s) for s, e in result]

@click.group()
def cli():
    pass

@cli.command()
@click.option("--queue-file", default="groom.yml", type=click.Path(), help="Queue file to groom (default: groom.yml)")
@click.option("--user", default=None, help="Username for SLURM squeue/sacct commands (default: current user)")
@click.option("--submit", is_flag=True, help="Submit jobs to SLURM (default: False)")
@click.option("--log-file", default="groom.log", type=click.Path(), help="Log file to append job activity to (default: groom.log)")
@click.option("--cleanup-job-ids", is_flag=True, help="Remove job IDs for completed jobs from the YAML file (default: False)")
@click.option("--chunk-size", type=int, default=None, help="Chunk size for array jobs (default: read from YAML)")
@click.option("--max-jobs", type=int, default=None, help="Max total running jobs (default: read from YAML)")
def groom(queue_file, user, submit, log_file, cleanup_job_ids, chunk_size, max_jobs):
    path = Path(queue_file)
    if path.exists():
        with open(path) as f:
            data = yaml.safe_load(f)
        original_data = copy.deepcopy(data)
    else:
        click.echo("[ERROR] Queue file does not exist.")
        return
    user = user or subprocess.getoutput("whoami")
    chunk_size = data["chunk_size"] if chunk_size is None else chunk_size
    max_jobs = data["max_jobs"] if max_jobs is None else max_jobs
    current_jobs = get_total_jobs(user)
    remaining_slots = max_jobs - current_jobs
    if remaining_slots <= 0:
        msg = f"No slots available ({current_jobs}/{max_jobs} jobs running)."
        click.echo(f"[INFO] {msg}")
        log_message(log_file, "info", msg)
        return
    completed_ids, failed_ids = get_completed_failed_job_ids()
    update_completed_failed_jobs(data["jobs"], completed_ids, failed_ids, log_file=log_file, cleanup_job_ids=cleanup_job_ids)
    for job in data["jobs"]:
        name = job["name"]
        task_ids = sorted(parse_ranges(job.get("range", [])))
        if not task_ids:
            click.echo(f"[WARN] Skipping job '{name}' due to empty range.")
            continue
        submitted = parse_ranges(job.get("submitted", []))
        completed = parse_ranges(job.get("completed", []))
        failed = parse_ranges(job.get("failed", []))
        job.setdefault("job_ids", {})
        handled = submitted.union(completed).union(failed)
        remaining = sorted(set(task_ids) - handled)
        available_chunks = chunk_task_ids(remaining, chunk_size)
        for start, end in available_chunks:
            size = end - start + 1
            if size > remaining_slots:
                continue
            success, job_id = submit_array(job["script"], name, start, end, dry_run=not submit, log_file=log_file)
            if success:
                chunk_str = f"{start}-{end}" if start != end else str(start)
                job.setdefault("submitted", []).append(chunk_str)
                job["job_ids"][chunk_str] = job_id
                remaining_slots -= size
            if remaining_slots <= 0:
                break
        submitted = parse_ranges(job.get("submitted", []))
        job["submitted"] = format_ranges(submitted)
    diff = compute_diff(original_data, data)
    if submit:
        if data != original_data:
            with open(path, "w") as f:
                yaml.safe_dump(data, f)
            click.echo("[DONE] YAML updated.")
            click.echo(diff)
            log_message(log_file, "info", "YAML updated and written to disk.")
        else:
            click.echo("[OK] No changes to write.")
            log_message(log_file, "info", "No updates.")
    else:
        click.echo("[DRY-RUN] No jobs submitted and YAML not written to disk.")
        if diff is None:
            click.echo("[OK] No changes to YAML.")
            log_message(log_file, "info", "No changes to YAML.")
        else:
            click.echo_via_pager(diff)
        log_message(log_file, "info", "Dry-run: nothing submitted and nothing written to disk.")

@cli.command()
@click.option("--job", default=None, help="Job name to resubmit (default: all jobs with failed tasks)")
@click.option("--queue-file", default="groom.yml", type=click.Path(), help="Queue file to modify (default: groom.yml)")
@click.option("--yes", is_flag=True, help="Automatically confirm changes without prompting (default: False)")
@click.option("--log-file", default="groom.log", type=click.Path(), help="Log file to write to (default: groom.log)")
@click.option("--max-resubmissions", default=3, type=int, help="Maximum number of resubmissions per task (0 = unlimited)")
def resubmit(job, queue_file, log_file, yes, max_resubmissions):
    path = Path(queue_file)
    if not path.exists():
        click.echo("[ERROR] Queue file does not exist.")
        return
    with open(path) as f:
        data = yaml.safe_load(f)
    original_data = copy.deepcopy(data)
    jobs = data.get("jobs", [])
    if job:
        jobs = [j for j in jobs if j["name"] == job]
        if not jobs:
            click.echo(f"[ERROR] Job '{job}' not found in queue.")
            return
    modified = False
    for j in jobs:
        name = j["name"]
        failed = sorted(parse_ranges(j.get("failed", [])))
        if not failed:
            click.echo(f"[INFO] No failed tasks to clean for job '{name}'.")
            continue
        job_ids = j.get("job_ids", {})
        submitted = parse_ranges(j.get("submitted", []))
        resubmit_counts = j.get("resubmit_counts", {}).copy()
        updated_job_ids = {}
        updated_submitted = set(submitted)
        contaminated_chunks = set()
        chunk_to_id = {}
        for chunk_str, jid in job_ids.items():
            if "-" in chunk_str:
                s, e = map(int, chunk_str.split("-"))
            else:
                s = e = int(chunk_str)
            chunk_to_id[(s, e)] = jid
        eligible_failed = []
        for task_id in failed:
            count = resubmit_counts.get(str(task_id), 0)
            if max_resubmissions == 0 or count < max_resubmissions:
                eligible_failed.append(task_id)
            else:
                log_message(log_file, "warning", f"[SKIPPED] {name}: task {task_id} exceeded max resubmissions ({count} ≥ {max_resubmissions})")
        if not eligible_failed:
            click.echo(f"[INFO] No eligible failed tasks to resubmit for job '{name}'.")
            continue
        for task_id in eligible_failed:
            for (s, e), jid in chunk_to_id.items():
                if s <= task_id <= e:
                    contaminated_chunks.add((s, e))
                    resubmit_counts[str(task_id)] = resubmit_counts.get(str(task_id), 0) + 1
                    log_message(log_file, "info", f"[CLEANUP] {name}: task {task_id} failed in chunk {s}-{e}")
                    break
        for (s, e), jid in chunk_to_id.items():
            if (s, e) not in contaminated_chunks:
                updated_job_ids[format_range(s, e)] = jid
                continue
            failed_in_chunk = {t for t in eligible_failed if s <= t <= e}
            survivors = [t for t in range(s, e + 1) if t not in failed_in_chunk]
            if not survivors:
                continue
            new_ranges = split_into_ranges(survivors)
            for (ns, ne) in new_ranges:
                chunk_key = format_range(ns, ne)
                updated_job_ids[chunk_key] = jid
                updated_submitted.update(range(ns, ne + 1))
        j["job_ids"] = updated_job_ids
        original_submitted = parse_ranges(j.get("submitted", []))
        clean_tasks = sorted(set(original_submitted) - set(eligible_failed))
        j["submitted"] = [
            f"{s}-{e}" if s != e else str(s)
            for (s, e) in split_into_ranges(clean_tasks)
        ]
        j["resubmit_counts"] = resubmit_counts
        j["failed"] = []
        modified = True
        click.echo(f"[OK] {name}: cleaned eligible failed tasks, rebuilt job_ids and submitted.")
    if modified:
        if write_yaml_with_confirmation(data, original_data, path, yes=yes):
            click.echo("[DONE] Resubmission cleanup completed.")
            log_message(log_file, "info", "Resubmission cleanup completed and written to disk.")
        else:
            click.echo("[ABORTED] No changes were made.")
            log_message(log_file, "info", "Resubmission cleanup aborted, no changes made.")
    else:
        click.echo("[OK] No changes made to queue.")
        log_message(log_file, "info", "No changes made to queue.")

@cli.command()
@click.argument("job_definition")
@click.option("--queue-file", default="groom.yml", type=click.Path(), help="Queue file to write to (default: groom.yml)")
@click.option("--chunk-size", type=int, default=10, help="Chunk size for array jobs (default: 10)")
@click.option("--max-jobs", type=int, default=200, help="Max total running jobs (default: 200)")
@click.option("--log-file", default="groom.log", type=click.Path(), help="Log file to write to (default: groom.log)")
@click.option("--yes", is_flag=True, help="Automatically confirm changes without prompting (default: False)")
def append(job_definition, queue_file, chunk_size, max_jobs, log_file, yes):
    path = Path(queue_file)
    if path.exists():
        with open(path) as f:
            data = yaml.safe_load(f)
    else:
        data = {"jobs": []}
    original_data = copy.deepcopy(data)
    parts = job_definition.split(":")
    if len(parts) != 2:
        raise click.UsageError("append must be in the format 'script:range'")
    script_path = Path(parts[0])
    if not script_path.exists():
        raise click.BadParameter(f"Script file does not exist: {script_path}")
    name = script_path.stem
    range_input = parts[1].split(",")
    range_parsed = sorted(parse_ranges(range_input))
    if not range_parsed:
        raise click.BadParameter("Empty or invalid task range.")
    job_entry = {
        "name": name,
        "script": str(script_path),
        "range": format_ranges(range_parsed),
        "submitted": [],
        "completed": [],
        "failed": [],
        "job_ids": {},
    }
    data.setdefault("jobs", []).append(job_entry)
    data.setdefault("chunk_size", chunk_size)
    data.setdefault("max_jobs", max_jobs)
    if write_yaml_with_confirmation(data, original_data, path, yes=yes):
        click.echo(f"[OK] Appended job '{name}' with tasks: {format_ranges(range_parsed)}")
        log_message(log_file, "info", f"Appended job '{name}' with tasks: {format_ranges(range_parsed)}")
    else:
        click.echo("[ABORTED] No changes were made.")
        log_message(log_file, "info", "Append operation aborted, no changes made.")

@cli.command()
@click.option("--job", default=None, help="Job name to show status for (default: all jobs)")
@click.option("--fields", default="name,range,submitted,completed,failed,job_ids", help="Comma-separated fields to display (default: name,range,submitted,completed,failed,job_ids)")
@click.option("--queue-file", default="groom.yml", type=click.Path(), help="Queue file to read from (default: groom.yml)")
def status(queue_file, job, fields, log_file):
    path = Path(queue_file)
    if not path.exists():
        click.echo("[ERROR] Queue file does not exist.")
        return
    with open(path) as f:
        data = yaml.safe_load(f)
    jobs = data.get("jobs", [])
    if not jobs:
        click.echo("[INFO] No jobs found in queue.")
        return
    fields = set(fields.split(","))
    if job:
        jobs = [j for j in jobs if j["name"] == job]
        if not jobs:
            click.echo(f"[ERROR] Job '{job}' not found in queue.")
            return
    for j in jobs:
        output = []
        if "name" in fields:
            output.append(f"Name: {j['name']}")
        if "range" in fields:
            output.append(f"Range: {format_ranges(parse_ranges(j.get('range', [])))}")
        if "submitted" in fields:
            output.append(f"Submitted: {format_ranges(parse_ranges(j.get('submitted', [])))}")
        if "completed" in fields:
            output.append(f"Completed: {format_ranges(parse_ranges(j.get('completed', [])))}")
        if "failed" in fields:
            output.append(f"Failed: {format_ranges(parse_ranges(j.get('failed', [])))}")
        if "job_ids" in fields:
            job_ids = j.get("job_ids", {})
            output.append(f"Job IDs: {', '.join([f'{k}: {v}' for k, v in job_ids.items()])}")
        click.echo("\n".join(output))
        click.echo("-" * 40)
