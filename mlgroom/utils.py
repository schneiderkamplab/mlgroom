import click
from datetime import datetime
import difflib
import io
import os
from pathlib import Path
import shutil
import tempfile
import yaml

__all__ = ["chunkify", "compute_diff", "format_ranges", "log_message", "parse_ranges", "write_yaml_with_confirmation"]

def atomic_yaml_save(path: Path, data, backup_suffix=".bak"):
    path = path.resolve()
    dir_path = path.parent
    backup_path = path.with_suffix(path.suffix + backup_suffix)

    if path.exists():
        shutil.copy2(path, backup_path)

    with tempfile.NamedTemporaryFile("w", dir=dir_path, delete=False, prefix=path.name + ".", suffix=".tmp") as tmp:
        tmp_path = Path(tmp.name)
        try:
            yaml.safe_dump(data, tmp)
            tmp.flush()
            os.fsync(tmp.fileno())
        except Exception as e:
            tmp_path.unlink(missing_ok=True)
            raise e

    try:
        tmp_path.replace(path)
    except Exception as e:
        tmp_path.unlink(missing_ok=True)
        raise e

def chunkify(start, end, size):
    return [(i, min(i + size - 1, end)) for i in range(start, end + 1, size)]

def compute_diff(original_data, modified_data):
    original_yaml_lines = dump_yaml_to_str(original_data)
    modified_yaml_lines = dump_yaml_to_str(modified_data)
    diff = difflib.unified_diff(
        original_yaml_lines,
        modified_yaml_lines,
        fromfile="original",
        tofile="modified"
    )
    diff = "".join(diff)
    return diff if diff else None

def dump_yaml_to_str(obj):
    buf = io.StringIO()
    yaml.safe_dump(obj, buf)
    return buf.getvalue().splitlines(keepends=True)

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

def parse_ranges(ranges):
    result = set()
    for r in ranges or []:
        if isinstance(r, int):
            result.add(r)
        elif '-' in str(r):
            start, end = map(int, str(r).split('-'))
            result.update(range(start, end + 1))
        else:
            result.add(int(r))
    return result

def write_yaml_with_confirmation(data, original_data, path, yes=False):
    if yes:
        atomic_yaml_save(path, data)
        return True
    diff = compute_diff(original_data, data)
    if diff is not None:
        click.echo("Proposed changes (diff):")
        click.echo_via_pager(diff)
    if click.confirm("Apply changes?", default=False):
        atomic_yaml_save(path, data)
        return True
    else:
        click.echo("[ABORTED] No changes were made.")
        return False
