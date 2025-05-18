import os
from typing import List, Dict, Any

import typer
import requests

API_URL = os.environ.get("BATCH_API_URL", "http://localhost:8000")

app = typer.Typer()
model_app = typer.Typer(name="model")
job_app = typer.Typer(name="job")
app.add_typer(model_app)
app.add_typer(job_app)


def _ms_to_time(ms: int) -> str:
    """Convert milliseconds to MM:SS string."""
    seconds = int(ms // 1000)
    return f"{seconds // 60:02d}:{seconds % 60:02d}"


def _progress_bar(ok: int, total: int, state: str, width: int = 17) -> str:
    if total <= 0:
        bar = "-" * width
    else:
        filled = int((ok / total) * width)
        remainder = width - filled
        if state in {"FAILED", "CANCELLED"}:
            bar = "#" * filled + "X" * remainder
        else:
            bar = "#" * filled + "-" * remainder
    return f"[{bar}]"


def _format_jobs(jobs: List[Dict[str, Any]]) -> str:
    header = (
        "JOB      MODEL       STATE           TOTAL   OK      ERRORS  PROGRESS      WAITING  PROCESSSING"
    )
    lines = [header]
    for job in jobs:
        total = job.get("totals", {}).get("rows", 0)
        ok = job.get("totals", {}).get("ok", 0)
        errors = job.get("totals", {}).get("errors", 0)
        waiting = job.get("timings", {}).get("waiting_ms", 0)
        processing = job.get("timings", {}).get("processing_ms", 0)

        bar = _progress_bar(ok, total, job.get("state", ""))
        percent = int((ok / total) * 100) if total else 0

        model = job.get("model_id", "")
        if len(model) > 10:
            model = model[:8] + ".."

        line = (
            f"{job.get('id', ''):<8} {model:<11} {job.get('state', ''):<15}"
            f" {total:>7,} {ok:>7,} {errors:>7,} {bar} {percent:>3}%   "
            f"{_ms_to_time(waiting):>5}        {_ms_to_time(processing):>5}"
        )
        lines.append(line)
    return "\n".join(lines)


@model_app.command("list")
def model_list():
    r = requests.get(f"{API_URL}/models")
    typer.echo(r.json())


@model_app.command("describe")
def model_describe(model_id: str):
    r = requests.get(f"{API_URL}/models/{model_id}")
    typer.echo(r.json())


@model_app.command("create")
def model_create(name: str, schema: str):
    with open(schema) as f:
        schema_data = f.read()
    r = requests.post(f"{API_URL}/models", json={"name": name, "schema": schema_data})
    typer.echo(r.json())


@model_app.command("update")
def model_update(model_id: str, schema: str):
    with open(schema) as f:
        schema_data = f.read()
    r = requests.put(f"{API_URL}/models/{model_id}", json={"schema": schema_data})
    typer.echo(r.json())


@model_app.command("delete")
def model_delete(model_id: str):
    r = requests.delete(f"{API_URL}/models/{model_id}")
    typer.echo(r.status_code)


@job_app.command("list")
def job_list():
    r = requests.get(f"{API_URL}/jobs")
    typer.echo(_format_jobs(r.json()))


@job_app.command("create")
def job_create(model_id: str, data_file: str):
    with open(data_file, "rb") as f:
        files = {"file": (os.path.basename(data_file), f)}
        r = requests.post(f"{API_URL}/jobs", params={"model_id": model_id}, files=files)
    if r.status_code == 202:
        typer.echo(f"job {r.json().get('job_id')} created.")
    else:
        typer.echo(r.text)


@job_app.command("status")
def job_status(job_id: str):
    r = requests.get(f"{API_URL}/jobs/{job_id}")
    if r.status_code == 200:
        typer.echo(_format_jobs([r.json()]))
    else:
        typer.echo(r.text)


@job_app.command("cancel")
def job_cancel(job_id: str):
    r = requests.delete(f"{API_URL}/jobs/{job_id}")
    if r.status_code == 202:
        typer.echo(f"job {job_id} cancelled")
    else:
        typer.echo(r.text)


@job_app.command("rejected")
def job_rejected(job_id: str):
    r = requests.get(f"{API_URL}/jobs/{job_id}/rejected")
    typer.echo(r.text)


if __name__ == "__main__":
    app()
