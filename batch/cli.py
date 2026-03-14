import json
import os
from typing import List, Dict, Any

import requests
import typer

API_URL = os.environ.get("BATCH_API_URL", "http://localhost:8000")
API_TIMEOUT_SECONDS = float(os.environ.get("BATCH_API_TIMEOUT_SECONDS", "30"))

app = typer.Typer()
model_app = typer.Typer(name="model")
job_app = typer.Typer(name="job")
app.add_typer(model_app)
app.add_typer(job_app)


def _request(method: str, path: str, **kwargs) -> requests.Response:
    kwargs.setdefault("timeout", API_TIMEOUT_SECONDS)
    try:
        return requests.request(method, f"{API_URL}{path}", **kwargs)
    except requests.RequestException as exc:
        typer.echo(f"request failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc


def _echo_json(payload: Any) -> None:
    typer.echo(json.dumps(payload))


def _error_text(response: requests.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        payload = None

    if isinstance(payload, dict) and payload.get("detail"):
        return str(payload["detail"])
    if response.text.strip():
        return response.text.strip()
    return f"request failed with status {response.status_code}"


def _exit_for_response(response: requests.Response) -> None:
    typer.echo(_error_text(response), err=True)
    raise typer.Exit(code=1)


def _ms_to_time(ms: int) -> str:
    """Convert milliseconds to MM:SS string."""
    seconds = int(ms // 1000)
    return f"{seconds // 60:02d}:{seconds % 60:02d}"


def _progress_bar(ok: int, total: int, state: str, width: int = 17) -> str:
    if total <= 0:
        filled = 0
    else:
        fraction = max(0.0, min(ok / total, 1.0))
        filled = int(fraction * width)
    remainder = width - filled
    if total <= 0:
        bar = "-" * width
    elif state in {"FAILED", "CANCELLED"}:
        bar = "#" * filled + "X" * remainder
    else:
        bar = "#" * filled + "-" * remainder
    return f"[{bar}]"


def _format_jobs(jobs: List[Dict[str, Any]]) -> str:
    header = "JOB      MODEL       STATE           TOTAL   OK      ERRORS  PROGRESS      WAITING  PROCESSING"
    lines = [header]
    for job in jobs:
        total = job.get("totals", {}).get("rows", 0)
        ok = job.get("totals", {}).get("ok", 0)
        errors = job.get("totals", {}).get("errors", 0)
        waiting = job.get("timings", {}).get("waiting_ms", 0)
        processing = job.get("timings", {}).get("processing_ms", 0)

        bar = _progress_bar(ok, total, job.get("state", ""))
        if total:
            percent = int(min(max((ok / total) * 100, 0), 100))
        else:
            percent = 0

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
    response = _request("GET", "/models")
    if response.status_code != 200:
        _exit_for_response(response)
    _echo_json(response.json())


@model_app.command("describe")
def model_describe(model_id: str):
    response = _request("GET", f"/models/{model_id}")
    if response.status_code != 200:
        _exit_for_response(response)
    _echo_json(response.json())


@model_app.command("create")
def model_create(name: str, schema: str):
    with open(schema, encoding="utf-8") as f:
        schema_data = f.read()
    response = _request("POST", "/models", json={"name": name, "schema": schema_data})
    if response.status_code != 201:
        _exit_for_response(response)
    _echo_json(response.json())


@model_app.command("update")
def model_update(model_id: str, schema: str):
    with open(schema, encoding="utf-8") as f:
        schema_data = f.read()
    response = _request("PUT", f"/models/{model_id}", json={"schema": schema_data})
    if response.status_code != 200:
        _exit_for_response(response)
    _echo_json(response.json())


@model_app.command("delete")
def model_delete(model_id: str):
    response = _request("DELETE", f"/models/{model_id}")
    if response.status_code != 204:
        _exit_for_response(response)
    typer.echo("204")


@job_app.command("list")
def job_list():
    response = _request("GET", "/jobs")
    if response.status_code != 200:
        _exit_for_response(response)
    typer.echo(_format_jobs(response.json()))


@job_app.command("create")
def job_create(model_id: str, data_file: str):
    with open(data_file, "rb") as f:
        files = {"file": (os.path.basename(data_file), f)}
        response = _request("POST", "/jobs", params={"model_id": model_id}, files=files)
    if response.status_code != 202:
        _exit_for_response(response)
    typer.echo(f"job {response.json().get('job_id')} created.")


@job_app.command("status")
def job_status(job_id: str):
    response = _request("GET", f"/jobs/{job_id}")
    if response.status_code != 200:
        _exit_for_response(response)
    typer.echo(_format_jobs([response.json()]))


@job_app.command("cancel")
def job_cancel(job_id: str):
    response = _request("DELETE", f"/jobs/{job_id}")
    if response.status_code != 202:
        _exit_for_response(response)
    typer.echo(f"job {job_id} cancelled")


@job_app.command("rejected")
def job_rejected(job_id: str):
    response = _request("GET", f"/jobs/{job_id}/rejected")
    if response.status_code != 200:
        _exit_for_response(response)
    typer.echo(response.text)
