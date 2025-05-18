import os
import typer
import requests

API_URL = os.environ.get("BATCH_API_URL", "http://localhost:8000")

app = typer.Typer()
model_app = typer.Typer(name="model")
job_app = typer.Typer(name="job")
app.add_typer(model_app)
app.add_typer(job_app)


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
    typer.echo(r.json())


@job_app.command("create")
def job_create(model_id: str, data_file: str):
    with open(data_file, "rb") as f:
        files = {"file": (os.path.basename(data_file), f)}
        r = requests.post(f"{API_URL}/jobs", params={"model_id": model_id}, files=files)
    typer.echo(r.json())


@job_app.command("status")
def job_status(job_id: str):
    r = requests.get(f"{API_URL}/jobs/{job_id}")
    typer.echo(r.json())


@job_app.command("cancel")
def job_cancel(job_id: str):
    r = requests.delete(f"{API_URL}/jobs/{job_id}")
    typer.echo(r.json())


@job_app.command("rejected")
def job_rejected(job_id: str):
    r = requests.get(f"{API_URL}/jobs/{job_id}/rejected")
    typer.echo(r.text)


if __name__ == "__main__":
    app()
