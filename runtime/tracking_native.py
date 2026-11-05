"""Tracking MLflow por APIs Databricks já presentes; sem instalar mlflow."""
from datetime import datetime, timezone

PREFIX = "/Users/<SEU_USUARIO>/portfolio-estudos/"
HOST = "https://<SEU_WORKSPACE>.cloud.databricks.com"


def record(experiment_name, run_name, params, metrics, client=None):
    if not experiment_name.startswith(PREFIX):
        raise ValueError("Experimento fora do laboratório")
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.ml import Metric, Param, RunTag, UpdateRunStatus
    except ImportError:
        return {"status": "standby", "reason": "SDK nativo ausente; nenhuma biblioteca foi instalada"}
    stage = "workspace_client"
    try:
        client = client if client is not None else WorkspaceClient(host=HOST)
        if client.config.host.rstrip("/") != HOST:
            raise ValueError("Workspace diferente do estudo")
        stage = "experiment_lookup"
        client.workspace.mkdirs(experiment_name.rsplit("/", 1)[0])
        try:
            lookup = getattr(client.experiments, "get_by_name", None) or client.experiments.get_experiment_by_name
            experiment = lookup(experiment_name).experiment
        except Exception as error:
            if getattr(error, "error_code", None) not in {"RESOURCE_DOES_NOT_EXIST", "NOT_FOUND"}:
                raise
            experiment = None
        experiment_id = experiment.experiment_id if experiment else client.experiments.create_experiment(experiment_name).experiment_id
        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        stage = "create_run"
        response = client.experiments.create_run(experiment_id=experiment_id, run_name=run_name, start_time=now,
            tags=[RunTag(key="portfolio", value="aurora"), RunTag(key="tracking_method", value="databricks_sdk_native")])
        run_id = getattr(response.run.info, "run_id", None) or response.run.info.run_uuid
        stage = "log_batch"
        client.experiments.log_batch(run_id=run_id,
            params=[Param(key=str(key), value=str(value)) for key, value in params.items()],
            metrics=[Metric(key=str(key), value=float(value), timestamp=now, step=0) for key, value in metrics.items() if value is not None])
        stage = "finish"
        client.experiments.update_run(run_id=run_id, status=UpdateRunStatus.FINISHED, end_time=int(datetime.now(timezone.utc).timestamp() * 1000))
        return {"status": "logged", "experiment_id": experiment_id, "run_id": run_id,
                "url": f"{HOST}/?o=7474651520605774#mlflow/experiments/{experiment_id}/runs/{run_id}"}
    except Exception as error:
        # Tracking não deve liberar uma carga inválida nem forçar instalação externa.
        return {"status": "standby", "reason": "Tracking nativo indisponível", "error_type": type(error).__name__,
                "stage": stage, "missing_attribute": getattr(error, "name", None)}
