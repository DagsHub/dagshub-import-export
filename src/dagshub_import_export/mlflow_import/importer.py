import logging
import os

import mlflow
from dagshub.common.api import RepoAPI, UserAPI
from mlflow_export_import.bulk.export_all import export_all
from mlflow_export_import.bulk.import_experiments import import_experiments
from mlflow_export_import.bulk.import_models import import_models

from dagshub_import_export.util import get_token
from mlflow.tracking import MlflowClient

logger = logging.getLogger(__name__)


def reimport_mlflow(source: RepoAPI, destination: RepoAPI):
    # TODO: use a persistent dir for the import, so we don't have to redownload every time
    # temp_dir = "coco_reimport"
    # os.makedirs(temp_dir, exist_ok=True)
    with TemporaryDirectory() as temp_dir:
        _export_mlflow(source, temp_dir)
        # TODO: copy the data to another folder, massage  it to change:
        # Tags: "dagshub.datasets.dataset_id", "dagshub.datasets.datasource_id"
        # Artifacts: anything with "dagshub.dataset.json"
        _import_mlflow(destination, temp_dir)
    logger.info("Finished reimporting MLflow data")


def _export_mlflow(repo: RepoAPI, dest_dir: str):
    logger.info(f"Exporting MLflow data from {repo.repo_url} to {dest_dir}")
    _set_mlflow_auth(repo)
    client = _get_mlflow_client(repo)

    export_all(dest_dir, mlflow_client=client)


def _import_mlflow(repo: RepoAPI, source_dir: str):
    _set_mlflow_auth(repo)
    client = _get_mlflow_client(repo)

    # TODO: check if mlflow is empty. If it's not, prompt user to remove everything (rerunning will create duplicates)

    import_experiments(os.path.join(source_dir, "experiments"), mlflow_client=client)
    import_models(source_dir, mlflow_client=client, delete_model=True)


def _get_mlflow_client(repo: RepoAPI):
    return MlflowClient(tracking_uri=f"{repo.repo_url}.mlflow")


def _set_mlflow_auth(repo: RepoAPI):
    token = get_token(repo.host)
    mlflow.set_tracking_uri(f"{repo.repo_url}.mlflow")
    os.environ["MLFLOW_TRACKING_USERNAME"] = UserAPI.get_user_from_token(token, host=repo.host).username
    os.environ["MLFLOW_TRACKING_PASSWORD"] = token
    os.environ["MLFLOW_TRACKING_TOKEN"] = token
