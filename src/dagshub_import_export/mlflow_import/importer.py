import datetime
import json
import logging
import os
import shutil
from tempfile import TemporaryDirectory

import mlflow
from dagshub.common.api import RepoAPI, UserAPI
from dagshub.data_engine.model.datasource import Datasource
from mlflow_export_import.bulk.export_all import export_all
from mlflow_export_import.bulk.import_experiments import import_experiments
from mlflow_export_import.bulk.import_models import import_models

from dagshub_import_export.dataengine import set_dataengine_host, get_dataset, get_datasource
from dagshub_import_export.models.dataengine_mappings import DataengineMappings
from dagshub_import_export.util import get_token
from mlflow.tracking import MlflowClient

logger = logging.getLogger(__name__)


def reimport_mlflow(source: RepoAPI, destination: RepoAPI, ds_map: DataengineMappings):
    with TemporaryDirectory() as temp_dir:
        _export_mlflow(source, temp_dir)
        processed_mlflow_dir = None
        try:
            processed_mlflow_dir = change_dataengine_ids(source, destination, temp_dir, ds_map)
            _import_mlflow(destination, processed_mlflow_dir)
        finally:
            if processed_mlflow_dir is not None:
                shutil.rmtree(processed_mlflow_dir)
    logger.info("Finished reimporting MLflow data")


def _export_mlflow(repo: RepoAPI, dest_dir: str):
    logger.info(f"Exporting MLflow data from {repo.repo_url} to {dest_dir}")
    _set_mlflow_auth(repo)
    client = _get_mlflow_client(repo)

    export_all(dest_dir, mlflow_client=client)


def change_dataengine_ids(source: RepoAPI, destination: RepoAPI, source_dir: str, ds_map: DataengineMappings) -> str:
    """
    Makes a copy of the source_dir and changes data engine related metadata to have correct IDs
    This includes:
        Tags: "dagshub.datasets.dataset_id", "dagshub.datasets.datasource_id"
        Artifacts: anything with "dagshub.dataset.json"

    Any file that is not changed is symlinked

    Returns a path to the modified directory
    """

    dest_dir = source_dir + "_modified"
    os.makedirs(dest_dir, exist_ok=True)

    for root, dirs, files in os.walk(source_dir):
        for dirname in dirs:
            dir_path = os.path.join(root, dirname)
            dest_path = dir_path.replace(source_dir, dest_dir, 1)
            os.makedirs(dest_path, exist_ok=True)

        for file in files:
            file_path = os.path.join(root, file)
            dest_file_path = file_path.replace(
                source_dir, dest_dir, 1
            )  # Replace the first occurrence of source_dir with dest_dir
            if not _mlflow_file_needs_change(file):
                try:
                    os.symlink(os.path.abspath(file_path), dest_file_path)
                except FileExistsError:
                    pass
            else:
                _change_mlflow_file(file_path, dest_file_path, source, destination, ds_map)

    return dest_dir


def _mlflow_file_needs_change(filename: str) -> bool:
    return filename.endswith(".dagshub.dataset.json") or filename == "run.json"


def _change_mlflow_file(
    file_path: str, dest_file_path: str, source: RepoAPI, destination: RepoAPI, ds_map: DataengineMappings
):
    file_content = open(file_path, "rb").read()
    if file_path.endswith(".dagshub.dataset.json"):
        new_file_content = _process_datasource_json(file_content, source, destination, ds_map)
    elif file_path.endswith("run.json"):
        new_file_content = _process_mlflow_run_json(file_content, ds_map)
    else:
        raise RuntimeError(f"Don't know how to process mlflow file {file_path}")
    with open(dest_file_path, "wb") as f:
        f.write(new_file_content)


def _process_mlflow_run_json(content: bytes, ds_map: DataengineMappings) -> bytes:
    json_content = json.loads(content)
    tags = json_content.get("mlflow", {}).get("tags", {})
    if "dagshub.datasets.dataset_id" in tags:
        dataset_id = tags["dagshub.datasets.dataset_id"]
        if dataset_id in ds_map.datasets:
            tags["dagshub.datasets.dataset_id"] = ds_map.datasets[dataset_id]
    if "dagshub.datasets.datasource_id" in tags:
        datasource_id = tags["dagshub.datasets.datasource_id"]
        if datasource_id in ds_map.datasources:
            tags["dagshub.datasets.datasource_id"] = ds_map.datasources[datasource_id]
    return json.dumps(json_content, indent=2).encode("utf-8")


def _process_datasource_json(
    content: bytes, source: RepoAPI, destination: RepoAPI, ds_map: DataengineMappings
) -> bytes:
    json_content = json.loads(content)
    set_dataengine_host(source)
    source_ds = Datasource.load_from_serialized_state(json_content)

    # Load the corresponding datasource/dataset, and apply the query from the deserialized source one
    # Then after that serialize that datasource back.
    if "dataset_id" in json_content and json_content["dataset_id"] is not None:
        ds_id = json_content["dataset_id"]
        dest_ds = get_dataset(destination.host, destination.full_name, ds_map.datasets[ds_id])
    else:
        ds_id = json_content["datasource_id"]
        dest_ds = get_datasource(destination.host, destination.full_name, ds_map.datasets[ds_id])

    dest_ds._query = source_ds._query
    # set the as of to the current time, because we don't have historical time
    dest_ds._query.as_of = None
    # noinspection PyProtectedMember
    serialized = dest_ds._to_dict(datetime.datetime.now())
    return json.dumps(serialized, indent=2).encode("utf-8")


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
