import logging
from pathlib import Path
from tempfile import TemporaryDirectory

from dagshub_import_export.dataengine import reimport_dataengine_datasources, reimport_dataengine_metadata
from dagshub_import_export.git_module import clone_repo, mirror_repo
from dagshub_import_export.mlflow_import.importer import reimport_mlflow
from dagshub_import_export.rclone import copy_rclone_dvc, copy_rclone_repo_bucket
from dagshub_import_export.util import parse_repo_url, init_logging

logger = logging.getLogger(__name__)


def reimport_repo(
    source_url: str,
    destination_url: str,
    git=True,
    dvc=True,
    repo_bucket=True,
    mlflow=True,
    data_engine=True,
):
    # TODO: add prerequisite checks for CLI commands: git, dvc, rclone
    source_repo = parse_repo_url(source_url).repoApi
    destination_repo = parse_repo_url(destination_url).repoApi

    logger.info("Importing repository from %s to %s", source_url, destination_url)

    with TemporaryDirectory() as temp_dir:
        git_repo = clone_repo(source_repo, temp_dir)

        if git:
            logger.info("Mirroring Git repository")
            mirror_repo(git_repo, destination_repo)

        if dvc:
            logger.info("Copying DVC data")
            copy_rclone_dvc(source_repo, destination_repo)

        if repo_bucket:
            logger.info("Copying repository bucket data")
            copy_rclone_repo_bucket(source_repo, destination_repo)

        if mlflow:
            logger.info("Copying MLflow data")
            reimport_mlflow(source_repo, destination_repo)

        if data_engine:
            logger.info("Copying Data Engine data")
            ds_map = reimport_dataengine_datasources(source_repo, destination_repo)
            reimport_dataengine_metadata(source_repo, destination_repo, ds_map, Path(temp_dir))
            # print(ds_map)


def testing_mlflow():
    source_url = "https://dagshub.com/Dean/COCO_1K"
    destination_url = "http://localhost:8080/kirill/COCO_1K_Backup"
    reimport_repo(source_url, destination_url, git=False, dvc=False, repo_bucket=False, mlflow=True, data_engine=False)


def testing_dataengine():
    source_url = "https://dagshub.com/Dean/COCO_1K"
    destination_url = "http://localhost:8080/kirill/COCO_1K_Backup"
    reimport_repo(source_url, destination_url, git=False, dvc=False, repo_bucket=False, mlflow=False, data_engine=True)


def main() -> None:
    # source_url = "https://dagshub.com/KBolashev/coco8-pose"
    source_url = "https://dagshub.com/KBolashev/mlflow_repo"
    destination_url = "http://localhost:8080/kirill/coco8-pose-backup"
    reimport_repo(source_url, destination_url, git=False, dvc=False, repo_bucket=True, mlflow=False, data_engine=False)


if __name__ == "__main__":
    init_logging()
    # main()
    # testing_mlflow()
    testing_dataengine()
