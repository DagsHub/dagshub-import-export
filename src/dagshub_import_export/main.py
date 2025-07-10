import pathlib
from typing import Optional

import click

import logging
from pathlib import Path
from tempfile import TemporaryDirectory

from dagshub.common.api import RepoAPI
from dagshub.common.api.repo import RepoNotFoundError
from dagshub.common.util import multi_urljoin

from dagshub_import_export.checks import (
    run_dataengine_checks,
    can_push_git,
    RepoNotReadyError,
    print_accessing_users,
)
from dagshub_import_export.dataengine import reimport_dataengine_datasources, reimport_dataengine_metadata
from dagshub_import_export.git_module import reimport_git_repo
from dagshub_import_export.labelstudio_import.importer import reimport_labelstudio
from dagshub_import_export.mlflow_import.importer import reimport_mlflow
from dagshub_import_export.models.import_config import ImportConfig
from dagshub_import_export.rclone import copy_rclone_dvc, copy_rclone_repo_bucket
from dagshub_import_export.util import parse_repo_url, init_logging, logger_name

logger = logging.getLogger(logger_name)


class DagshubRepoParamType(click.ParamType):
    name = "dagshub_repo"

    def convert(self, value, param, ctx):
        try:
            return parse_repo_url(value)
        except ValueError as e:
            self.fail(f"Invalid Dagshub repository URL: {value}. Error: {e}", param, ctx)


DAGSHUB_REPO = DagshubRepoParamType()


def run_preflight_checks(import_config: ImportConfig) -> bool:
    source, destination = import_config.source_and_destination

    print_accessing_users(import_config)

    try:
        dest_info = destination.get_repo_info()
        can_push = dest_info.permissions.get("push", False)
        if not can_push:
            raise RepoNotReadyError("You do not have write permission for the destination repository.")

        if import_config.git:
            can_push = can_push_git(source, destination)
            import_config.git = can_push
        if import_config.datasources:
            run_dataengine_checks(import_config)
        # if import_config.mlflow:
        # mlflow_checks(import_config)
        return True
    except RepoNotFoundError:
        print(
            f"Destination repository not found!\n"
            f"Create it first by going to this link:\n"
            f"{multi_urljoin(destination.host, 'repo/create')}?preselect=none\n"
            f"Or mirror an existing repository:\n"
            f"{multi_urljoin(destination.host, 'repo/connect/any')}"
        )
        return False
    except RepoNotReadyError as e:
        print(f"Cannot import repository because a prerequisite check failed: {e.message}")
        return False


def reimport_repo(import_config: ImportConfig):
    source, destination = import_config.source_and_destination
    logger.info(f"Importing repository from {source.repo_url} to {destination.repo_url}")

    if import_config.git:
        logger.info("\n  ======  GIT  ======  \n")
        reimport_git_repo(import_config)

    if import_config.dvc:
        logger.info("\n  ======  DVC  ======  \n")
        logger.info("Copying DVC data")
        copy_rclone_dvc(import_config)

    if import_config.repo_bucket:
        logger.info("\n  ======  REPO BUCKET  ======  \n")
        logger.info("Copying repository bucket data")
        copy_rclone_repo_bucket(import_config)

    if import_config.datasources:
        logger.info("\n  ======  DATASOURCES  ======  \n")
        ds_map = reimport_dataengine_datasources(import_config)

        if import_config.metadata:
            logger.info("\n  ======  DATASOURCE METADATA  ======  \n")
            logger.info("Copying Data Engine data")
            reimport_dataengine_metadata(import_config, ds_map)

        if import_config.mlflow:
            logger.info("\n  ======  MLFLOW  ======  \n")
            logger.info("Copying MLflow data")
            reimport_mlflow(import_config, ds_map)

        if import_config.labelstudio:
            logger.info("\n  ======  LABELSTUDIO  ======  \n")
            logger.info("Copying Label Studio data")
            reimport_labelstudio(import_config, ds_map)

    logger.info("DONE IMPORTING!")


def main(
    source_repo: RepoAPI,
    destination_repo: RepoAPI,
    git: bool,
    dvc: bool,
    repo_bucket: bool,
    datasources: bool,
    metadata: bool,
    mlflow: bool,
    labelstudio: bool,
    directory: Path | None = None,
) -> None:
    init_logging()
    tmp_dir: Optional[TemporaryDirectory] = None
    try:
        if directory is None:
            tmp_dir = TemporaryDirectory()
        directory = Path(directory) if directory else Path(tmp_dir.name)

        import_config = ImportConfig.create(
            source=source_repo,
            destination=destination_repo,
            git=git,
            dvc=dvc,
            repo_bucket=repo_bucket,
            datasources=datasources,
            metadata=metadata,
            mlflow=mlflow,
            labelstudio=labelstudio,
            directory=directory,
        )
        logger.info(import_config)
        preflight_success = run_preflight_checks(import_config)
        if not preflight_success:
            return

        reimport_repo(import_config)
    finally:
        if tmp_dir is not None:
            tmp_dir.cleanup()


@click.command()
@click.argument("source_url", type=DAGSHUB_REPO)
@click.argument("destination_url", type=DAGSHUB_REPO)
@click.option(
    "--git",
    is_flag=True,
    help="Clone the full git repository to the destination. Does not do anything if the destination is a mirror",
)
@click.option("--dvc", is_flag=True, help="Copy DVC data")
@click.option("--bucket", is_flag=True, help="Copy contents of the repository bucket.")
@click.option(
    "--datasources", is_flag=True, help="Copy Data Engine datasources and datasets (does not include metadata)"
)
@click.option("--mlflow", is_flag=True, help="Copy MLflow data")
@click.option("--metadata", is_flag=True, help="Copy Data Engine metadata for all datasources")
@click.option("--labelstudio", is_flag=True, help="Copy Label Studio projects (will also copy enable --metadata)")
@click.option(
    "--directory",
    type=click.Path(path_type=pathlib.Path),
    help="Directory to store data in. "
    "If not provided, a temporary directory that is removed after execution will be used.",
)
@click.version_option()
def cli(
    source_url: RepoAPI,
    destination_url: RepoAPI,
    git: bool,
    dvc: bool,
    bucket: bool,
    datasources: bool,
    mlflow: bool,
    metadata: bool,
    labelstudio: bool,
    directory: Path | None = None,
):
    """

    SOURCE_URL and DESTINATION_URL are the URLs of the source and destination repositories.

    \b
    Example:
    dagshub-import-export https://dagshub.com/Dean/COCO_1K https://my-dagshub-deployment.com/user/COCO_1K_Backup

    By default, all imports are enabled. If you provide a flag for a specific import, then only enabled imports will be run.

    \b
    IMPORTANT: You need to make sure that the following is done, before running this command:
    - The source and destination repositories exist and are accessible from this computer,
      and you have permission to write to the destination.
    - (for datasources) The destination repository has the same connected integrations as the source repository.
    - (for MLflow) There is nothing in the destination repository's MLflow.
    """
    main(
        source_url,
        destination_url,
        git=git,
        dvc=dvc,
        repo_bucket=bucket,
        datasources=datasources,
        metadata=metadata,
        mlflow=mlflow,
        labelstudio=labelstudio,
        directory=directory,
    )


if __name__ == "__main__":
    cli()
