import pathlib
from typing import Optional

import click

import logging
from pathlib import Path
from tempfile import TemporaryDirectory

from dagshub.common.api import RepoAPI

from dagshub_import_export.checks import can_push_git, run_dataengine_checks
from dagshub_import_export.dataengine import reimport_dataengine_datasources, reimport_dataengine_metadata
from dagshub_import_export.git_module import clone_repo, mirror_repo
from dagshub_import_export.labelstudio_import.importer import reimport_labelstudio
from dagshub_import_export.mlflow_import.importer import reimport_mlflow
from dagshub_import_export.models.import_config import ImportConfig
from dagshub_import_export.rclone import copy_rclone_dvc, copy_rclone_repo_bucket
from dagshub_import_export.util import parse_repo_url, init_logging

logger = logging.getLogger(__name__)


class DagshubRepoParamType(click.ParamType):
    name = "dagshub_repo"

    def convert(self, value, param, ctx):
        try:
            return parse_repo_url(value)
        except ValueError as e:
            self.fail(f"Invalid Dagshub repository URL: {value}. Error: {e}", param, ctx)


DAGSHUB_REPO = DagshubRepoParamType()


def reimport_repo(
    source_url: str,
    destination_url: str,
    # TODO: turn these on by default
    git=False,
    dvc=False,
    repo_bucket=False,
    mlflow=False,
    metadata=False,
    datasources=False,
    labelstudio=False,
):
    # TODO: add prerequisite checks for CLI commands: git, dvc, rclone
    source_repo = parse_repo_url(source_url)
    destination_repo = parse_repo_url(destination_url)

    logger.info("Importing repository from %s to %s", source_url, destination_url)

    datasources = datasources or (mlflow or metadata or labelstudio)

    with TemporaryDirectory() as temp_dir:
        git_repo = clone_repo(source_repo, temp_dir)

        if git:
            if can_push_git(source_repo, destination_repo):
                logger.info("Mirroring Git repository")
                mirror_repo(git_repo, destination_repo)

        if dvc:
            logger.info("Copying DVC data")
            copy_rclone_dvc(source_repo, destination_repo)

        if repo_bucket:
            logger.info("Copying repository bucket data")
            copy_rclone_repo_bucket(source_repo, destination_repo)

        if datasources:
            ds_map = reimport_dataengine_datasources(source_repo, destination_repo)

        if mlflow:
            logger.info("Copying MLflow data")
            reimport_mlflow(source_repo, destination_repo, ds_map)

        if metadata:
            run_dataengine_checks(source_repo, destination_repo)
            logger.info("Copying Data Engine data")
            reimport_dataengine_metadata(source_repo, destination_repo, ds_map, Path(temp_dir))

        if labelstudio:
            logger.info("Copying Label Studio data")
            reimport_labelstudio(source_repo, destination_repo, ds_map)


def testing_mlflow():
    source_url = "https://dagshub.com/Dean/COCO_1K"
    destination_url = "http://localhost:8080/kirill/COCO_1K_Backup"
    reimport_repo(source_url, destination_url, git=False, dvc=False, repo_bucket=False, mlflow=True, metadata=False)


def testing_dataengine():
    source_url = "https://dagshub.com/Dean/COCO_1K"
    destination_url = "http://localhost:8080/kirill/COCO_1K_Backup"
    reimport_repo(source_url, destination_url, git=False, dvc=False, repo_bucket=False, mlflow=False, metadata=True)


def testing_mirror_cloning():
    source_url = "https://dagshub.com/Dean/COCO_1K"
    destination_url = "http://localhost:8080/kirill/mlflow_repo_mirror"
    reimport_repo(source_url, destination_url, git=True, dvc=False, repo_bucket=False, mlflow=False, metadata=False)


def copy_coco_1k():
    # Copies only data and repo of COCO 1k, no mlflow or data engine
    source_url = "https://dagshub.com/Dean/COCO_1K"
    destination_url = "http://localhost:8080/kirill/COCO_1K_Backup"
    reimport_repo(source_url, destination_url, git=True, dvc=True, repo_bucket=True, mlflow=False, metadata=False)


def copy_coco_1k_mlflow():
    source_url = "https://dagshub.com/Dean/COCO_1K"
    destination_url = "http://localhost:8080/kirill/COCO_1K_Backup"
    reimport_repo(source_url, destination_url, git=False, dvc=False, repo_bucket=False, mlflow=True, metadata=False)


def copy_coco_1k_test():
    source_url = "https://dagshub.com/Dean/COCO_1K"
    destination_url = "https://test.dagshub.com/kirill/COCO_1K_Backup"
    reimport_repo(source_url, destination_url, git=True, dvc=True, repo_bucket=True, mlflow=False, metadata=False)


def copy_coco_1k_test_labelstudio():
    source_url = "https://dagshub.com/Dean/COCO_1K"
    destination_url = "https://test.dagshub.com/kirill/COCO_1K_Backup"
    reimport_repo(
        source_url,
        destination_url,
        labelstudio=True,
    )


# def main() -> None:
#     # source_url = "https://dagshub.com/KBolashev/coco8-pose"
#     source_url = "https://dagshub.com/KBolashev/mlflow_repo"
#     destination_url = "http://localhost:8080/kirill/coco8-pose-backup"
#     reimport_repo(source_url, destination_url, git=False, dvc=False, repo_bucket=True, mlflow=False, metadata=False)


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
    finally:
        if tmp_dir is not None:
            tmp_dir.cleanup()

    #
    # reimport_repo(
    #     source_repo.repo_url,
    #     destination_repo.repo_url,
    #     git=True,
    #     dvc=True,
    #     repo_bucket=True,
    #     mlflow=False,
    #     metadata=False,
    # )


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
    click.echo("Hello from click")
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
