import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

from dagshub.common.api import RepoAPI

from dagshub_import_export.util import logger_name

logger = logging.getLogger(logger_name)


@dataclass
class ImportConfig:
    source: RepoAPI
    destination: RepoAPI
    git: bool
    dvc: bool
    repo_bucket: bool
    datasources: bool
    mlflow: bool
    metadata: bool
    labelstudio: bool
    directory: Path

    @staticmethod
    def create(
        source: RepoAPI,
        destination: RepoAPI,
        git: bool,
        dvc: bool,
        repo_bucket: bool,
        datasources: bool,
        mlflow: bool,
        metadata: bool,
        labelstudio: bool,
        directory: Path,
    ) -> "ImportConfig":
        if not any([git, dvc, repo_bucket, datasources, mlflow, metadata, labelstudio]):
            git = True
            dvc = True
            repo_bucket = True
            datasources = True
            mlflow = True
            metadata = True
            labelstudio = True

        if labelstudio and not metadata:
            if not (metadata and datasources):
                logger.info("Importing Label Studio requires importing datasources and metadata, enabling them.")
                metadata = True
                datasources = True

        if mlflow:
            if not datasources:
                logger.info("Importing MLflow requires Data Engine datasources, enabling it.")
                datasources = True

        if metadata:
            if not datasources:
                logger.info("Importing Data Engine metadata requires Data Engine datasources, enabling it.")
                datasources = True

        directory = directory / source.full_name
        directory = directory.absolute()
        directory.mkdir(parents=True, exist_ok=True)

        return ImportConfig(
            source=source,
            destination=destination,
            git=git,
            dvc=dvc,
            repo_bucket=repo_bucket,
            datasources=datasources,
            metadata=metadata,
            mlflow=mlflow,
            labelstudio=labelstudio,
            directory=directory,
        )

    def __str__(self):
        return (
            f"Importing from:\n"
            f"\t{self.source.repo_url}\n"
            f"to\n"
            f"\t{self.destination.repo_url}\n"
            f"Enabled imports:\n"
            f"\tGit:           {self.git}\n"
            f"\tDVC:           {self.dvc}\n"
            f"\tRepo Bucket:   {self.repo_bucket}\n"
            f"\tDatasources:   {self.datasources}\n"
            f"\tMetadata:      {self.metadata}\n"
            f"\tMLflow:        {self.mlflow}\n"
            f"\tLabel Studio:  {self.labelstudio}\n"
            f"\nStorage directory: {self.directory}\n"
        )

    @property
    def source_and_destination(self) -> Tuple[RepoAPI, RepoAPI]:
        return self.source, self.destination
