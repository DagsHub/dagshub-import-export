import logging
import os
import subprocess
from pathlib import Path

from dagshub_import_export.models.import_config import ImportConfig
from dagshub_import_export.util import get_token, logger_name
from dagshub.common.api import RepoAPI
import git

logger = logging.getLogger(logger_name)


def reimport_git_repo(import_config: ImportConfig):
    source, destination = import_config.source_and_destination

    logger.info("Cloning Git repository")
    git_dir = import_config.directory / "repo"
    git_repo = clone_repo(source, git_dir)
    mirror_repo(git_repo, destination)


def mirror_repo(repo: git.Repo, destination_repo: RepoAPI):
    cwd = os.getcwd()
    try:
        os.chdir(repo.working_dir)
        subprocess.run(
            [
                "git",
                "push",
                "--mirror",
                get_git_url(destination_repo, include_token=True),
            ],
            check=True,
        )
    finally:
        os.chdir(cwd)


def clone_repo(repo: RepoAPI, local_path: Path) -> git.Repo:
    """
    Clone a Git repository to a local path.
    """
    url = get_git_url(repo, include_token=True)
    return git.Repo.clone_from(url, local_path)


def get_git_url(repo: RepoAPI, include_token=False) -> str:
    url = f"{repo.repo_url}.git"
    if include_token:
        token = get_token(repo.host)
        url = url.replace("://", f"://{token}:{token}@", 1)
    return url
