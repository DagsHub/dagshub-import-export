import os
import subprocess

from dagshub_import_export.util import get_token
from dagshub.common.api import RepoAPI
import git


def mirror_repo(repo: git.Repo, destination_repo: RepoAPI):
    # TODO: check that destination is empty
    # repo.create_remote("destination", get_git_url(destination_repo, include_token=True))
    # repo.remote("destination").push()
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


def clone_repo(repo: RepoAPI, local_path: str) -> git.Repo:
    """
    Clone a Git repository to a local path.

    Args:
        repo_url (str): The URL of the Git repository to clone.
        local_path (str): The local path where the repository should be cloned.
    """
    url = get_git_url(repo, include_token=True)
    return git.Repo.clone_from(url, local_path)


def get_git_url(repo: RepoAPI, include_token=False) -> str:
    url = f"{repo.repo_url}.git"
    if include_token:
        token = get_token(repo.host)
        url = url.replace("://", f"://{token}:{token}@", 1)
    return url
