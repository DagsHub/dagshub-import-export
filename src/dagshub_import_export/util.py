import logging
from pathlib import PurePosixPath
from urllib.parse import urlparse

import dagshub.auth
from dagshub.common.api import RepoAPI
from dagshub.common.util import multi_urljoin


def parse_repo_url(repo_url) -> RepoAPI:
    """
    Extracts the host from a given repository URL.

    Args:
        repo_url (str): The URL of the repository.

    Returns:
        str: The host part of the URL.
    """
    if not repo_url.startswith("http"):
        raise ValueError("Repository URL must start with 'http' or 'https'.")

    parsed_url = urlparse(repo_url)
    path = PurePosixPath(parsed_url.path)
    if len(path.parts) < 3:
        raise ValueError("Repository URL must be in the format of 'http(s)://host/user/repo'")
    hostname = f"{parsed_url.scheme}://{parsed_url.netloc}"
    hostname = multi_urljoin(hostname, *path.parts[:-2])
    repo = path.parts[-2] + "/" + path.parts[-1]
    repoApi = RepoAPI(host=hostname, repo=repo)
    return repoApi


def get_token(host):
    return dagshub.auth.get_token(host=host)


def init_logging():
    logging.basicConfig(level=logging.INFO)
    # Turn off noisy network-level loggers that clutter up info output
    logging.getLogger("gql.transport.requests").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
