import logging
import os
from pathlib import PurePosixPath
from urllib.parse import urlparse

import dagshub.auth
import tenacity
import urllib3
from dagshub.common.api import RepoAPI
from dagshub.common.util import multi_urljoin
from tenacity import before_sleep_log

logger_name = "dagshub_import_export"

logger = logging.getLogger(logger_name)

retry_5_times = tenacity.retry(
    stop=tenacity.stop_after_attempt(5), reraise=True, before_sleep=before_sleep_log(logger, logging.INFO)
)


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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    # Turn off noisy network-level loggers that clutter up info output
    logging.getLogger("gql.transport.requests").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    urllib3.disable_warnings()
    # Also tune the connection pool size for MLflow, so it doesn't spam urllib3 warnings
    os.environ["MLFLOW_HTTP_POOL_MAXSIZE"] = "50"
