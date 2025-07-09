import configparser
import logging
import subprocess
from pathlib import Path
from typing import Callable

from dagshub.auth import get_token
from dagshub.auth.token_auth import DagshubAuthenticator
from dagshub.common.api import RepoAPI
from dagshub.common.util import multi_urljoin

from dagshub_import_export.models.import_config import ImportConfig

logger = logging.getLogger(__name__)


def copy_rclone_repo_bucket(import_config: ImportConfig):
    source, destination = import_config.source_and_destination
    config_dir = import_config.directory / "rclone_bucket"
    copy_rclone(source, destination, _repo_bucket_endpoint, _repo_bucket_name, config_dir)


def copy_rclone_dvc(import_config: ImportConfig):
    source, destination = import_config.source_and_destination
    config_dir = import_config.directory / "rclone_dvc"
    copy_rclone(source, destination, _dvc_bucket_endpoint, _dvc_bucket_name, config_dir)


def copy_rclone(source: RepoAPI, destination: RepoAPI, endpoint_fn: Callable, bucket_fn: Callable, config_dir: Path):
    rclone_cfg_path = generate_rclone_config(source, destination, config_dir, endpoint_fn)

    source_address = f"source:{bucket_fn(source)}"
    destination_address = f"destination:{bucket_fn(destination)}"

    args = [
        "rclone",
        "copy",
        "--config",
        str(rclone_cfg_path),
        source_address,
        destination_address,
        "--no-update-modtime",
        "--progress",
    ]

    logger.info(f"Running rclone command: {' '.join(args)}")
    subprocess.run(args)


def generate_rclone_config(source: RepoAPI, destination: RepoAPI, cfg_dir: Path, endpoint_fn: Callable) -> Path:
    config = configparser.ConfigParser()
    conf_path = cfg_dir / "rclone.conf"

    with conf_path.open("w") as conf_file:
        # Configure source bucket
        source_endpoint = endpoint_fn(source)
        source_token = _get_token(source)
        config["source"] = _get_rclone_config(source_endpoint, source_token)

        # Configure destination bucket
        destination_endpoint = endpoint_fn(destination)
        destination_token = _get_token(destination)
        config["destination"] = _get_rclone_config(destination_endpoint, destination_token)

        config.write(conf_file)

    return conf_path


def _get_rclone_config(endpoint_url: str, token: str) -> dict:
    return {
        "type": "s3",
        "provider": "Other",
        "access_key_id": token,
        "secret_access_key": token,
        "endpoint": endpoint_url,
    }


def _repo_bucket_endpoint(repo: RepoAPI) -> str:
    return multi_urljoin(repo.host, "api/v1/repo-buckets/s3", repo.owner)


def _dvc_bucket_endpoint(repo: RepoAPI) -> str:
    return f"{repo.repo_url}.s3"


def _repo_bucket_name(repo: RepoAPI) -> str:
    return repo.repo_name


def _dvc_bucket_name(_: RepoAPI) -> str:
    return "dvc"


def _get_token(repo: RepoAPI) -> str:
    if isinstance(repo.auth, DagshubAuthenticator):
        token = repo.auth.token_text
    else:
        token = get_token(host=repo.host)
    return token
