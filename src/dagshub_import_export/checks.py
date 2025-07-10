import logging

from dagshub.common.api import RepoAPI
from dagshub.common.api.repo import BranchNotFoundError
from dagshub.common.api.responses import StorageAPIEntry
from dagshub.common.util import multi_urljoin

from dagshub_import_export.models.import_config import ImportConfig

logger = logging.getLogger(__name__)


class RepoNotReadyError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return f"RepoNotReadyError: {self.message}"


def can_push_git(source: RepoAPI, destination: RepoAPI) -> bool:
    repo_info = destination.get_repo_info()
    if repo_info.mirror:
        logger.info("Destination repo is a mirror, not pushing git")
        return False

    try:
        source_head = source.last_commit_sha()
    except BranchNotFoundError:
        logger.info("Source git repository is empty, skipping pushing git")
        return False

    try:
        destination_head = destination.last_commit_sha()
    except BranchNotFoundError:
        destination_head = None

    if destination_head is None:
        return True
    elif source_head == destination_head:
        logger.info("Destination git repository is already up to date")
        return False
    else:
        # TODO: add URL for removing/recreating
        raise RepoNotReadyError(
            f"Destination repo {destination.repo_name} is not empty, please delete it and create a new blank repo.\n"
            f"Link to settings: {multi_urljoin(destination.repo_url, 'settings')}"
        )


def run_dataengine_checks(import_config: ImportConfig):
    check_integration_parity(import_config.source, import_config.destination)


def check_integration_parity(source: RepoAPI, destination: RepoAPI):
    source_integrations = source.get_connected_storages()
    destination_integrations = destination.get_connected_storages()

    missing_integrations: list[StorageAPIEntry] = []

    for source_integration in source_integrations:
        if _is_repo_bucket(source, source_integration):
            if not any(_is_repo_bucket(destination, dest_integration) for dest_integration in destination_integrations):
                raise RepoNotReadyError(
                    f"Destination repo {destination.repo_name} does not have a repo bucket."
                    f"\nContact your administrators."
                )
        else:
            if not any(
                _is_equal_integration(source_integration, dest_integration)
                for dest_integration in destination_integrations
            ):
                missing_integrations.append(source_integration)
    if missing_integrations:
        msg = "\n".join([f"\t{i.name}" for i in missing_integrations])
        raise RepoNotReadyError(
            f"Destination repo {destination.repo_name} does not have the following integrations:"
            f"\n{msg}"
            f"\nPlease add them."
        )


def _is_repo_bucket(repo: RepoAPI, storage: StorageAPIEntry):
    return storage.name == repo.repo_name and storage.protocol == "s3"


def _is_equal_integration(source: StorageAPIEntry, destination: StorageAPIEntry) -> bool:
    return source.name == destination.name and source.protocol == destination.protocol
