import logging
from tempfile import TemporaryDirectory

from dagshub_import_export.git_module import clone_repo, mirror_repo
from dagshub_import_export.util import parse_repo_url

logger = logging.getLogger(__name__)


def reimport_repo(source_url: str, destination_url: str, git=True):
    # TODO: add prerequisite checks for CLI commands: git, dvc, rclone
    source_repo = parse_repo_url(source_url)
    destination_repo = parse_repo_url(destination_url)

    with TemporaryDirectory() as temp_dir:
        git_repo = clone_repo(source_repo.repoApi, temp_dir)

        if git:
            logger.info(
                "Mirroring Git repository from %s to %s",
                source_url,
                destination_url,
            )
            mirror_repo(git_repo, destination_repo.repoApi)


def main() -> None:
    source_url = "https://dagshub.com/KBolashev/coco8-pose"
    destination_url = "http://localhost:8080/kirill/coco8-pose-backup"
    reimport_repo(source_url, destination_url, git=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
