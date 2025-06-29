from tempfile import TemporaryDirectory

from dagshub.common.api import RepoAPI


def reimport_mlflow(source: RepoAPI, destination: RepoAPI):
    # TODO: use a persistent dir for the import, so we don't have to redownload every time
    with TemporaryDirectory() as temp_dir:
        pass


def _get_mlflow_client(repo: RepoAPI):
    from mlflow.tracking import MlflowClient

    return MlflowClient(
        tracking_uri=repo.repo_url)
