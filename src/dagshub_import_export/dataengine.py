import logging

from dagshub.common import config
from dagshub.common.api import RepoAPI
from dagshub.data_engine import datasources, datasets
from dagshub.data_engine.model.datasource import Datasource

from dagshub_import_export.models.dataengine_mappings import DataengineMappings


logger = logging.getLogger(__name__)


def reimport_dataengine(source: RepoAPI, destination: RepoAPI) -> DataengineMappings:
    """
    reimport datasources and datasets
    """
    logger.info(f"Copying datasources and datasets from {source.repo_url} to {destination.repo_url}")
    set_dataengine_host(source)
    source_datasources = datasources.get_datasources(source.full_name)
    source_datasets = datasets.get_datasets(source.full_name)

    set_dataengine_host(destination)
    destination_existing_datasources = datasources.get_datasources(destination.full_name)
    destination_existing_datasets = datasets.get_datasets(destination.full_name)

    res = DataengineMappings(
        get_already_imported_datasources(source_datasources, destination_existing_datasources),
        get_already_imported_datasets(source_datasets, destination_existing_datasets),
    )

    for source_ds in source_datasources:
        if source_ds.source.id in res.datasources:
            logger.info(f"Datasource {source_ds.source.name} already exists in destination, skipping")
            continue
        ds_path = source_ds.source.path
        revision = None
        # TODO: also replace repo bucket ? need to check
        if ds_path.startswith("repo://"):
            ds_path = ds_path.removeprefix(f"repo://{source.full_name}")
            revision, ds_path = ds_path.split(":")
            revision = revision.removeprefix("/")
        new_ds = datasources.create_datasource(destination.full_name, source_ds.source.name, ds_path, revision=revision)
        res.datasources[source_ds.source.id] = new_ds.source.id

    for source_dataset in source_datasets:
        if source_dataset.assigned_dataset.dataset_id in res.datasets:
            logger.info(
                f"Dataset {source_dataset.assigned_dataset.dataset_name} already exists in destination, skipping"
            )
            continue
        # TODO: ordering might be important here (for versioning)
        ds_id = res.datasources[source_dataset.source.id]
        ds = datasources.get_datasource(destination.full_name, id=ds_id)
        ds._query = source_dataset.assigned_dataset.query
        with_dataset = ds.save_dataset(name=source_dataset.assigned_dataset.dataset_name)
        res.datasets[source_dataset.assigned_dataset.dataset_id] = with_dataset.assigned_dataset.dataset_id

    return res


def _ds_equals(a: Datasource, b: Datasource) -> bool:
    """
    Check if two datasources are equal based on their source path and type.
    """
    return _get_pure_ds_path(a) == _get_pure_ds_path(b) and a.source.name == b.source.name


def _get_pure_ds_path(ds: Datasource) -> str:
    """
    Get the pure path of the datasource without the repo URL.
    """
    p = ds.source.path
    if p.startswith("repo://"):
        p = p.removeprefix(f"repo://{ds.source.repoApi.full_name}")
    # TODO: check repo bucket
    return p


def _dataset_equals(a: Datasource, b: Datasource) -> bool:
    """
    Check if two datasets are equal based on their assigned dataset ID.
    """
    if a.assigned_dataset is None or b.assigned_dataset is None:
        return False
    return (
        a.assigned_dataset.dataset_name == b.assigned_dataset.dataset_name
        and a.assigned_dataset.query.to_json() == b.assigned_dataset.query.to_json()
    )


def get_already_imported_datasources(source: list[Datasource], destination: list[Datasource]) -> dict[int, int]:
    res = {}
    for source_ds in source:
        for dest_ds in destination:
            if _ds_equals(source_ds, dest_ds):
                res[source_ds.source.id] = dest_ds.source.id
                break
    return res


def get_already_imported_datasets(source: list[Datasource], destination: list[Datasource]) -> dict[int, int]:
    res = {}
    for source_ds in source:
        for dest_ds in destination:
            if _dataset_equals(source_ds, dest_ds):
                res[source_ds.assigned_dataset.dataset_id] = dest_ds.assigned_dataset.dataset_id
                break
    return res


def set_dataengine_host(repo: RepoAPI):
    config.host = repo.host
