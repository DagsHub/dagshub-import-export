import logging
from functools import lru_cache
from pathlib import Path

import dagshub.common.config
import pandas as pd
from dagshub.common import config
from dagshub.common.api import RepoAPI
from dagshub.data_engine import datasources, datasets
from dagshub.data_engine.annotation import MetadataAnnotations
from dagshub.data_engine.model.datasource import Datasource
from dagshub.data_engine.model.schema_util import metadata_type_lookup_reverse

from dagshub_import_export.models.dataengine_mappings import DataengineMappings


logger = logging.getLogger(__name__)


def reimport_dataengine_datasources(source: RepoAPI, destination: RepoAPI) -> DataengineMappings:
    """
    reimport datasources and datasets
    """
    logger.info(f"Copying datasources and datasets from {source.repo_url} to {destination.repo_url} (without metadata)")
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
        _transfer_field_definitions(source_ds, new_ds)
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


def _transfer_field_definitions(source: Datasource, destination: Datasource):
    for field in source.fields:
        try:
            builder = destination.metadata_field(field.name).set_type(
                metadata_type_lookup_reverse[field.valueType.value]
            )
            if field.tags is not None:
                # noinspection PyProtectedMember
                builder._add_tags(field.tags)
            builder.apply()
        except Exception as e:
            logger.error(
                f"Failed to transfer field definition of {field} from {source.source.name} to {destination}: {e}"
            )


def reimport_dataengine_metadata(
    source: RepoAPI, destination: RepoAPI, de_mappings: DataengineMappings, storage_path: Path
):
    logger.info(f"Copying Data Engine metadata from {source.repo_url} to {destination.repo_url}")
    set_dataengine_host(source)
    source_datasource_list = datasources.get_datasources(source.full_name)
    source_datasources = {ds.source.id: ds for ds in source_datasource_list}

    set_dataengine_host(destination)
    destination_datasource_list = datasources.get_datasources(destination.full_name)
    destination_datasources = {ds.source.id: ds for ds in destination_datasource_list}

    for orig_ds_id, new_ds_id in de_mappings.datasources.items():
        orig_ds = source_datasources.get(orig_ds_id)
        if orig_ds is None:
            raise ValueError(
                f"Original datasource with ID {orig_ds_id} not found in datasources of repo {source.full_name}."
            )
        dest_ds = destination_datasources.get(new_ds_id)
        if dest_ds is None:
            raise ValueError(
                f"Destination datasource with ID {new_ds_id} not found in datasources of repo {destination.full_name}."
            )
        _reimport_datasource_metadata(orig_ds, dest_ds, storage_path)
        # TODO: remove so we import more than one datasource's metadata
        break


def _reimport_datasource_metadata(orig_ds: Datasource, new_ds: Datasource, storage_path: Path):
    logger.info(f"Reimporting metadata from datasource {orig_ds.source.name}")
    metadata_parquet = fetch_datasource_metadata(orig_ds, storage_path)
    upload_datasource_metadata(new_ds, metadata_parquet)


def fetch_datasource_metadata(ds: Datasource, metadata_dir: Path) -> Path:
    df = get_exportable_dataframe(ds)
    out_path = metadata_dir / f"metadata_{ds.source.name}.parquet"
    df.to_parquet(out_path)
    return out_path


def get_exportable_dataframe(ds: Datasource) -> pd.DataFrame:
    res = ds.fetch()
    # TODO: filter out autogenerated stuff
    res.download_binary_columns()
    df = res.dataframe

    def serialize_annotation(ann):
        if ann is None:
            return None
        elif isinstance(ann, MetadataAnnotations):
            # TODO: change URLs
            return ann.to_ls_task()
        elif isinstance(ann, bytes):
            return ann
        elif isinstance(ann, str):
            return ann.encode("utf-8")
        logger.warning(f"Unsupported annotation type: {type(ann)}: {ann}")
        return None

    for field in df.columns:
        if not _is_field_importable(field):
            df.drop(columns=[field], inplace=True)

    for ann_field in ds.annotation_fields:
        if ann_field in df.columns:
            df[ann_field] = df[ann_field].apply(serialize_annotation)

    return df


@lru_cache
def get_datasource(host: str, repo_name: str, ds_id: int) -> Datasource:
    orig_host = dagshub.common.config.host
    try:
        dagshub.common.config.host = host
        ds = datasources.get_datasource(repo_name, id=ds_id)
        return ds
    finally:
        dagshub.common.config.host = orig_host


@lru_cache
def get_dataset(host: str, repo_name: str, ds_id: int) -> Datasource:
    orig_host = dagshub.common.config.host
    try:
        dagshub.common.config.host = host
        ds = datasets.get_dataset(repo_name, id=ds_id)
        return ds
    finally:
        dagshub.common.config.host = orig_host


_autogenerated_fields = {"datapoint_id", "dagshub_download_url"}


def _is_field_importable(field: str) -> bool:
    return field not in _autogenerated_fields


def upload_datasource_metadata(ds: Datasource, metadata_parquet: Path):
    ds.upload_metadata_from_file(str(metadata_parquet))


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
