import logging
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

from dagshub import get_label_studio_client
from dagshub.common.api import RepoAPI
from dagshub.data_engine.model.datapoint import Datapoint
from dagshub.data_engine.model.datasource import Datasource
from dagshub.data_engine.model.query_result import QueryResult

from dagshub_import_export.dataengine import get_datasource
from dagshub_import_export.models.dataengine_mappings import DataengineMappings
from dagshub_import_export.models.import_config import ImportConfig
from dagshub_import_export.util import logger_name

logger = logging.getLogger(logger_name)

if TYPE_CHECKING:
    from label_studio_sdk import Project, LabelStudio


def reimport_labelstudio(import_config: ImportConfig, ds_map: DataengineMappings):
    source, destination = import_config.source_and_destination

    source_ls = _get_ls_client(source)
    projects = source_ls.projects.list()

    # TODO: check what already exists in target labelstudio, and don't import (or maybe remove?) projects that already exist

    for project in projects.items:
        import_ls_project(source_ls, project, import_config, ds_map)
        break  # TODO: get rid


def import_ls_project(
    ls_client: "LabelStudio", project: "Project", import_config: ImportConfig, ds_map: DataengineMappings
):
    source, destination = import_config.source_and_destination

    logger.info(f"Importing Label Studio project {project.title} to {destination.repo_url}")
    source_project_info = get_ls_project_info(ls_client, project)

    source_ds = get_datasource(host=source.host, repo=source.full_name, ds_id=source_project_info.datasource_id)
    target_ds_id = ds_map.datasources[source_project_info.datasource_id]
    target_ds = get_datasource(host=destination.host, repo=destination.full_name, ds_id=target_ds_id)

    target_datapoints = _get_matching_datapoints(source_ds, target_ds, source_project_info.datapoints)
    target_ds.send_datapoints_to_annotation(
        datapoints=target_datapoints,
        fields_to_embed=_get_embedded_fields(target_ds, source_project_info.fields),
        open_project=False,
        ignore_warning=True,
    )

    pass

    # if ds_id is None:
    #     return
    #
    # target_ds_id = ds_map.datasources.get(ds_id)
    # if target_ds_id is None:
    #     logger.info(f"Datasource {ds_id} not found in destination, skipping project {project.title}")
    #     return
    #
    # target_ds = get_datasource(host=destination.host, repo=destination.full_name, ds_id=target_ds_id)

    # TODO:
    """
    - fetch all tasks in the project
    - get the associated datapoint ids
    
    - fetch all datapoints from the base and target datasource
    - do a mapping of dp id -> dp id
    - run datasource.send_datapoints_to_annotation() with the mapped datapoints
    """
    # target_ds.annotate()


def _get_matching_datapoints(source_ds: Datasource, target_ds: Datasource, datapoints: list[int]) -> list[Datapoint]:
    source_ds.clear_query(reset_to_dataset=False)
    target_ds.clear_query(reset_to_dataset=False)

    # Get only paths, we only need that to perform the mapping
    source_dps = _dp_id_lookup(source_ds.select("path").all())
    target_dps = target_ds.select("path").all()

    res = []

    for dp_id in datapoints:
        source_dp = source_dps.get(dp_id)
        if source_dp is None:
            logger.warning(f"Datapoint with id {dp_id} not found in source datasource, skipping")
            continue
        try:
            target_dp = target_dps[source_dp.path]
            res.append(target_dp)
        except KeyError:
            logger.warning(f"Datapoint with path {source_dp.path} not found in target datasource, skipping")
            continue

    return res


def _get_embedded_fields(target_ds: Datasource, fields: set[str]) -> list[str]:
    res = []
    for field in fields:
        if target_ds.has_field(field):
            res.append(field)
    return res


def _dp_id_lookup(dps: QueryResult) -> dict[int, Datapoint]:
    return {dp.datapoint_id: dp for dp in dps}


@dataclass
class LabelStudioProjectInfo:
    datasource_id: int
    datapoints: list[int]
    fields: set[str]


def get_ls_project_info(ls_client: "LabelStudio", project: "Project") -> Optional[LabelStudioProjectInfo]:
    tasks = ls_client.tasks.list(project=project.id)
    if not tasks.items:
        logger.info(f"Project {project.id} has no tasks, can't import it")
        return None

    project_info = LabelStudioProjectInfo(datasource_id=0, datapoints=[], fields=set())

    for task in tasks:
        if project_info.datasource_id == 0 and task.meta.get("datasource_id") is not None:
            project_info.datasource_id = task.meta["datasource_id"]
        if task.meta.get("datapoint_id") is not None:
            project_info.datapoints.append(task.meta["datapoint_id"])
        for field in task.data.keys():
            project_info.fields.add(field)

    return project_info


def _get_ls_client(repo: RepoAPI):
    return get_label_studio_client(repo.full_name, host=repo.host)
