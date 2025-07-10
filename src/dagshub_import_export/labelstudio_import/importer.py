import logging
from typing import Optional, TYPE_CHECKING

from dagshub import get_label_studio_client
from dagshub.common.api import RepoAPI

from dagshub_import_export.dataengine import get_datasource
from dagshub_import_export.models.dataengine_mappings import DataengineMappings
from dagshub_import_export.models.import_config import ImportConfig
from dagshub_import_export.util import logger_name

logger = logging.getLogger(logger_name)

if TYPE_CHECKING:
    from label_studio_sdk import Project, LabelStudio


def reimport_labelstudio(import_config: ImportConfig, ds_map: DataengineMappings):
    source, destination = import_config.source_and_destination
    # FIXME: remove once done
    logger.warning("Not implemented yet, skipping Label Studio import")
    return

    source_ls = _get_ls_client(source)
    projects = source_ls.projects.list()

    for project in projects.items:
        import_ls_project(source_ls, project, ds_map)

    print(source_ls)


def import_ls_project(ls_client: "LabelStudio", project: "Project", destination: RepoAPI, ds_map: DataengineMappings):
    logger.info(f"Importing Label Studio project {project.title} to {destination.repo_url}")
    ds_id = get_project_associated_datasource(ls_client, project)

    if ds_id is None:
        return

    target_ds_id = ds_map.datasources.get(ds_id)
    if target_ds_id is None:
        logger.info(f"Datasource {ds_id} not found in destination, skipping project {project.title}")
        return

    target_ds = get_datasource(host=destination.host, repo=destination.full_name, ds_id=target_ds_id)

    # TODO:
    """
    - fetch all tasks in the project
    - get the associated datapoint ids
    
    - fetch all datapoints from the base and target datasource
    - do a mapping of dp id -> dp id
    - run datasource.send_datapoints_to_annotation() with the mapped datapoints
    """
    # target_ds.annotate()


def get_project_associated_datasource(ls_client: "LabelStudio", project: "Project") -> Optional[int]:
    tasks = ls_client.tasks.list(project=project.id, page_size=1)
    if not tasks.items:
        logger.info(f"Project {project.id} has no tasks, can't import it")
        return None

    task = tasks.items[0]

    project_ds_id = task.meta.get("datasource_id")
    if project_ds_id is None:
        logger.info(f"Project {project.id} task doesn't have an assigned datasource, can't import it")
        return None

    return project_ds_id


def _get_ls_client(repo: RepoAPI):
    return get_label_studio_client(repo.full_name, host=repo.host)
