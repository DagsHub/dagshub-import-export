import logging
import re
import time
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

import httpx
from dagshub import get_label_studio_client
from dagshub.common.api import RepoAPI
from dagshub.common.util import multi_urljoin
from dagshub.data_engine.model.datapoint import Datapoint
from dagshub.data_engine.model.datasource import Datasource
from dagshub.data_engine.model.query_result import QueryResult
from pydantic import BaseModel

from dagshub_import_export.dataengine import get_datasource
from dagshub_import_export.models.dataengine_mappings import DataengineMappings
from dagshub_import_export.models.import_config import ImportConfig
from dagshub_import_export.util import logger_name, retry_5_times

logger = logging.getLogger(logger_name)

if TYPE_CHECKING:
    from label_studio_sdk import Project, LabelStudio


class LSProjectLimitExceededError(Exception):
    def __str__(self):
        return "Limit of annotation projects reached. Contact administrators to increate the repo limit."


def reimport_labelstudio(import_config: ImportConfig, ds_map: DataengineMappings):
    source, destination = import_config.source_and_destination

    print(
        "\n\n!!WARNING!!\n\n"
        "Label Studio import might require answering to some prompts, "
        "so make sure to not run it in a non-interactive environment.\n\n"
    )

    # TODO: add wait for workspace setup
    source_ls = _get_ls_client(source)
    destination_ls = _get_ls_client(destination)

    for project in _get_projects_to_import(source_ls, destination_ls):
        try:
            import_ls_project(source_ls, destination_ls, project, import_config, ds_map)
        except LSProjectLimitExceededError as e:
            logger.error(str(e))
            return


def _get_projects_to_import(source_ls: "LabelStudio", dest_ls: "LabelStudio") -> list["Project"]:
    """Return only projects that don't already exist in the destination Label Studio"""
    source_projects = list(source_ls.projects.list().items)
    destination_projects = list(dest_ls.projects.list())

    if not source_projects:
        logger.info("No projects in the source Label Studio")
        return []

    dp_titles = set([p.title for p in destination_projects])

    projects_to_import = []
    existing_projects = []

    for p in source_projects:
        if p.title in dp_titles:
            existing_projects.append(p)
        else:
            projects_to_import.append(p)

    if projects_to_import:
        logger.info(f"Projects to import: {', '.join([p.title for p in projects_to_import])}")
    else:
        logger.info("No projects to import, all projects already exist in the destination Label Studio")
        return []

    if existing_projects:
        logger.info(
            f"Projects that already exist in the target Label Studio: {', '.join([p.title for p in existing_projects])}"
        )

    return projects_to_import


def import_ls_project(
    source_ls: "LabelStudio",
    dest_ls: "LabelStudio",
    project: "Project",
    import_config: ImportConfig,
    ds_map: DataengineMappings,
):
    source, destination = import_config.source_and_destination

    logger.info(f"Importing Label Studio project {project.title} to {destination.repo_url}")
    source_project_info = get_ls_project_info(source_ls, project)
    if source_project_info is None:
        return

    if source_project_info.datasource_id not in ds_map.datasources:
        logger.warning(
            f"Datasource with ID {source_project_info.datasource_id} from project {project.title} "
            "does not exist. Maybe it was deleted. Skipping importing this project."
        )
        return

    source_ds = get_datasource(host=source.host, repo=source.full_name, ds_id=source_project_info.datasource_id)
    target_ds_id = ds_map.datasources[source_project_info.datasource_id]
    target_ds = get_datasource(host=destination.host, repo=destination.full_name, ds_id=target_ds_id)

    if not target_ds.annotation_fields:
        logger.warning(
            f"Target datasource {target_ds.source.name} has no annotation fields, "
            "you will need to create one before importing the project."
        )
        return

    target_datapoints = _get_matching_datapoints(source_ds, target_ds, source_project_info.datapoints)
    print("\n\nIgnore the following URL, the script will import the project for you by itself")
    annotation_url = target_ds.send_datapoints_to_annotation(
        datapoints=target_datapoints,
        fields_to_embed=_get_embedded_fields(target_ds, source_project_info.fields),
        open_project=False,
        ignore_warning=True,
    )
    print("\n\n")
    annotation_field = _choose_annotation_field(target_ds, source_project_info)
    dest_project_url = create_ls_project(annotation_url, destination, project, annotation_field)
    try:
        _transfer_ls_project_settings(project, dest_ls, dest_project_url)
    except AssertionError:
        logger.warning(f"Failed to transfer settings for project {project.title}, you will have to do it manually")
    logger.info(f"Done importing project {project.title}")


def create_ls_project(
    annotation_url: str, destination: RepoAPI, original_project: "Project", annotation_field: str
) -> Optional[str]:
    dp_uuid = _get_datapoints_uuids(annotation_url)
    create_url = multi_urljoin(destination.repo_api_url, "data-engine/annotations/create-tasks")

    project_settings = DagshubLSProjectInitSettings(
        project_name=original_project.title, uuid=dp_uuid, read_annotation_name=annotation_field
    )
    res = httpx.post(create_url, json=project_settings.model_dump(), auth=destination.auth)
    if res.status_code != 200:
        if res.text and "too many annotation projects" in res.text:
            raise LSProjectLimitExceededError()
        logger.warning(f"Failed to create Label Studio project (Status code {res.status_code}): {res.text}")
        return None
    project_url = res.json().get("project_link")
    return project_url


def _choose_annotation_field(dest_ds: Datasource, source_project_info: "LabelStudioProjectInfo") -> str:
    possible_fields = _get_possible_annotation_fields(dest_ds, source_project_info)

    if len(possible_fields) == 1:
        return possible_fields[0]
    elif len(possible_fields) > 1:
        return prompt_user_choice(
            possible_fields,
            "Multiple possible annotation fields found. "
            "Please choose the one that is used as the annotation task field in the source project:",
        )
    else:
        return prompt_user_choice(
            dest_ds.annotation_fields,
            "The source Label Studio project doesn't have any annotation fields that match the ones in the datasource. "
            "Please choose an annotation field to use as the LS task:",
        )


def _get_possible_annotation_fields(dest_ds: Datasource, source_project_info: "LabelStudioProjectInfo") -> list[str]:
    res = []
    for field in dest_ds.annotation_fields:
        if field in source_project_info.fields:
            res.append(field)
    return res


def _get_datapoints_uuids(url: str) -> str:
    """
    Parses out the uuid sent from the backend for the datapoint session uuid that were sent to annotation

    "https://test.dagshub.com/kirill/COCO_1K_Backup/annotations/de/send-to-annotation?datapointsUuid=24b10f5e-3df1-49dd-aaab-0558a38d2f84"
    """
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    dp_uuid = query_params.get("datapointsUuid", [None])[0]
    if dp_uuid is None:
        raise ValueError("No datapointsUuid found in the URL")
    return dp_uuid


def _get_project_id_from_url(url: Optional[str]) -> int:
    if url is None:
        raise ValueError("URL is None, cannot extract project ID")
    match = re.search(r"/projects/(\d+)/data$", url)
    if not match:
        raise ValueError(f"Project ID not found in URL {url}")
    return int(match.group(1))


@retry_5_times
def _transfer_ls_project_settings(
    source_project: "Project", target_ls: "LabelStudio", target_project_url: str
) -> "Project":
    # For some reason you need to fetch the project a couple of times with waiting,
    # otherwise the label config doesn't get saved (and might even get rolled back
    # That's why this function is both retried, and also has sleeps in it
    # Technically this can be retried 5x5=25 times (5 for checking in this function, and 5 for the retry decorator)
    logger.info("Transferring project settings")

    target_project_id = _get_project_id_from_url(target_project_url)

    successes = 0
    tries = 5
    success_threshold = 3

    for _ in range(tries):
        if successes >= success_threshold:
            break
        target_ls.projects.update(
            id=target_project_id,
            description=source_project.description,
            label_config=source_project.label_config,
            expert_instruction=source_project.expert_instruction,
        )
        time.sleep(3)
        proj = target_ls.projects.get(id=target_project_id)
        if proj.label_config == source_project.label_config:
            successes += 1
        else:
            successes = 0
    if successes < success_threshold:
        raise AssertionError(
            f"Failed to transfer project settings for project {source_project.id} to target project {target_project_id}"
        )
    return target_ls.projects.get(id=target_project_id)


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


def prompt_user_choice(options: list[Any], prompt: str = "Choose an option:") -> Any:
    if not options:
        raise ValueError("No options provided to choose from.")
    if len(options) == 1:
        return options[0]
    while True:
        print(prompt)
        for idx, option in enumerate(options, 1):
            print(f"{idx:>3}: {option}")
        choice = input("Enter the number of your choice: ")
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(options):
                return options[idx]
        print("Invalid choice. Please try again.")


@dataclass
class LabelStudioProjectInfo:
    datasource_id: int
    datapoints: list[int]
    fields: set[str]


# The defaults are specific for the setup we're using here - not transferring configs from existing,
# create a new project with its own name
class DagshubLSProjectInitSettings(BaseModel):
    project_name_select: bool = True
    project_id: int = 0
    project_name: str
    enable_settings_re_use: bool = False
    enable_settings_re_use_project_id: int = 0
    read_annotation_name: str
    uuid: str


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
