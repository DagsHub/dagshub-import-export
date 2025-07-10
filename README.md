# DagsHub Import-Export tool

This Python package can be used as a CLI to copy almost all aspects of a DagsHub repo, even across different DagsHub installations.

This copies:
- [x] Git
- [x] Data stored in the repo's DagsHub storage bucket
- [x] DVC managed data, provided that it's pushed to the DagsHub repo's builtin DVC remote
- [x] MLflow experiments
- [x] MLflow models (?)
- [x] MLflow artifacts
- [x] DagsHub Data Engine datasources and datasets, including all __latest__ metadata, annotations, etc.

__Not copied yet:__
- [ ] Label Studio projects and tasks. Prior to running this tool, you should click the green Save icon at the top of the Label Studio project UI to save the tasks as metadata back to Data Engine, which does get copied and can be used to re-create the Label Studio project and tasks in the new location.
- [ ] MLflow traces (LLM traces feature)
- [ ] _Historical_ versions of Data Engine (datasources) metadata values. Only the current version of metadata values is currently copied. Contact us if copying the full history is a requirement.

## Requirements
To run: Python 3.10 or later

### Prerequisites:
- The source and destination repositories exist and are accessible from this computer,
  and you have permission to write to the destination.
- (for datasources) The destination repository has (at least) the same connected buckets as the source repository. You need to manually connect the same buckets in the target repo before starting this script. If any bucket is missing in the target, the script will print a warning and refuse to start.
- (for MLflow) There is nothing in the destination repository's MLflow. If any experiments are already logged in the target repo, the script will print a warning and refuse to start.
- Since Label Studio projects and tasks are currently not copied, it's __strongly recommended__ that you Save any existing Label Studio projects back to a Data Engine metadata field. Otherwise, the existing human-labeled task data will be left behind.


## Installation

With uv (recommended):
```shell
uv tool install git+https://github.com/dagshub/dagshub-import-export
```

With regular pip (recommended to create a virtual environment first):
```shell
pip install git+https://github.com/dagshub/dagshub-import-export
```

## Usage
```shell
dagshub-import-export https://one-dagshub-deployment.com/user/repo-to-import-from https://another-dagshub-deployment.com/other-user/other-repo-to-export-to
```

By default, everything will be imported.

You can choose which specific things to import by using corresponding flags:
- `--git`
- `--dvc`
- `--bucket`
- `--datasources`
- `--metadata`
- `--mlflow`
- `--labelstudio`

Turning any of them on will make it so ONLY the subsystems set with the flags are imported.

Includes a vendored version of MLflow's [mlflow-export-import](https://github.com/mlflow/mlflow-export-import), licensed under Apache 2.0, with DagsHub specific fixes
