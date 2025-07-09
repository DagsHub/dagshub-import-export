
## Requirements
To run: Python 3.10 or later

Repository requirements:
- The source and destination repositories exist and are accessible from this computer,
  and you have permission to write to the destination.
- (for datasources) The destination repository has the same connected integrations as the source repository.
- (for MLflow) There is nothing in the destination repository's MLflow.


## Installation

```shell
pip install git+https:///github.com/dagshub/dagshub-import-export
```

## Usage
```shell
dagshub-import-export https://one-dagshub-deployment.com/user/repo https://another-dagshub-deployment.com/other-user/other-repo
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