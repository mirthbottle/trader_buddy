# Gain Tracker ETL

## Install

```
export PATH="$PATH:/home/vscode/.local/bin"
```

## Local Testing

- cd to `projects/gain_tracker/`
- set environment vars
- run dagster

```
set -a
source ../../jupyter-notebooks/.env
set +a

dagster dev -f gain_tracker/definitions.py
```

## Tests

run unit tests from `projects/gain_tracker` folder

```
python -m pytest -vs tests/
```