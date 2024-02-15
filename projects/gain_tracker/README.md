# Gain Tracker ETL

## Local Testing

- set environment vars
- run dagster from `projects/gain_tracker/gain_tracker`

```
set +a
source ../../jupyter-notebooks/.env
set -a

dagster dev -f definitions.py
```

## Tests

run unit tests from `projects/gain_tracker` folder

```
python -m pytest -vs tests/
```