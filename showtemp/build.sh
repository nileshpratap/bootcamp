#!/usr/bin/env bash
# exit on error
set -o errexit

curl -sSL https://install.python-poetry.org | python3 -
source $HOME/.poetry/env

poetry install

python manage.py collectstatic --no-input
python manage.py migrate