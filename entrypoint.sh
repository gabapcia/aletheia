#!/bin/sh

set -e

airflow db init

exec $@
