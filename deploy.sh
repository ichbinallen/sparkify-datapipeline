#!/bin/bash

rm -rf  /home/allen/airflow/plugins
cp ./dags/sparkify-dag.py ~/airflow/dags/sparkify-dag.py
cp -r ./plugins ~/airflow/plugins
