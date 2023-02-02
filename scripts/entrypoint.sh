#!/usr/bin/env bash

airflow db init
airflow connections add 'postgres' \
    --conn-uri 'postgresql://dwh:dwh@postgres:5432/dwh'
airflow connections add 'clickhouse' \
    --conn-uri 'sqlite://bi:password@clickhouse/sales_mart'
airflow webserver
