import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator


with DAG(
    dag_id="ETL",
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
) as dag:
    postgres_ddl = PostgresOperator(
        task_id="populate_postgres",
        postgres_conn_id="postgres",
        sql="sql/postgres-ddl.sql",
    )
    clickhouse_ddl = ClickHouseOperator(
        task_id="populate_clickhouse",
        clickhouse_conn_id="clickhouse",
        sql=(
        """
        DROP TABLE IF EXISTS fact_sales_mart;
        """, """
        CREATE TABLE fact_sales_mart (
              id Int16
            , invoice_id FixedString(11)
            , "date" Date
            , "datetime" DateTime
            , epoch Int64
            , "hour" Int8
            , "minute" Int8
            , day_suffix String
            , day_name String
            , day_of_week Int8
            , day_of_month Int8
            , day_of_quarter Int8
            , day_of_year Int16
            , week_of_month Int8
            , week_of_year Int8
            , month_actual Int8
            , month_name String
            , month_name_short FixedString(3)
            , quarter_actual Int8
            , quarter_name String
            , year_actual Int16
            , first_day_of_week Date
            , last_day_of_week Date
            , first_day_of_month Date
            , last_day_of_month Date
            , first_day_of_quarter Date
            , last_day_of_quarter Date
            , first_day_of_year Date
            , last_day_of_year Date
            , mmyyyy FixedString(6)
            , mmddyyyy FixedString(8)
            , weekend Bool
            , branch FixedString(1)
            , city String
            , customer_type String
            , gender String
            , product_line String
            , payment String
            , unit_price Decimal(4,2)
            , quantity Int8
            , tax Decimal(8,4)
            , total Decimal(8,4)
            , cogs Decimal(6,2)
            , gross_margine_percentage Decimal(11,10)
            , gross_income Decimal(8,4)
            , rating Decimal(2,1)
        ) ENGINE = MergeTree()
        ORDER BY date
        PARTITION BY year_actual
        """, """
        DROP DATABASE IF EXISTS postgresql
        """, """
        CREATE DATABASE postgresql
        ENGINE = PostgreSQL('postgres:5432', 'dwh', 'dwh', 'dwh', 'dwh', 1)
        """
        ),
    )
    load_csv = PostgresOperator(
        task_id="load_csv",
        postgres_conn_id="postgres",
        sql="sql/load_csv.sql",
    )
    validate_raw_data = PostgresOperator(
        task_id="validate_raw_data",
        postgres_conn_id="postgres",
        sql="sql/validate.sql",
    )
    load_nds = PostgresOperator(
        task_id="load_nds",
        postgres_conn_id="postgres",
        sql="sql/nds.sql",
    )
    load_dwh = PostgresOperator(
        task_id="load_dwh",
        postgres_conn_id="postgres",
        sql="sql/dwh.sql",
    )
    load_mart = ClickHouseOperator(
        task_id="load_mart",
        clickhouse_conn_id="clickhouse",
        sql="""
        INSERT INTO fact_sales_mart 
        SELECT * FROM postgresql.fact_sales_w
        """,
    )

    [postgres_ddl, clickhouse_ddl] >> load_csv >> validate_raw_data >> load_nds >> load_dwh >> load_mart
