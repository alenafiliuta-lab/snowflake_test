from airflow import DAG
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id='snowflake_local_file_upload',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    put_task = SQLExecuteQueryOperator(
        task_id='put_file_to_snowflake_stage',
        conn_id='snowflake_conn_id',
        hook_params={
            'database': 'MY_DB',
            'schema': 'PUBLIC'
        },
        sql="PUT 'file:///opt/airflow/data/Airline_Dataset.csv' @my_internal_stage",
        autocommit=True,
    )

    copy_task = SQLExecuteQueryOperator(
        task_id='copy_into_table',
        conn_id='snowflake_conn_id',
        hook_params={
            'database': 'MY_DB',
            'schema': 'PUBLIC'
        },
        sql="""
            COPY INTO Airline_Dataset
            FROM @my_internal_stage/Airline_Dataset.csv
            FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"');
        """,
        autocommit=True,
    )

    silver_load = SQLExecuteQueryOperator(
        task_id='load_silver_layer',
        conn_id='snowflake_conn_id',
        sql="CALL load_all_silver_tables();",
        hook_params={
            'database': 'MY_DB',
            'schema': 'PUBLIC'
        },
        autocommit=True,
        split_statements=False,
    )
    gold_load = SQLExecuteQueryOperator(
        task_id='load_gold_layer',
        conn_id='snowflake_conn_id',
        sql="CALL load_gold_layer();",
        hook_params={
            'database': 'MY_DB',
            'schema': 'PUBLIC'
        },
        autocommit=True,
        split_statements=False,
    )

    var =put_task >> copy_task >>silver_load >>gold_load
