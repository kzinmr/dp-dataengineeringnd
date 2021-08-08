import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

CREATE_TABLES_SQL_PATH = "create_tables.sql"

dag = DAG(
    "setup_database",
    description="Create tables in Redshift.",
    schedule_interval=None,
    start_date=datetime.datetime(2019, 1, 1),
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)
end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

with open(CREATE_TABLES_SQL_PATH) as fp:
    sql_statements = fp.read().split("\n\n")

drop_create_table_sqls = [
    PostgresOperator(
        task_id="create_logdata_table",
        dag=dag,
        postgres_conn_id="redshift",
        sql=sql_statement.strip(),
    )
    for sql_statement in sql_statements
]

for drop_create_table in drop_create_table_sqls:
    start_operator >> drop_create_table

for drop_create_table in drop_create_table_sqls:
    drop_create_table >> end_operator
