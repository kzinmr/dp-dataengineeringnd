from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """Load the fact table data from staging tables.
    Args:
        redshift_conn_id: Redshift connection id.
        table_name: The name of the fact table.
        insert_columns: The columns of the INSERT statement to the table.
        insert_select: The SELECT part of the INSERT statement to the table.
        truncate_table: Whether to TRUNCATE table for initialization.
        Set false by default to avoid inserting the entire records in the
        massive fact table.
    Returns:
    Raises:
    """

    ui_color = "#F98866"

    TRUNCATE_SQL = """
        TRUNCATE TABLE {};
        """

    INSERT_SQL = """
        INSERT INTO {} ({}) {};
        """

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        table_name="public.songplays",
        insert_columns="",
        insert_select="",  # SqlQueries.songplay_table_insert
        truncate_table=False,
        *args,
        **kwargs,
    ):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.insert_columns = insert_columns
        self.insert_select = insert_select
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.truncate_table:
            self.log.info(f"TRUNCATE table {self.table_name}")
            truncate_sql = self.TRUNCATE_SQL.format(self.table_name)
            redshift_hook.run(truncate_sql)
        self.log.info(f"INSERT data into fact table {self.table_name}")
        insert_sql = self.INSERT_SQL.format(
            self.table_name, self.insert_columns, self.insert_select
        )
        redshift_hook.run(insert_sql)
