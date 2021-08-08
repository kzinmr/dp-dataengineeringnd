from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """Load the dimension table data from staging tables.
    Args:
        redshift_conn_id: Redshift connection id.
        table_name: The name of the dimension table.
        insert_columns: The columns of the INSERT statement to the table.
        insert_select: The SELECT part of the INSERT statement to the table.
        truncate_table: Whether to TRUNCATE table for initialization.
        Set true by default to implement truncate-insert pattern for dimension
        tables.
    Returns:
    Raises:
    """

    ui_color = "#80BD9E"

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
        table_name="",  # public.users, songs, artists, time
        insert_columns="",
        insert_select="",  # SqlQueries.user_table_insert, ...
        truncate_table=True,
        *args,
        **kwargs,
    ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
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
        self.log.info(f"INSERT data into dimension table {self.table_name}")
        insert_sql = self.INSERT_SQL.format(
            self.table_name, self.insert_columns, self.insert_select
        )
        redshift_hook.run(insert_sql)
