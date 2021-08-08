from dateutil import parser
from airflow.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """Load JSON data from S3 into Redshift.
    Args:
        redshift_conn_id: Redshift connection id.
        aws_credentials_id: AWS credentials for accessing S3
        table_name: The name of staging table.
        auto_or_jsonpaths: 'auto' or JSONPaths file used to parse JSON.
        execution_date: A templated field of the execution time that enables
        to load timestamped files from S3 based on it and run backfills.
        truncate_table: Whether to TRUNCATE table for initialization.
    Returns:
    Raises:
    """

    ui_color = "#358140"

    TRUNCATE_SQL = """
        TRUNCATE TABLE {};
        """

    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        """

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table_name="",  # public.staging_events, public.staging_songs
        s3_bucket="",  # s3://udacity-dend/log_data, song_data
        auto_or_jsonpaths="auto",  # log_json_path.json
        truncate_table=True,
        execution_date="{{ execution_date }}",
        *args,
        **kwargs,
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.aws_credentials_id = aws_credentials_id
        self.auto_or_jsonpaths = auto_or_jsonpaths
        self.truncate_table = truncate_table
        self.execution_date = execution_date

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.truncate_table:
            # To execute TRUNCATE,
            # you must be the owner of the table or a superuser.
            self.log.info(f"TRUNCATE table {self.table_name}")
            truncate_sql = self.TRUNCATE_SQL.format(self.table_name)
            redshift_hook.run(truncate_sql)

        # partitionBy("year", "month")
        execution_date = parser.parse(self.execution_date)
        s3_path = self.s3_bucket + "/{}/{}".format(
            execution_date.year, execution_date.month
        )

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        copy_sql = self.COPY_SQL.format(
            self.table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.auto_or_jsonpaths,
        )
        self.log.info(f"COPY JSON to staging table: {self.table_name}")
        redshift_hook.run(copy_sql)
