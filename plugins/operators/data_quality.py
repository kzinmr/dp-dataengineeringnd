from typing import Callable, List
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """Check the data quality of created tables.
    Args:
        redshift_conn_id: Redshift connection id.
        check_queries: SQL queries for data quality checks.
        predicates: Predicates to validate the results of check_queries.
    Returns:
    Raises:
        ValueError if one of the following situation occurs:
        - No records are returned from Redshift.
        - Any of the returned records are invalid for the given predicates.
    """

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        check_queries: List[str] = [],
        predicates: List[Callable[[int], bool]] = [],
        *args,
        **kwargs,
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_queries = check_queries
        self.predicates = predicates

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for query, predicate in zip(self.check_queries, self.predicates):
            self.log.info(f"Data quality check: {query}")

            records = redshift_hook.get_records(query)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    "Data quality check failed... "
                    f"no results are returned for {query}"
                )

            num_records = int(records[0][0])
            if not predicate(num_records):
                raise ValueError(
                    "Data quality check failed... "
                    f"returned: {num_records}, query: {query}"
                )

            self.log.info("passed!")
