from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_query="",
                 expected_result="",
                 *args, 
                 **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.expected_result = expected_result 
        self.test_query = test_query


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        records = redshift_hook.get_records(self.test_query)

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed, returned no results")

        num_records = records[0][0]
        
        if num_records < 1:
            raise ValueError(f"Data quality check failed, contained 0 rows")

        self.log.info(f"Data quality check passed with {records[0][0]} records")
