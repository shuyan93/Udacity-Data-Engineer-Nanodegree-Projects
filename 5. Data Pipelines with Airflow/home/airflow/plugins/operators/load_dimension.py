from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = """
        INSERT INTO {}
        {}
        ;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 delete_load=False,
                 sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.delete_load = delete_load
        self.sql = sql

 
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_load:
            self.log.info("Truncating Redshift table")
            redshift.run("DELETE FROM {}".format(self.destination_table))

        sql_statement = LoadDimensionOperator.insert_sql.format(
            self.destination_table,
            self.sql
        )
        self.log.info(f"Executing {formatted_sql} ...")
        redshift.run(sql_statement)