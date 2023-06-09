from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    delete_load_sql='''
    drop table if exists {}
    create table {}
    as
    {}
    '''

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id="",
                 table="",
                 sql_stmt="",
                 *args, **kwargs):
        

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_stmt=sql_stmt

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql=LoadFactOperator.delete_load_sql.format(
            self.table,
            self.table,
            self.sql_stmt
        )
        self.log.info(f"Loading fact table '{self.table}' into Redshift")
        redshift.run(formatted_sql)
        
