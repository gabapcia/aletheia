from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook


class CreateKeyspaceOperator(BaseOperator):
    def __init__(self, keyspace: str, rf: int = 3, conn_id: str = 'cassandra_default', *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.rf = rf
        self.keyspace = keyspace

    def execute(self, context: Context) -> None:
        hook = CassandraHook(self.conn_id)

        with hook.get_conn() as conn:
            conn.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS "{self.keyspace}"
                    WITH REPLICATION = {{
                        'class': 'SimpleStrategy',
                        'replication_factor': {self.rf}
                    }}
            """)
