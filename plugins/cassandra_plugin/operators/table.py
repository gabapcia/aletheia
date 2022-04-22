from typing import List, Dict
from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from cassandra.cluster import Session


class CreateTableOperator(BaseOperator):
    def __init__(
        self,
        keyspace: str,
        table: str,
        fields: Dict[str, str],
        partition: List[str],
        primary_key: List[str],
        indexes: List[str] = None,
        conn_id: str = 'cassandra_default',
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.keyspace = keyspace
        self.table = table
        self.fields = fields
        self.partition = partition
        self.primary_key = primary_key
        self.indexes = indexes

    def execute(self, context: Context) -> None:
        hook = CassandraHook(self.conn_id)

        with hook.get_conn() as conn:
            self._create_table(conn)

            if self.indexes:
                self._create_indexes(conn)

    def _create_table(self, conn: Session) -> None:
        fields = ',\n'.join([f'{field} {type_}' for field, type_ in self.fields.items()])
        partition = ', '.join(self.partition)
        primary_key = ', '.join(self.primary_key)
        if primary_key:
            primary_key = f', {primary_key}'

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS "{self.keyspace}"."{self.table}" (
                {fields},
                PRIMARY KEY (({partition}){primary_key})
            );
        """)

    def _create_indexes(self, conn: Session) -> None:
        for field in self.indexes:
            conn.execute(f"""
                CREATE INDEX IF NOT EXISTS {field}_idx
                ON "{self.keyspace}"."{self.table}"
                ({field});
            """)
