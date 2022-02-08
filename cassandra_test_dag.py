from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.apache.cassandra.sensors.record import CassandraRecordSensor
from airflow.providers.apache.cassandra.sensors.table import CassandraTableSensor
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
import pprint

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

hook = CassandraHook('cassandra_cqlproxy')
pp = pprint.PrettyPrinter(indent=4)
strCQL = "SELECT * FROM system.local;"

def check_table_exists(keyspace_name, table_name):
    print("Checking for the existence of " + keyspace_name + "." + table_name)
    hook.keyspace = keyspace_name
    return hook.table_exists(table_name)

def execute_query(query):
    pp.pprint(hook.get_conn().execute(query).current_rows)

with DAG(
    'cass_hooks_tutorial',
    default_args=default_args,
    description='An example Cassandra DAG using Cassandra hooks',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022,2,7),
    catchup=False,
    tags=['example','cassandra'],
) as dag:

    check_table_exists = PythonOperator(
        task_id="check_table_exists",
        python_callable=check_table_exists,
        op_args=['system','local'],
    )

    query_system_local = PythonOperator(
        task_id="query_system_local",
        python_callable=execute_query,
        op_args=[strCQL]
    )

    check_table_exists >> query_system_local
