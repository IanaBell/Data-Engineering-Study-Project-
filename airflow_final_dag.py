from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

LOAD_TABLE_BI = "select std7_111.f_load_simple_partition('std7_111.bills_item', 'calday', '2021-01-01', '2021-03-01', 'gp.bills_item', 'intern', 'intern')"
LOAD_TABLE_BH = "select std7_111.f_load_simple_partition('std7_111.bills_head', 'calday', '2021-01-01', '2021-03-01', 'gp.bills_head', 'intern', 'intern')"
LOAD_TABLE_TRAFFIC = "select std7_111.f_load_traffic_partition('std7_111.traffic', 'date', '2021-01-01', '2021-03-01', 'gp.traffic', 'intern', 'intern')"

DB_CONN = "gp_std7_111"
DB_SCHEMA = "std7_111"

DB_PROC_LOAD = "f_load_full"
FULL_LOAD_TABLES = ['stores', 'coupons', 'promos', 'promo_types']
FULL_LOAD_FILES = {'stores': 'stores', 'coupons': 'coupons', 'promos': 'promos', 'promo_types': 'promo_types'}
MD_TABLE_LOAD_QUERY = f"select {DB_SCHEMA}.{DB_PROC_LOAD}(%(tab_name)s, %(file_name)s);"

MART_QUERY = "select std7_111.f_load_mart_final('20210101', '20210228')"

default_args = {
    'depends_on_past': False,
    'owner': 'std7_111',
    'start_date': datetime(2024, 10, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    "std7_111_final_dag",
    max_active_runs=3,
    schedule_interval='@monthly',
    default_args=default_args,
    catchup=False,
) as dag:
    
    task_start = DummyOperator(task_id="start")
    
    t1 = PostgresOperator(task_id=f"start_insert_table_bi",
                                   postgres_conn_id=DB_CONN,
                                   sql = LOAD_TABLE_BI
                                  )
    t2 = PostgresOperator(task_id=f"start_insert_table_bh",
                                   postgres_conn_id=DB_CONN,
                                   sql = LOAD_TABLE_BH
                                  )
    t3 = PostgresOperator(task_id=f"start_insert_table_traffic",
                                   postgres_conn_id=DB_CONN,
                                   sql = LOAD_TABLE_TRAFFIC
                                  )
    
    with TaskGroup("full_insert") as task_full_insert_tables:
        for table in FULL_LOAD_TABLES:
            task = PostgresOperator(task_id =f"load_table_{table}",
                                   postgres_conn_id=DB_CONN,
                                   sql=MD_TABLE_LOAD_QUERY,
                                   parameters={'tab_name':f'{DB_SCHEMA}.{table}', 'file_name':f'{FULL_LOAD_FILES[table]}'}
                                   )
            
    task_mart = PostgresOperator(task_id=f"start_insert_mart",
                                 postgres_conn_id=DB_CONN,
                                 sql=MART_QUERY
                                 )
            
    task_end = DummyOperator(task_id="end")
    
    task_start >> t1 >> t2 >> t3 >> task_full_insert_tables >> task_mart >> task_end