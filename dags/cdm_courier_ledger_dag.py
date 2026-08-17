import pendulum
from airflow.decorators import dag, task
from config_const import ConfigConst
from repositories.pg_connect import ConnectionBuilder

from sprint5_project.courier_ledger import CourierLedgerLoader


@dag(
    schedule_interval='0/30 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm', 'courier_ledger'],
    is_paused_upon_creation=False
)
def project_sprint5_cdm_courier_ledger():
    @task
    def courier_ledger_load():
        dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)
        loader = CourierLedgerLoader(dwh_pg_connect)
        loader.load_report()

    courier_ledger_load()  # type: ignore


dag = project_sprint5_cdm_courier_ledger()  # noqa
