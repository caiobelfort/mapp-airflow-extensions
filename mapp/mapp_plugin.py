from airflow.plugins_manager import AirflowPlugin

from mapp.operators.dv_operators import HubFormatterOperator, LinkFormatterOperator, DatavaultInsertOperator

class MappPlugin(AirflowPlugin):
    name = "mapp_airflow_extensions"
    operators = [HubFormatterOperator, LinkFormatterOperator, DatavaultInsertOperator]
