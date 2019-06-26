from airflow.plugins_manager import AirflowPlugin

from mapp.operators.dv_operators import HubFormatterOperator, LinkFormatterOperator, DatavaultInsertOperator

class MappPlugin(AirflowPlugin):
    name = "mapp"
    operators = [HubFormatterOperator,
                 LinkFormatterOperator,
                 DatavaultInsertOperator]
