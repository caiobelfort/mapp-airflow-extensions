from airflow.plugins_manager import AirflowPlugin

from mapp.operators.dv_operators import *
from mapp.operators.mssql_operators import *
from mapp.operators.gcs_operators import *

class MappPlugin(AirflowPlugin):
    name = "mapp"
    operators = [
        HubFormatterOperator,
        LinkFormatterOperator,
        SatelliteFormatterOperator,
        MsSqlToGoogleCloudStorageOperator,
        PartitionedMsSqlToGoogleCloudStorageOperator,
        DatavaultInsertOperator,
        GCSObjectExistsShortCircuitOperator
    ]

