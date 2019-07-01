import datetime
import hashlib
import json
import os
import time
from tempfile import NamedTemporaryFile
from typing import List

import pandas as pd
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.plugins_manager import AirflowPlugin


def _get_data_from_gcs(gcp_conn_id, bucket, input):
    hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=gcp_conn_id)
    tmp_file = NamedTemporaryFile(delete=False)
    hook.download(bucket, input, tmp_file.name)
    filename = tmp_file.name

    return filename


def _get_key_values_as_lists(dictionary: dict, keys: list):
    """Retorna lista de lista dos valores de um dicionario"""
    return [[dictionary[v] for v in obj] if isinstance(obj, list) else [dictionary[obj]] for obj in keys]


def _convert_arr_to_str_arr(arr):
    """Convert uma lista de listas para lista de listas com todos os objetos como string"""
    return [[str(v) for v in subarr] for subarr in arr]


def _send_data_to_gcs(data: pd.DataFrame, gcp_conn_id, bucket, output):
    tmp_file = NamedTemporaryFile(delete=False)

    # Convert None to null
    data.to_json(tmp_file.name, orient='records', lines=True)

    hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=gcp_conn_id)
    hook.upload(bucket, output, filename=tmp_file.name, mime_type='application/json')
    os.remove(tmp_file.name)


class SatelliteFormatterOperator(BaseOperator):
    """
    Formats a GCS JSON file to Datavault Satellite Style and Send Back to GCS in the new format
    """

    template_fields = ('input_bucket', 'output_bucket', 'output_object', 'input_object', 'load_date')

    @apply_defaults
    def __init__(self,
                 input_bucket: str,
                 output_bucket: str,
                 input_object: str,
                 output_object: str,
                 business_keys: List[str],
                 checksum_keys: List[str],
                 record_source: str,
                 schema: List[dict],
                 load_date: str = '{{ds}}',
                 gcp_conn_id: str = 'google_cloud_default',
                 *args,
                 **kwargs
                 ):
        super(SatelliteFormatterOperator, self).__init__(*args, **kwargs)

        self.gcp_conn_id = gcp_conn_id
        self.input_bucket = input_bucket
        self.output_bucket = output_bucket
        self.input_object = input_object
        self.output_object = output_object
        self.business_keys = business_keys
        self.record_source = record_source
        self.load_date = load_date
        self.schema = schema
        self.checksum_keys = checksum_keys

    def execute(self, context):
        self.log.info('Getting data from GCS')
        filename = _get_data_from_gcs(self.gcp_conn_id, self.input_bucket, self.input_object)

        self.log.info('Processing lines...')
        with open(filename) as f:
            formatted_rows = pd.DataFrame(
                [self._format_row(json.loads(line)) for line in f],
                columns=self.schema
            ).drop_duplicates()

        self.log.info(f'Generate {len(formatted_rows)} candidates.')

        os.remove(filename)

        self.log.info('Sending processed data to GCS')
        _send_data_to_gcs(formatted_rows, self.gcp_conn_id, self.output_bucket, self.output_object)

    def _format_row(self, row):
        # Business Keys
        bks = _get_key_values_as_lists(row, self.business_keys)

        # Descriptive Keys
        cks = [c for sl in _get_key_values_as_lists(row, self.checksum_keys) for c in sl]

        # Gera hash key de cada business key fornecida via argumento de inicialização de classe
        hashed = []
        for b in _convert_arr_to_str_arr(bks):
            hashed.append(hashlib.md5('|'.join(sorted(b)).encode('utf-8')).hexdigest())

        # Gera hash do link a partir das hashs criadas pelas business keys
        hash_ = hashlib.md5('|'.join(sorted(hashed)).encode('utf-8')).hexdigest()

        checksum = hashlib.md5('|'.join(sorted([str(c) for c in cks])).encode('utf-8')).hexdigest()

        return [
                   hash_,
                   time.mktime(datetime.datetime.strptime(self.load_date, "%Y-%m-%d").timetuple()),
                   self.record_source
               ] + cks + [checksum]


class LinkFormatterOperator(BaseOperator):
    """
    Formats a GCS to Datavault Link and send back to GCS in new Format
    """

    template_fields = ('input_bucket', 'output_bucket', 'output_object', 'input_object', 'load_date')

    @apply_defaults
    def __init__(
            self,
            input_bucket: str,
            output_bucket: str,
            input_object: str,
            output_object: str,
            business_keys: List[str],
            record_source: str,
            schema: List[dict],
            load_date: str = '{{ds}}',
            gcp_conn_id='google_cloud_default',
            *args,
            **kwargs
    ):
        super(LinkFormatterOperator, self).__init__(*args, **kwargs)

        self.gcp_conn_id = gcp_conn_id
        self.input_bucket = input_bucket
        self.output_bucket = output_bucket
        self.input_object = input_object
        self.output_object = output_object

        if len(business_keys) < 2:
            raise RuntimeError('Link business key list must be of size larger than 1.')

        self.business_keys = business_keys
        self.record_source = record_source
        self.load_date = load_date
        self.schema = schema

    def execute(self, context):
        self.log.info('Getting data from GCS')
        filename = _get_data_from_gcs(self.gcp_conn_id, self.input_bucket, self.input_object)

        self.log.info('Processing lines...')
        with open(filename) as f:
            formatted_rows = pd.DataFrame(
                [self._format_row(json.loads(line)) for line in f],
                columns=self.schema
            ).drop_duplicates()

        self.log.info(f'Generate {len(formatted_rows)} candidates.')

        os.remove(filename)

        self.log.info('Sending processed data to GCS')
        _send_data_to_gcs(formatted_rows, self.gcp_conn_id, self.output_bucket, self.output_object)

    def _format_row(self, row):
        bks = _get_key_values_as_lists(row, self.business_keys)

        # Gera hash key de cada business key fornecida via argumento de inicialização de classe
        hashed = []
        for b in _convert_arr_to_str_arr(bks):
            hashed.append(hashlib.md5('|'.join(sorted(b)).encode('utf-8')).hexdigest())

        # Gera hash do link a partir das hashs criadas pelas business keys
        hash_ = hashlib.md5('|'.join(sorted(hashed)).encode('utf-8')).hexdigest()

        return [
                   hash_,
                   time.mktime(datetime.datetime.strptime(self.load_date, "%Y-%m-%d").timetuple()),
                   self.record_source
               ] + hashed


class HubFormatterOperator(BaseOperator):
    """
    Formats a GCS Object to Datavault Hub Format and send back to GCS in new format.
    """

    template_fields = ('input_bucket', 'output_bucket', 'output_object', 'input_object', 'load_date')

    @apply_defaults
    def __init__(
            self,
            input_bucket: str,
            output_bucket: str,
            input_object: str,
            output_object: str,
            business_keys: List[str],
            record_source: str,
            schema: List[dict],
            load_date: str = '{{ds}}',
            gcp_conn_id='google_cloud_default',
            *args,
            **kwargs
    ):
        super(HubFormatterOperator, self).__init__(*args, **kwargs)

        self.gcp_conn_id = gcp_conn_id
        self.input_bucket = input_bucket
        self.output_bucket = output_bucket
        self.input_object = input_object
        self.output_object = output_object

        if len(business_keys) > 1:
            raise RuntimeError('Hub business key list must be of size 1.')
        self.business_keys = business_keys
        self.record_source = record_source
        self.load_date = load_date
        self.schema = schema

    def execute(self, context):
        self.log.info('Getting data from GCS')
        filename = _get_data_from_gcs(self.gcp_conn_id, self.input_bucket, self.input_object)

        self.log.info('Processing lines...')
        with open(filename) as f:
            formatted_rows = pd.DataFrame(
                [self._format_row(json.loads(line)) for line in f],
                columns=self.schema
            ).drop_duplicates()

        self.log.info(f'Generate {len(formatted_rows)} candidates.')

        os.remove(filename)

        self.log.info('Sending processed data to GCS')
        _send_data_to_gcs(formatted_rows, self.gcp_conn_id, self.output_bucket, self.output_object)

    def _format_row(self, element):
        bks = _get_key_values_as_lists(element, self.business_keys)[0]

        hash_ = hashlib.md5('|'.join(sorted([str(v) for v in bks])).encode('utf-8')).hexdigest()

        return [
                   hash_,
                   time.mktime(datetime.datetime.strptime(self.load_date, "%Y-%m-%d").timetuple()),
                   self.record_source
               ] + bks  # Flattens the list


class DatavaultInsertOperator(BaseOperator):
    """
    """

    template_fields = ('staging_project_dataset_table_id',)

    def __init__(self,
                 bigquery_conn_id,
                 destination_project_dataset_table_id: str,
                 staging_project_dataset_table_id: str,
                 mode: str = 'hub',
                 schema: List[str] = None,
                 *args,
                 **kwargs
                 ):
        """
        :param destination_project_dataset_table_id:
            Where to insert the data in bigquery
        :param staging_project_dataset_table_id:
            Where staging data is located
        :param mode:
            Mode of insertion, if 'hub' and 'link', use only the table hash as check. If 'satellite' uses
            hash, load_date and checksum.
        :param schema:
            The schema of `hub_project_dataset_table_id`. If None, infer from BigQuery itself. The schema must have
            same order of columns as table and hub hash key MUST BE first column.
        :param args:
        :param kwargs:
        """
        super(DatavaultInsertOperator, self).__init__(*args, **kwargs)

        self.bigquery_conn_id = bigquery_conn_id
        self.destination_project_dataset_table_id = destination_project_dataset_table_id
        self.staging_project_dataset_table_id = staging_project_dataset_table_id

        self.schema = schema

        self.sql = """
            INSERT INTO `{table}` ({cols})
            SELECT 
                S.*
            FROM `{staging}` AS S 
                LEFT JOIN `{table}` AS T
                    ON S.{hash} = T.{hash}
            WHERE T.{hash} IS NULL 
        """
        if mode == 'satellite':
            self.sql += " OR (T.{hash} IS NOT NULL AND S.checksum <> T.checksum) "

        self.hook = None
        self.conn = None
        self.cursor = None

    def execute(self, context):
        # TODO Buscar schema do hub se não for passado schema via argumento

        sql = self.sql.format(
            table=self.destination_project_dataset_table_id,
            staging=self.staging_project_dataset_table_id,
            cols=','.join(self.schema),
            hash=self.schema[0]
        )

        self.log.info('Execution sql \n {}'.format(sql))

        self.hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id, use_legacy_sql=False)

        self.conn = self.hook.get_conn()

        self.cursor = self.conn.cursor()

        self.cursor.run_query(sql)

    def on_kill(self):
        super(DatavaultInsertOperator, self).on_kill()
        if self.cursor is not None:
            self.log.info('Cancelling running query')
            self.cursor.cancel_query()


class MappDvPlugin(AirflowPlugin):
    name = 'mapp_datavault'

    operators = [
        DatavaultInsertOperator,
        HubFormatterOperator,
        SatelliteFormatterOperator,
        LinkFormatterOperator
    ]
