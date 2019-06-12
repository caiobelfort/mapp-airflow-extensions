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


def _get_data_from_gcs(gcp_conn_id, bucket, input):
    hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=gcp_conn_id)
    tmp_file = NamedTemporaryFile(delete=False)
    hook.download(bucket, input, tmp_file.name)
    filename = tmp_file.name

    return filename


def _format_business_keys(row, business_keys):
    formatted_bk_list = []
    for bk in business_keys:
        if isinstance(bk, list):
            formatted_bk_list.append([str(row[v]) for v in bk])
        else:
            formatted_bk_list.append([str(row[bk])])

    return formatted_bk_list


def _send_data_to_gcs(data: pd.DataFrame, gcp_conn_id, bucket, output):
    tmp_file = NamedTemporaryFile(delete=False)
    data.to_json(tmp_file.name, orient='records', lines=True)
    hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=gcp_conn_id)
    hook.upload(bucket, output, filename=tmp_file.name, mime_type='application/json')
    os.remove(tmp_file.name)


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
        bks = _format_business_keys(row, self.business_keys)

        # Gera hash key de cada business key fornecida via argumento de inicialização de classe
        hashed = []
        for b in bks:
            hashed.append(hashlib.md5(''.join(b).encode('utf-8')).hexdigest())

        # Gera hash do link a partir das hashs criadas pelas business keys
        hash_ = hashlib.md5(''.join(hashed).encode('utf-8')).hexdigest()

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
        bks = _format_business_keys(element, self.business_keys)

        hashed = []
        for b in bks:
            hashed.append(hashlib.md5(''.join(b).encode('utf-8')).hexdigest())

        # Gera hash do link a partir das hashs criadas pelas business keys
        hash_ = hashlib.md5(''.join(hashed).encode('utf-8')).hexdigest()

        return [
                   hash_,
                   time.mktime(datetime.datetime.strptime(self.load_date, "%Y-%m-%d").timetuple()),
                   self.record_source
               ] + [b for sl in bks for b in sl]  # Flattens the list


class DatavaultInsertOperator(BaseOperator):
    """
    """

    template_fields = ('staging_project_dataset_table_id',)

    def __init__(self,
                 bigquery_conn_id,
                 hub_project_dataset_table_id: str,
                 staging_project_dataset_table_id: str,
                 schema: List[str] = None,
                 *args,
                 **kwargs
                 ):
        """
        :param hub_project_dataset_table_id:
            The [project_id]:dataset.table_id which is a Hub
        :param staging_project_dataset_table_id:
            The [project_id]:dataset.table_id which serves as staging for `hub_project_dataset_table_id`
        :param schema:
            The schema of `hub_project_dataset_table_id`. If None, infer from BigQuery itself. The schema must have
            same order of columns as table and hub hash key MUST BE first column.
        :param args:
        :param kwargs:
        """
        super(DatavaultInsertOperator, self).__init__(*args, **kwargs)

        self.bigquery_conn_id = bigquery_conn_id
        self.hub_project_dataset_table_id = hub_project_dataset_table_id
        self.staging_project_dataset_table_id = staging_project_dataset_table_id

        self.schema = schema

        # Creates INSERT INTO CLAUSE

    def execute(self, context):
        # TODO Buscar schema do hub se não for passado schema via argumento

        sql = """
              INSERT INTO `{hub}` ({cols})
              SELECT
                  S.*
              FROM `{staging}` AS S
                  LEFT JOIN `{hub}` AS T
                      ON S.{hash_key} = T.{hash_key}
              WHERE T.{hash_key} IS NULL
              """.format(
            hub=self.hub_project_dataset_table_id,
            staging=self.staging_project_dataset_table_id,
            cols=','.join(self.schema),
            hash_key=self.schema[0]
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
