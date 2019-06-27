from datetime import datetime
from time import mktime
from mapp.operators.mssql_operators import (
    MsSqlToGoogleCloudStorageOperator,
    PartitionedMsSqlToGoogleCloudStorageOperator
)

import unittest

from unittest import mock

TASK_ID = 'test-mssql-to-gcs'
MSSQL_CONN_ID = 'mssql_conn_test'
SQL = 'select 1'
PARTITION_SQL = 'select partition from (VALUES (42), (43), (44)) AS  X("partition")'
BUCKET = 'gs://test'
JSON_FILENAME = 'test_{}.ndjson'
JSON_PARITION_FILENAME = 'test_{}-{}.ndjson'
GZIP = False

DATETIMES = [
    datetime(2018, 10, 1, 10, 11),
    datetime(2018, 10, 1, 14, 12),
    datetime(2018, 11, 12, 12, 3)
]

ROWS = [
    ('mock_row_content_1', 42, DATETIMES[0]),
    ('mock_row_content_2', 43, DATETIMES[1]),
    ('mock_row_content_3', 44, DATETIMES[2])
]
CURSOR_DESCRIPTION = (
    ('some_str', 0, None, None, None, None, None),
    ('some_num', 3, None, None, None, None, None),
    ('some_datetime', 4, None, None, None, None)
)
NDJSON_LINES = [
    b'{"some_datetime": %.1f, "some_num": 42, "some_str": "mock_row_content_1"}\n' % mktime(DATETIMES[0].timetuple()),
    b'{"some_datetime": %.1f, "some_num": 43, "some_str": "mock_row_content_2"}\n' % mktime(DATETIMES[1].timetuple()),
    b'{"some_datetime": %.1f, "some_num": 44, "some_str": "mock_row_content_3"}\n' % mktime(DATETIMES[2].timetuple())
]

SCHEMA_FILENAME = 'schema_test.json'
SCHEMA_JSON = [
    b'[{"mode": "NULLABLE", "name": "some_str", "type": "STRING"}, ',
    b'{"mode": "NULLABLE", "name": "some_num", "type": "INTEGER"}, ',
    b'{"mode": "NULLABLE", "name": "some_datetime", "type": "TIMESTAMP"}]'
]

PARTITION_ROWS = [('partition', 42), ('partition', 43), ('partition', 44)]
PARTITION = [42, 43, 44]


class MsSqlToGoogleCloudStorageOperatorTest(unittest.TestCase):

    def test_init(self):
        """Test MySqlToGoogleCloudStorageOperator instance is properly initialized."""
        op = MsSqlToGoogleCloudStorageOperator(
            task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=JSON_FILENAME)
        self.assertEqual(op.task_id, TASK_ID)
        self.assertEqual(op.sql, SQL)
        self.assertEqual(op.bucket, BUCKET)
        self.assertEqual(op.filename, JSON_FILENAME)

    @mock.patch('mapp.operators.mssql_operators.MsSqlHook')
    @mock.patch('mapp.operators.mssql_operators.GoogleCloudStorageHook')
    def test_exec_success_json(self, gcs_hook_mock_class, mssql_hook_mock_class):
        """Test successful run of execute function for JSON"""
        op = MsSqlToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            mssql_conn_id=MSSQL_CONN_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=JSON_FILENAME)

        mssql_hook_mock = mssql_hook_mock_class.return_value
        mssql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mssql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type=None, gzip=False):
            self.assertEqual(BUCKET, bucket)
            self.assertEqual(JSON_FILENAME.format(0), obj)
            self.assertEqual('application/json', mime_type)
            self.assertEqual(GZIP, gzip)
            with open(tmp_filename, 'rb') as file:
                self.assertEqual(b''.join(NDJSON_LINES), file.read())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op.execute(None)

        mssql_hook_mock_class.assert_called_once_with(mssql_conn_id=MSSQL_CONN_ID)
        mssql_hook_mock.get_conn().cursor().execute.assert_called_once_with(SQL)

    @mock.patch('mapp.operators.mssql_operators.MsSqlHook')
    @mock.patch('mapp.operators.mssql_operators.GoogleCloudStorageHook')
    def test_file_splitting(self, gcs_hook_mock_class, mssql_hook_mock_class):
        """Test that ndjson is split by approx_max_file_size_bytes param."""
        mssql_hook_mock = mssql_hook_mock_class.return_value
        mssql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mssql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value
        expected_upload = {
            JSON_FILENAME.format(0): b''.join(NDJSON_LINES[:2]),
            JSON_FILENAME.format(1): NDJSON_LINES[2],
            JSON_FILENAME.format(3): b''
        }

        def _assert_upload(bucket, obj, tmp_filename, mime_type=None, gzip=False):
            self.assertEqual(BUCKET, bucket)
            self.assertEqual('application/json', mime_type)
            self.assertEqual(GZIP, gzip)
            with open(tmp_filename, 'rb') as file:
                self.assertEqual(expected_upload[obj], file.read())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = MsSqlToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=JSON_FILENAME,
            approx_max_file_size_bytes=len(expected_upload[JSON_FILENAME.format(0)]))
        op.execute(None)

        # Only two files has data, send only that two
        self.assertEqual(2, gcs_hook_mock.upload.call_count)

    @mock.patch('mapp.operators.mssql_operators.MsSqlHook')
    @mock.patch('mapp.operators.mssql_operators.GoogleCloudStorageHook')
    def test_schema_file(self, gcs_hook_mock_class, mssql_hook_mock_class):
        """Test writing schema files."""
        mssql_hook_mock = mssql_hook_mock_class.return_value
        mssql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mssql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip):
            if obj == SCHEMA_FILENAME:
                with open(tmp_filename, 'rb') as file:
                    self.assertEqual(b''.join(SCHEMA_JSON), file.read())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = MsSqlToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=JSON_FILENAME,
            schema_filename=SCHEMA_FILENAME
        )

        op.execute(None)

        self.assertEqual(2, gcs_hook_mock.upload.call_count)




