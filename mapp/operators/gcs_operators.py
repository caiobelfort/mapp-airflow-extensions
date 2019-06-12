from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import SkipMixin
from airflow.operators import BaseOperator
from airflow.utils import apply_defaults


class GCSObjectExistsShortCircuitOperator(BaseOperator, SkipMixin):
    """
    Check if a object exists in Cloud Storage, if don't exists skip all downstream tasks.
    """

    template_fields = ('bucket', 'filename')
    ui_color = '#de8ce0'

    @apply_defaults
    def __init__(self,
                 bucket: str,
                 filename: str,
                 google_cloud_storage_conn_id: str = 'google_cloud_default',
                 delegate_to: str = None,
                 *args,
                 **kwargs
                 ):
        """
        :param bucket: Bucket name
        :param filename: Object name in Cloud Storage relative to `bucket`.
        :param google_cloud_storage_conn_id:  Reference to a specific Google Cloud Storage hook.
        :param delegate_to: Account to impersonate
        """
        super(GCSObjectExistsShortCircuitOperator, self).__init__(*args, **kwargs)

        self.bucket = bucket
        self.filename = filename
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        cloud_storage_conn = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                                                    delegate_to=self.delegate_to
                                                    )

        condition = cloud_storage_conn.exists(self.bucket, self.filename)
        self.log.info("Condition result is %s", condition)

        if condition:
            self.log.info('Proceeding with downstream tasks...')
            return

        self.log.info('Skipping downstream tasks...')

        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        self.log.debug("Downstream task_ids %s", downstream_tasks)

        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)

        self.log.info("Done.")
