from typing import Iterable

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class SqlSensorV2(BaseSensorOperator):
    """
    This is an extension of existing SqlSensor class which support running a python_callable function once 
    sensor get expected value.

    This is to overcome the issue that maximum concurrent tasks are queued and just waiting for the result
    of python_callable function.
    """

    template_fields = ('sql',)  # type: Iterable[str]
    template_ext = ('.hql', '.sql',)  # type: Iterable[str]
    ui_color = '#f5f39d'

    @apply_defaults
    def __init__(self, 
                conn_id, 
                sql, 
                python_callable=None,
                parameters=None, 
                success=None, 
                failure=None, 
                fail_on_empty=False,
                 *args, **kwargs):
        self.conn_id = conn_id
        self.sql = sql
        self.python_callable = python_callable
        self.parameters = parameters
        self.success = success
        self.failure = failure
        self.fail_on_empty = fail_on_empty
        super().__init__(*args, **kwargs)

    def _get_hook(self):
        conn = BaseHook.get_connection(self.conn_id)

        allowed_conn_type = {'google_cloud_platform', 'jdbc', 'mssql',
                             'mysql', 'odbc', 'oracle', 'postgres',
                             'presto', 'sqlite', 'vertica'}
        if conn.conn_type not in allowed_conn_type:
            raise AirflowException("The connection type is not supported by SqlSensor. " +
                                   "Supported connection types: {}".format(list(allowed_conn_type)))
        return conn.get_hook()

    def poke(self, context):
        hook = self._get_hook()

        self.log.info('Poking: %s (with parameters %s)', self.sql, self.parameters)
        records = hook.get_records(self.sql, self.parameters)
        if not records:
            if self.fail_on_empty:
                raise AirflowException("No rows returned, raising as per fail_on_empty flag")
            else:
                return False
        first_cell = records[0][0]
        if self.failure is not None:
            if callable(self.failure):
                if self.failure(first_cell):
                    raise AirflowException(
                        "Failure criteria met. self.failure({}) returned True".format(first_cell))
            else:
                raise AirflowException("self.failure is present, but not callable -> {}".format(self.success))
        if self.success is not None:
            if callable(self.success):
                return self.success(first_cell)
            else:
                raise AirflowException("self.success is present, but not callable -> {}".format(self.success))
        
        # Run py function on success result return
        if bool(first_cell):
            self.python_callable()

        return bool(first_cell)