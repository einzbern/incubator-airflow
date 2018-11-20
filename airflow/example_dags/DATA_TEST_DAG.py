#!/usr/bin/env python
# -*- coding: utf-8 -*-
from airflow import configuration
from datetime import datetime, timedelta
import os

from airflow.models import DAG
from airflow.operators.aws_emr_operator import  DataEMRHiveOperator
from airflow.sensors.cross_app_external_sensor import CrossAppExternalSensor


dag_home = os.path.join(configuration.get('core', 'airflow_home'), 'dags/hive')

args = {
    'owner': 'DataTeam Alpha',
    'email': [
        'wenhaoxu@xiaohongshu.com',
    ],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_interval': timedelta(minutes=5),
    'schedule_interval': timedelta(1),
    'start_date': datetime(2018, 11, 11),
}

dag = DAG(
    dag_id='DATA_TEST_DAG',
    schedule_interval=timedelta(1),
    default_args=args,
    concurrency=3,
)

for hive_query_file in [
    os.path.join(dag_home, 'new_ariflow_operator_test.hql')
]:
    task_id = os.path.splitext(os.path.basename(hive_query_file))[0]
    locals()[task_id] = DataEMRHiveOperator(
        hql=hive_query_file,
        task_id=task_id,
        dag=dag,
    )


for task, external_dag_id, external_task_id in [
    ('new_ariflow_operator_test', 'example_bash_operator', 'run_this_last'),
]:
    if not isinstance(external_task_id, (tuple, list)):
        external_task_id = [external_task_id]
    if external_dag_id is None:
        locals()[task_id].set_upstream([locals()[_t] for _t in external_task_id])
    else:
        for _t in external_task_id:
            sensor_task_id = 'sensor.%s.%s' % (external_dag_id, _t)
            if locals().get(sensor_task_id):
                locals()[task_id].set_upstream(locals()[sensor_task_id])
            else:
                locals()[sensor_task_id] = CrossAppExternalSensor(
                    external_dag_id=external_dag_id,
                    external_task_id=_t,
                    dag=dag,
                    task_id=sensor_task_id,
                    read_address=configuration.get('sensor_check', 'check_sql_alchemy_conn')

                )
                locals()[task_id].set_upstream(locals()[sensor_task_id])

