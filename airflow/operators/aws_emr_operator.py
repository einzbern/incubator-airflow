import getpass
import os
import logging
import os
import re
import time

import boto3
import sqlparse
from TCLIService.ttypes import TOperationState
from pyhive import hive
from pyhive.exc import ProgrammingError
from thrift.transport.TSocket import TSocket

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults, AirflowException


class EMRClient(object):
    def __init__(self, cluster_name, **kwargs):
        # logging.info(kwargs)
        self.cluster_name = cluster_name
        self.hive_conf = kwargs.pop('hive_conf', {})
        self.username = kwargs.pop('username', 'hadoop')
        self.database = kwargs.pop('database', 'default')
        # logging.info("username %s", self.username)

    @property
    def client(self):
        if not hasattr(self, '_client'):
            self._client = boto3.client('emr')
        return self._client

    @property
    def cluster(self):
        if not hasattr(self, '_cluster'):
            clusters = filter(lambda x: x['Name'] == self.cluster_name, self.client.list_clusters(
                ClusterStates=[
                    'STARTING',
                    'RUNNING',
                    'WAITING',
                ],
            )['Clusters'])
            if clusters:
                self._cluster = clusters[0]
            else:
                raise AirflowException('running cluster %s not found', self.cluster_name)
        return self._cluster

    @property
    def dns_name(self):
        if not hasattr(self, '_dns_name'):
            self._dns_name = self.client.describe_cluster(ClusterId=self.cluster['Id'])['Cluster'][
                'MasterPublicDnsName']
        return self._dns_name

    @property
    def hive_conn(self):
        if not hasattr(self, '_hive_conn'):
            logging.info('connecting hive using configuration %s', self.hive_conf)
            self._hive_conn = hive.connect(
                self.dns_name,
                username=self.username,
                database=self.database,
                configuration=self.hive_conf,
            )
        return self._hive_conn


class EMRHiveOperator(BaseOperator):
    template_fields = ('hql',)
    template_ext = ('.hql', '.sql',)
    ui_color = '#c6a5bb'

    @apply_defaults
    def __init__(self, hql, cluster_name, *args, **kwargs):
        # logging.info(kwargs)
        super(EMRHiveOperator, self).__init__(*args, **kwargs)
        try:
            if os.path.isfile(hql):
                self.hql = open(hql).read().decode('utf-8')
            else:
                self.hql = hql.decode('utf-8')
        except TypeError:
            self.hql = hql.decode('utf-8')
        hive_conf = {
            'hive.exec.dynamic.partition.mode': 'nonstrict',
            'hive.exec.max.dynamic.partitions': '5000',
            'hive.exec.compress.output': 'true',
            'avro.output.codec': 'snappy',
            'hive.groupby.orderby.position.alias': 'true',
        }
        hive_conf.update(kwargs.pop('hive_conf', {}))
        self.set_opts = re.findall(r'^[\n\r\s]*set ([a-zA-Z0-9._]+)\s*=([a-zA-Z0-9._]+);', self.hql, re.I | re.M)
        if self.set_opts:
            hive_conf.update(dict(self.set_opts))
        self.emr_client = EMRClient(cluster_name, hive_conf=hive_conf, username=kwargs.get('username', 'hadoop'))
        self.hive_conf = hive_conf
        # self.metrics = {
        #     'time.cost.seconds': None,
        #     'hive.table.etl.totalSize': None,
        #     'hive.table.etl.numRows': None,
        #     'hive.table.etl.numFiles': None,
        # }

    # def collect_metrics(self):
    #     hive_conn = self.emr_client.hive_conn
    #     cursor = hive_conn.cursor()
    #     s = re.search(r'-- @desc_partition (.+?)\n', self.hql)
    #     if s:
    #         query_desc_partition = s.group(1)
    #         cursor.execute(query_desc_partition)
    #         rows = cursor.fetchall()
    #         cols = map(lambda x: x[0], cursor.description)
    #         cursor.close()
    #         desc = dict([(d['col_name'], d['data_type']) for d in [dict(zip(cols, row)) for row in rows]])
    #         table_inf = desc.get('Detailed Table Information') or desc.get('Detailed Partition Information')
    #         self.metrics['hive.table.etl.totalSize'] = re.search(r'totalSize=(\d+)', table_inf).group(1)
    #         self.metrics['hive.table.etl.numRows'] = re.search(r'numRows=(\d+)', table_inf).group(1)
    #         self.metrics['hive.table.etl.numFiles'] = re.search(r'numFiles=(\d+)', table_inf).group(1)

    def execute(self, context):
        logging.info('opts in query %s', self.set_opts)
        t1 = time.time()
        queries = filter(None, map(
            lambda x: x.value.strip('\n\r\t; '),
            sqlparse.parse('\n'.join(filter(lambda x: not x.startswith('--'), self.hql.splitlines()))),
        ))
        cursor = self.emr_client.hive_conn.cursor()
        self.cursor = cursor
        for _query in queries:
            cursor.execute(_query, async=True)
            status = cursor.poll().operationState
            while status in (
                TOperationState.INITIALIZED_STATE,
                TOperationState.RUNNING_STATE,
                TOperationState.PENDING_STATE,
            ):
                for message in cursor.fetch_logs():
                    logging.info(message)
                status = cursor.poll().operationState
            for message in cursor.fetch_logs():
                logging.info(message)
            if status in (
                # TOperationState.FINISHED_STATE,
                TOperationState.CANCELED_STATE,
                TOperationState.CLOSED_STATE,
                TOperationState.ERROR_STATE,
                TOperationState.UKNOWN_STATE,
            ):
                for message in cursor.fetch_logs():
                    logging.error(message)
                raise ProgrammingError(cursor.poll().errorMessage)
        if hasattr(self, 'callback'):
            self.callback(context)
        cursor.close()
        # self.metrics['time.cost.seconds'] = int(time.time() - t1)

    def dry_run(self):
        logging.info('opts in query %s', self.set_opts)
        logging.info(self.hive_conf)
        logging.info(self.hql)

    def on_kill(self):
        self.cursor.cancel()

    # def post_execute(self, context):
    #     if context['test_mode']:
    #         return
    #     try:
    #         self.collect_metrics()
    #         session = settings.Session()
    #         for k, v in self.metrics.items():
    #             if v is not None:
    #                 logging.info('metric %s value %s', k, v)
    #                 session.add(TaskInstanceMetrics(context['ti'], k, v))
    #         session.commit()
    #         session.close()
    #     except:
    #         logging.exception('failed to post_execute')


username = getpass.getuser()
yarn_queue = username == 'data' and 'root.offline' or 'root.adhoc'


class DataEMRHiveOperator(EMRHiveOperator):

    def __init__(self, hql, *args, **kwargs):
        if 'hive_conf' not in kwargs:
            kwargs['hive_conf'] = {}

        kwargs['hive_conf'].update({
            'tez.queue.name': yarn_queue,
            'mapred.job.queue.name': yarn_queue,
        })
        cluster_name = kwargs.pop('cluster_name', None) or 'Data-Hive-ETL'
        super(DataEMRHiveOperator, self).__init__(hql=hql, cluster_name=cluster_name, *args, **kwargs)
