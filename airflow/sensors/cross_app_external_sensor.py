#!/usr/bin/env python
# -*- coding: utf-8 -*-

from airflow.sensors.external_task_sensor import ExternalTaskSensor

from sqlalchemy import create_engine
from airflow import configuration
from sqlalchemy.pool import NullPool
from sqlalchemy.orm import scoped_session, sessionmaker


class CrossAppExternalSensor(ExternalTaskSensor):

    ui_color = '#21441e'

    def __init__(self, read_address, *args, **kwargs):
        super(CrossAppExternalSensor, self).__init__(*args, **kwargs)
        # read_address = configuration.get('sensor_check', 'check_sql_alchemy_conn')
        if not read_address:
            read_address = configuration.get('core', 'sql_alchemy_conn')
        self.read_address = read_address

    def _build_read_session(self):
        read_engine = create_engine(self.read_address, poolclass=NullPool)
        read_session_builder = scoped_session(
            sessionmaker(autocommit=False, autoflush=False, bind=read_engine))
        return read_session_builder()

    def poke(self, context, session=None):
        return super(CrossAppExternalSensor, self).poke(context, self._build_read_session())
