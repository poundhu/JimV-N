#!/usr/bin/env python
# -*- coding: utf-8 -*-


import commands
import traceback

import jimit as ji
import json

import redis
import time

from models import LogLevel, EmitKind, GuestState, ResponseState, HostEvent
from models import GuestCollectionPerformanceDataKind, HostCollectionPerformanceDataKind


__author__ = 'James Iter'
__date__ = '2017/3/13'
__contact__ = 'james.iter.cn@gmail.com'
__copyright__ = '(c) 2017 by James Iter.'


class Utils(object):

    exit_flag = False
    thread_counter = 0

    @staticmethod
    def shell_cmd(cmd):
        try:
            exit_status, output = commands.getstatusoutput(cmd)

            return exit_status, str(output)

        except Exception as e:
            return -1, e.message

    @classmethod
    def signal_handle(cls, signum=0, frame=None):
        cls.exit_flag = True

    @staticmethod
    def md5(_str):
        import hashlib
        m = hashlib.md5()
        m.update(_str)
        return m.hexdigest()

    @classmethod
    def uuid_by_decimal(cls, _str, _len=16):
        """
        :param _str: 欲哈希成数字的字符串
        :param _len: 返回的位宽
        :return: 指定位宽度的十进制数字
        """
        import string
        return int(string.atoi(cls.md5(_str=_str), 16).__str__()[:_len])


class Emit(object):

    def __init__(self):
        # 初始化时，host_event_report_queue 必须由具体实例来指定
        self.upstream_queue = None
        self.hostname = ji.Common.get_hostname()
        self.node_id = Utils.uuid_by_decimal(_str=self.hostname, _len=16)
        self.r = None

    def emit(self, _kind=None, _type=None, message=None):
        from initialize import logger

        if all([key is None for key in [_kind, _type, message]]):
            logger.warning(u'参数 _kind, _type, message 均不能为None.')
            return False

        msg = json.dumps({'kind': _kind, 'type': _type, 'timestamp': ji.Common.ts(), 'host': self.hostname,
                          'node_id': self.node_id, 'message': message}, ensure_ascii=False)
        try:
            return self.r.rpush(self.upstream_queue, msg)

        except redis.exceptions.ConnectionError as e:
            logger.error(traceback.format_exc())
            # 防止循环线程，在redis连接断开时，混水写入日志
            time.sleep(5)


class LogEmit(Emit):
    def __init__(self):
        super(LogEmit, self).__init__()

    def emit2(self, _type=None, message=None):
        return self.emit(_kind=EmitKind.log.value, _type=_type, message=message)

    def debug(self, msg):
        return self.emit2(_type=LogLevel.debug.value, message=msg)

    def info(self, msg):
        return self.emit2(_type=LogLevel.info.value, message=msg)

    def warn(self, msg):
        return self.emit2(_type=LogLevel.warn.value, message=msg)

    def error(self, msg):
        return self.emit2(_type=LogLevel.error.value, message=msg)

    def critical(self, msg):
        return self.emit2(_type=LogLevel.critical.value, message=msg)


class GuestEventEmit(Emit):
    def __init__(self):
        super(GuestEventEmit, self).__init__()

    def emit2(self, _type=None, uuid=None, os_template_image_id=None, migrating_info=None, xml=None, progress=None):
        return self.emit(_kind=EmitKind.guest_event.value, _type=_type, message={
            'uuid': uuid, 'os_template_image_id': os_template_image_id, 'migrating_info': migrating_info, 'xml': xml,
            'progress': progress})

    def no_state(self, uuid):
        return self.emit2(_type=GuestState.no_state.value, uuid=uuid)

    def running(self, uuid):
        return self.emit2(_type=GuestState.running.value, uuid=uuid)

    def blocked(self, uuid):
        return self.emit2(_type=GuestState.blocked.value, uuid=uuid)

    def paused(self, uuid):
        return self.emit2(_type=GuestState.paused.value, uuid=uuid)

    def shutdown(self, uuid):
        return self.emit2(_type=GuestState.shutdown.value, uuid=uuid)

    def shutoff(self, uuid):
        return self.emit2(_type=GuestState.shutoff.value, uuid=uuid)

    def crashed(self, uuid):
        return self.emit2(_type=GuestState.crashed.value, uuid=uuid)

    def pm_suspended(self, uuid):
        return self.emit2(_type=GuestState.pm_suspended.value, uuid=uuid)

    def migrating(self, uuid, migrating_info):
        return self.emit2(_type=GuestState.migrating.value, uuid=uuid, migrating_info=migrating_info)

    def update(self, uuid, xml):
        return self.emit2(_type=GuestState.update.value, uuid=uuid, xml=xml)

    def creating(self, uuid, progress):
        return self.emit2(_type=GuestState.creating.value, uuid=uuid, progress=progress)

    def snapshot_converting(self, uuid, os_template_image_id, progress):
        return self.emit2(_type=GuestState.snapshot_converting.value, uuid=uuid,
                          os_template_image_id=os_template_image_id, progress=progress)


class HostEventEmit(Emit):
    def __init__(self):
        super(HostEventEmit, self).__init__()

    def emit2(self, _type=None, message=None):
        return self.emit(_kind=EmitKind.host_event.value, _type=_type, message=message)

    def heartbeat(self, message):
        return self.emit2(_type=HostEvent.heartbeat.value, message=message)


class ResponseEmit(Emit):
    def __init__(self):
        super(ResponseEmit, self).__init__()

    def emit2(self, _type=None, _object=None, action=None, uuid=None, data=None, passback_parameters=None):
        return self.emit(_kind=EmitKind.response.value, _type=_type,
                         message={'_object': _object, 'action': action, 'uuid': uuid, 'data': data,
                                  'passback_parameters': passback_parameters})

    def success(self, _object, action, uuid, passback_parameters, data=None):
        return self.emit2(_type=ResponseState.success.value, _object=_object, action=action, uuid=uuid, data=data,
                          passback_parameters=passback_parameters)

    def failure(self, _object, action, uuid, passback_parameters, data=None):
        return self.emit2(_type=ResponseState.failure.value, _object=_object, action=action, uuid=uuid, data=data,
                          passback_parameters=passback_parameters)


class GuestCollectionPerformanceEmit(Emit):
    def __init__(self):
        super(GuestCollectionPerformanceEmit, self).__init__()

    def emit2(self, _type=None, data=None):
        return self.emit(_kind=EmitKind.guest_collection_performance.value, _type=_type,
                         message={'data': data})

    def cpu_memory(self, data=None):
        return self.emit2(_type=GuestCollectionPerformanceDataKind.cpu_memory.value, data=data)

    def traffic(self, data=None):
        return self.emit2(_type=GuestCollectionPerformanceDataKind.traffic.value, data=data)

    def disk_io(self, data=None):
        return self.emit2(_type=GuestCollectionPerformanceDataKind.disk_io.value, data=data)


class HostCollectionPerformanceEmit(Emit):
    def __init__(self):
        super(HostCollectionPerformanceEmit, self).__init__()

    def emit2(self, _type=None, data=None):
        return self.emit(_kind=EmitKind.host_collection_performance.value, _type=_type,
                         message={'data': data})

    def cpu_memory(self, data=None):
        return self.emit2(_type=HostCollectionPerformanceDataKind.cpu_memory.value, data=data)

    def traffic(self, data=None):
        return self.emit2(_type=HostCollectionPerformanceDataKind.traffic.value, data=data)

    def disk_usage_io(self, data=None):
        return self.emit2(_type=HostCollectionPerformanceDataKind.disk_usage_io.value, data=data)

