#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import shutil
import traceback
import jimit as ji

import guestfs
import libvirt
import threading
from gluster import gfapi
import xml.etree.ElementTree as ET
import libvirt_qemu
import json

from initialize import logger, log_emit, guest_event_emit, q_creating_guest, response_emit
from models.status import OSTemplateInitializeOperateKind, StorageMode
from disk import Disk


__author__ = 'James Iter'
__date__ = '2017/3/1'
__contact__ = 'james.iter.cn@gmail.com'
__copyright__ = '(c) 2017 by James Iter.'


class Guest(object):
    jimv_edition = None
    storage_mode = None
    gf = None
    dfs_volume = None
    thread_mutex_lock = threading.Lock()

    def __init__(self, **kwargs):
        self.uuid = kwargs.get('uuid', None)
        self.name = kwargs.get('name', None)
        self.password = kwargs.get('password', None)
        # 模板镜像路径
        self.template_path = kwargs.get('template_path', None)
        # 不提供链接克隆(完整克隆，后期可以在模板目录，直接删除模板文件。从理论上讲，基于完整克隆的 Guest 读写速度、快照都应该快于链接克隆。)
        # self.clone = True
        # Guest 系统盘及数据磁盘
        self.disk = kwargs.get('disk', None)
        self.xml = kwargs.get('xml', None)
        # Guest 系统镜像路径，不包含 dfs 卷标
        self.system_image_path = None
        self.g = guestfs.GuestFS(python_return_dict=True)

    @classmethod
    def init_gfapi(cls):
        cls.thread_mutex_lock.acquire()

        if cls.gf is None:
            cls.gf = gfapi.Volume('127.0.0.1', cls.dfs_volume)
            cls.gf.mount()

        cls.thread_mutex_lock.release()

        return cls.gf

    def generate_system_image(self):
        if self.storage_mode in [StorageMode.ceph.value, StorageMode.glusterfs.value]:
            if self.storage_mode == StorageMode.glusterfs.value:
                if not self.gf.isfile(self.template_path):
                    log = u' '.join([u'域', self.name, u', UUID', self.uuid, u'所依赖的模板', self.template_path, u'不存在.'])
                    logger.error(msg=log)
                    log_emit.error(msg=log)
                    return False

                if not self.gf.isdir(os.path.dirname(self.system_image_path)):
                    self.gf.makedirs(os.path.dirname(self.system_image_path), 0755)

                self.gf.copyfile(self.template_path, self.system_image_path)

        elif self.storage_mode in [StorageMode.local.value, StorageMode.shared_mount.value]:
            if not os.path.exists(self.template_path) or not os.path.isfile(self.template_path):
                log = u' '.join([u'域', self.name, u', UUID', self.uuid, u'所依赖的模板', self.template_path, u'不存在.'])
                logger.error(msg=log)
                log_emit.error(msg=log)
                return False

            if not os.access(self.template_path, os.R_OK):
                log = u' '.join([u'域', self.name, u', UUID', self.uuid, u'所依赖的模板', self.template_path, u'无权访问.'])
                logger.error(msg=log)
                log_emit.error(msg=log)
                return False

            system_image_path_dir = os.path.dirname(self.system_image_path)

            if not os.path.exists(system_image_path_dir):
                os.makedirs(system_image_path_dir, 0755)

            elif not os.path.isdir(system_image_path_dir):
                os.rename(system_image_path_dir, system_image_path_dir + '.bak')
                os.makedirs(system_image_path_dir, 0755)

            shutil.copyfile(self.template_path, self.system_image_path)

        else:
            raise ValueError('Unknown value of storage_mode.')

        return True

    def execute_os_template_initialize_operates(self, guest=None, os_template_initialize_operates=None, os_type=None):
        if not isinstance(os_template_initialize_operates, list):
            raise ValueError('The os_template_initialize_operates must be a list.')

        if os_template_initialize_operates.__len__() < 1:
            return True

        self.xml = guest.XMLDesc()
        root = ET.fromstring(self.xml)

        if self.storage_mode in [StorageMode.ceph.value, StorageMode.glusterfs.value]:
            for dev in root.findall('devices/disk'):
                filename = dev.find('source').get('name')
                _format = dev.find('driver').attrib['type']
                protocol = dev.find('source').get('protocol')
                server = dev.find('source/host').get('name')
                self.g.add_drive(filename=filename, format=_format, protocol=protocol, server=[server])

        elif self.storage_mode in [StorageMode.local.value, StorageMode.shared_mount.value]:
            for dev in root.findall('devices/disk'):
                filename = dev.find('source').get('file')
                _format = dev.find('driver').attrib['type']
                self.g.add_drive(filename=filename, format=_format, protocol='file')

        self.g.launch()
        self.g.mount(self.g.inspect_os()[0], '/')

        for os_template_initialize_operate in os_template_initialize_operates:
            if os_template_initialize_operate['kind'] == OSTemplateInitializeOperateKind.cmd.value:

                if str(os_type).lower().find('windows') >= 0:
                    continue

                self.g.sh(os_template_initialize_operate['command'])

            elif os_template_initialize_operate['kind'] == OSTemplateInitializeOperateKind.write_file.value:

                content = os_template_initialize_operate['content']
                if str(os_type).lower().find('windows') >= 0:
                    content = content.replace('\r', '').replace('\n', '\r\n')

                self.g.write(os_template_initialize_operate['path'], content)

            elif os_template_initialize_operate['kind'] == OSTemplateInitializeOperateKind.append_file.value:

                content = os_template_initialize_operate['content']
                if str(os_type).lower().find('windows') >= 0:
                    content = content.replace('\r', '').replace('\n', '\r\n')

                self.g.write_append(os_template_initialize_operate['path'], content)

            else:
                continue

        self.g.shutdown()
        self.g.close()

        return True

    def define_by_xml(self, conn=None):
        try:
            if conn.defineXML(xml=self.xml):
                log = u' '.join([u'域', self.name, u', UUID', self.uuid, u'定义成功.'])
                logger.info(msg=log)
                log_emit.info(msg=log)
            else:
                log = u' '.join([u'域', self.name, u', UUID', self.uuid, u'定义时未预期返回.'])
                logger.info(msg=log)
                log_emit.info(msg=log)
                return False

        except libvirt.libvirtError as e:
            logger.error(e.message)
            log_emit.error(e.message)
            return False

        return True

    def start_by_uuid(self, conn=None):
        try:
            domain = conn.lookupByUUIDString(uuidstr=self.uuid)
            domain.create()
            log = u' '.join([u'域', self.name, u', UUID', self.uuid, u'启动成功.'])
            logger.info(msg=log)
            log_emit.info(msg=log)

        except libvirt.libvirtError as e:
            logger.error(e.message)
            log_emit.error(e.message)
            return False

        return True

    @staticmethod
    def guest_state_report(guest):

        try:
            _uuid = guest.UUIDString()
            state, maxmem, mem, ncpu, cputime = guest.info()
            # state 参考链接：
            # http://libvirt.org/docs/libvirt-appdev-guide-python/en-US/html/libvirt_application_development_guide_using_python-Guest_Domains-Information-State.html
            # http://stackoverflow.com/questions/4986076/alternative-to-virsh-libvirt

            log = u' '.join([u'域', guest.name(), u', UUID', _uuid, u'的状态改变为'])

            if state == libvirt.VIR_DOMAIN_RUNNING:
                log += u' Running。'
                guest_event_emit.running(uuid=_uuid)

            elif state == libvirt.VIR_DOMAIN_BLOCKED:
                log += u' Blocked。'
                guest_event_emit.blocked(uuid=_uuid)

            elif state == libvirt.VIR_DOMAIN_PAUSED:
                log += u' Paused。'
                guest_event_emit.paused(uuid=_uuid)

            elif state == libvirt.VIR_DOMAIN_SHUTDOWN:
                log += u' Shutdown。'
                guest_event_emit.shutdown(uuid=_uuid)

            elif state == libvirt.VIR_DOMAIN_SHUTOFF:
                log += u' Shutoff。'
                guest_event_emit.shutoff(uuid=_uuid)

            elif state == libvirt.VIR_DOMAIN_CRASHED:
                log += u' Crashed。'
                guest_event_emit.crashed(uuid=_uuid)

            elif state == libvirt.VIR_DOMAIN_PMSUSPENDED:
                log += u' PM_Suspended。'
                guest_event_emit.pm_suspended(uuid=_uuid)

            else:
                log += u' NO_State。'

                guest_event_emit.no_state(uuid=_uuid)

            logger.info(log)
            log_emit.info(log)

        except Exception as e:
            logger.error(e.message)
            log_emit.error(e.message)

    @staticmethod
    def update_xml(guest):
        xml = guest.XMLDesc(flags=libvirt.VIR_DOMAIN_XML_SECURE)
        if xml is None:
            return

        else:
            guest_event_emit.update(uuid=guest.UUIDString(), xml=xml)

    @classmethod
    def create(cls, conn, msg):

        try:
            Guest.storage_mode = msg['storage_mode']

            guest = Guest(uuid=msg['uuid'], name=msg['name'], template_path=msg['template_path'],
                          disk=msg['disks'][0], xml=msg['xml'])

            if Guest.storage_mode == StorageMode.glusterfs.value:
                Guest.dfs_volume = msg['dfs_volume']
                Guest.init_gfapi()

            guest.system_image_path = guest.disk['path']

            q_creating_guest.put({
                'storage_mode': Guest.storage_mode,
                'dfs_volume': Guest.dfs_volume,
                'uuid': guest.uuid,
                'template_path': guest.template_path,
                'system_image_path': guest.system_image_path
            })

            if not guest.generate_system_image():
                raise RuntimeError('System image generate failure.')

            if not guest.define_by_xml(conn=conn):
                raise RuntimeError('Define the instance of virtual machine by xml failure.')

            guest_event_emit.creating(uuid=guest.uuid, progress=92)

            disk_info = dict()

            if Guest.storage_mode == StorageMode.glusterfs.value:
                disk_info = Disk.disk_info_by_glusterfs(dfs_volume=guest.dfs_volume,
                                                        image_path=guest.system_image_path)

            elif Guest.storage_mode in [StorageMode.local.value, StorageMode.shared_mount.value]:
                disk_info = Disk.disk_info_by_local(image_path=guest.system_image_path)

            # 由该线程最顶层的异常捕获机制，处理其抛出的异常
            guest.execute_os_template_initialize_operates(
                guest=conn.lookupByUUIDString(uuidstr=guest.uuid),
                os_template_initialize_operates=msg['os_template_initialize_operates'], os_type=msg['os_type'])

            extend_data = dict()
            extend_data.update({'disk_info': disk_info})

            guest_event_emit.creating(uuid=guest.uuid, progress=97)

            if not guest.start_by_uuid(conn=conn):
                raise RuntimeError('Start the instance of virtual machine by uuid failure.')

            cls.quota(guest=conn.lookupByUUIDString(uuidstr=guest.uuid), msg=msg)

            response_emit.success(_object=msg['_object'], action=msg['action'], uuid=msg['uuid'],
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

        except:
            logger.error(traceback.format_exc())
            log_emit.error(traceback.format_exc())
            response_emit.failure(_object=msg['_object'], action=msg.get('action'), uuid=msg.get('uuid'),
                                  passback_parameters=msg.get('passback_parameters'))

    @staticmethod
    def quota(guest=None, msg=None):
        assert isinstance(guest, libvirt.virDomain)
        assert isinstance(msg, dict)

        for disk in msg['disks']:
            libvirt_qemu.qemuMonitorCommand(guest, json.dumps({
                    'execute': 'block_set_io_throttle',
                    'arguments': {
                        'device': 'drive-virtio-disk' + str(disk['sequence']),
                        'iops': int(disk['iops']),
                        'iops_rd': int(disk['iops_rd']),
                        'iops_wr': int(disk['iops_wr']),
                        'iops_max': int(disk['iops_max']),
                        'iops_max_length': int(disk['iops_max_length']),
                        'bps': int(disk['bps']),
                        'bps_rd': int(disk['bps_rd']),
                        'bps_wr': int(disk['bps_wr']),
                        'bps_max': int(disk['bps_max']),
                        'bps_max_length': int(disk['bps_max_length'])
                    }
                }),
                libvirt_qemu.VIR_DOMAIN_QEMU_MONITOR_COMMAND_DEFAULT)

    @staticmethod
    def update_ssh_key(guest=None, msg=None):
        assert isinstance(guest, libvirt.virDomain)
        assert isinstance(msg, dict)

        libvirt_qemu.qemuAgentCommand(guest, json.dumps({
                'execute': 'guest-exec',
                'arguments': {
                    'path': 'mkdir',
                    'capture-output': False,
                    'arg': [
                        '-p',
                        '/root/.ssh'
                    ]
                }
            }),
            3,
            libvirt_qemu.VIR_DOMAIN_QEMU_AGENT_COMMAND_NOWAIT)

        redirection_symbol = '>'

        ret_s = list()

        for i, ssh_key in enumerate(msg['ssh_keys']):
            if i > 0:
                redirection_symbol = '>>'

            ret = libvirt_qemu.qemuAgentCommand(guest, json.dumps({
                    'execute': 'guest-exec',
                    'arguments': {
                        'path': '/bin/sh',
                        'capture-output': True,
                        'arg': [
                            '-c',
                            ' '.join(['echo', '"' + ssh_key + '"', redirection_symbol, '/root/.ssh/authorized_keys'])
                        ]
                    }
                }),
                3,
                libvirt_qemu.VIR_DOMAIN_QEMU_AGENT_COMMAND_NOWAIT)

            ret = json.loads(ret)

            ret = libvirt_qemu.qemuAgentCommand(guest, json.dumps({
                    'execute': 'guest-exec-status',
                    'arguments': {
                        'pid': ret['return']['pid']
                    }
                }),
                3,
                libvirt_qemu.VIR_DOMAIN_QEMU_AGENT_COMMAND_NOWAIT)

            ret_s.append(json.loads(ret))

        return ret_s

    @staticmethod
    def create_snapshot(guest=None, msg=None):
        extend_data = dict()

        try:
            assert isinstance(guest, libvirt.virDomain)
            assert isinstance(msg, dict)
            snap_xml = """
                <domainsnapshot>
                </domainsnapshot>
            """

            snap_flags = 0
            snap_flags |= libvirt.VIR_DOMAIN_SNAPSHOT_CREATE_ATOMIC

            ret = guest.snapshotCreateXML(xmlDesc=snap_xml, flags=snap_flags)

            parent_id = ''

            try:
                parent = ret.getParent()
                parent_id = parent.getName()

            except libvirt.libvirtError, e:
                if e.get_error_code() == libvirt.VIR_ERR_NO_DOMAIN_SNAPSHOT:
                    parent_id = '-'

            extend_data.update({'snapshot_id': ret.getName(), 'parent_id': parent_id, 'xml': ret.getXMLDesc()})

            response_emit.success(_object=msg['_object'], action=msg['action'], uuid=msg['uuid'],
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

        except:
            logger.error(traceback.format_exc())
            log_emit.error(traceback.format_exc())
            response_emit.failure(_object=msg['_object'], action=msg.get('action'), uuid=msg.get('uuid'),
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

    @staticmethod
    def delete_snapshot(guest=None, msg=None):
        extend_data = dict()

        try:
            assert isinstance(guest, libvirt.virDomain)
            assert isinstance(msg, dict)

            snapshot = guest.snapshotLookupByName(name=msg['snapshot_id'])
            snapshot.delete()

            response_emit.success(_object=msg['_object'], action=msg['action'], uuid=msg['uuid'],
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

        except:
            logger.error(traceback.format_exc())
            log_emit.error(traceback.format_exc())
            response_emit.failure(_object=msg['_object'], action=msg.get('action'), uuid=msg.get('uuid'),
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

    @staticmethod
    def revert_snapshot(guest=None, msg=None):
        extend_data = dict()

        try:
            assert isinstance(guest, libvirt.virDomain)
            assert isinstance(msg, dict)

            snap_flags = 0
            snap_flags |= libvirt.VIR_DOMAIN_SNAPSHOT_REVERT_FORCE
            snapshot = guest.snapshotLookupByName(name=msg['snapshot_id'])

            try:
                guest.revertToSnapshot(snap=snapshot, flags=0)

            except libvirt.libvirtError, e:
                # 给予一次重新恢复的机会
                if e.get_error_code() == libvirt.VIR_ERR_SYSTEM_ERROR:
                    guest.revertToSnapshot(snap=snapshot, flags=snap_flags)

            # 如果恢复后的 Guest 为 Running 状态，则同步其系统时间。
            if guest.isActive():
                # https://qemu.weilnetz.de/doc/qemu-ga-ref.html#index-guest_002dset_002dtime
                try:
                    libvirt_qemu.qemuAgentCommand(guest, json.dumps({
                            'execute': 'guest-set-time',
                            'arguments': {
                                'time': int(ji.Common.ts() * (10**9))
                            }
                        }),
                        3,
                        libvirt_qemu.VIR_DOMAIN_QEMU_AGENT_COMMAND_NOWAIT)

                except libvirt.libvirtError, e:
                    logger.error(e.message)
                    log_emit.error(e.message)

            response_emit.success(_object=msg['_object'], action=msg['action'], uuid=msg['uuid'],
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

        except:
            logger.error(traceback.format_exc())
            log_emit.error(traceback.format_exc())
            response_emit.failure(_object=msg['_object'], action=msg.get('action'), uuid=msg.get('uuid'),
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

