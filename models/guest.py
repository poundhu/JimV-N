#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import traceback
import jimit as ji
import subprocess
import signal
import time
import re
import fcntl

import guestfs
import libvirt
import xml.etree.ElementTree as ET
import libvirt_qemu
import json
import base64

from initialize import log_emit, guest_event_emit, q_creating_guest, response_emit
from models.jimvn_exception import CommandExecFailed
from models.status import OSTemplateInitializeOperateKind, StorageMode
from models.utils import Utils
from models.storage import Storage
from models import GuestState


__author__ = 'James Iter'
__date__ = '2017/3/1'
__contact__ = 'james.iter.cn@gmail.com'
__copyright__ = '(c) 2017 by James Iter.'


class Guest(object):
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
        self.system_image_path = self.disk['path']
        self.g = guestfs.GuestFS(python_return_dict=True)
        self.storage = Storage(storage_mode=kwargs.get('storage_mode', None), dfs_volume=kwargs.get('dfs_volume', None))

    def generate_system_image(self):
        self.storage.copy_file(src=self.template_path, dst=self.system_image_path)

    def define_by_xml(self, conn=None):
        return conn.defineXML(xml=self.xml)

    def execute_os_template_initialize_operates(self, dom=None, os_template_initialize_operates=None, os_type=None):
        assert isinstance(dom, libvirt.virDomain)

        if not isinstance(os_template_initialize_operates, list):
            raise ValueError('The os_template_initialize_operates must be a list.')

        if os_template_initialize_operates.__len__() < 1:
            return True

        is_windows = False

        if str(os_type).lower().find('windows') >= 0:
            is_windows = True

        self.xml = dom.XMLDesc()
        root = ET.fromstring(self.xml)

        for dev in root.findall('devices/disk'):
            filename = dev.find('source').get('file')
            _format = dev.find('driver').attrib['type']

            if filename is None:
                filename = dev.find('source').get('name')
                protocol = dev.find('source').get('protocol')
                server = dev.find('source/host').get('name')
                self.g.add_drive(filename=filename, format=_format, protocol=protocol, server=[server])

            else:
                self.g.add_drive(filename=filename, format=_format, protocol='file')

        self.g.launch()
        self.g.mount(self.g.inspect_os()[0], '/')

        for os_template_initialize_operate in os_template_initialize_operates:
            if os_template_initialize_operate['kind'] == OSTemplateInitializeOperateKind.cmd.value:

                # 暂不支持 Windows 命令
                if is_windows:
                    continue

                self.g.sh(os_template_initialize_operate['command'])

            elif os_template_initialize_operate['kind'] == OSTemplateInitializeOperateKind.write_file.value:

                content = os_template_initialize_operate['content']
                if is_windows:
                    content = content.replace('\r', '').replace('\n', '\r\n')

                self.g.write(os_template_initialize_operate['path'], content)

            elif os_template_initialize_operate['kind'] == OSTemplateInitializeOperateKind.append_file.value:

                content = os_template_initialize_operate['content']
                if is_windows:
                    content = content.replace('\r', '').replace('\n', '\r\n')

                self.g.write_append(os_template_initialize_operate['path'], content)

            else:
                continue

        self.g.shutdown()
        self.g.close()

        return True

    def start_by_uuid(self, conn=None):
        dom = conn.lookupByUUIDString(uuidstr=self.uuid)
        dom.create()

    @staticmethod
    def get_state(dom=None):
        def is_running(_dom=None):
            running = False

            try:
                exec_ret = libvirt_qemu.qemuAgentCommand(_dom, json.dumps({
                                'execute': 'guest-ping',
                                'arguments': {
                                }
                            }),
                            3,
                            libvirt_qemu.VIR_DOMAIN_QEMU_AGENT_COMMAND_NOWAIT)

                running = True

            except:
                pass

            return running

        assert isinstance(dom, libvirt.virDomain)

        _uuid = dom.UUIDString()
        state, maxmem, mem, ncpu, cputime = dom.info()
        # state 参考链接：
        # http://libvirt.org/docs/libvirt-appdev-guide-python/en-US/html/libvirt_application_development_guide_using_python-Guest_Domains-Information-State.html
        # http://stackoverflow.com/questions/4986076/alternative-to-virsh-libvirt

        if state == libvirt.VIR_DOMAIN_RUNNING:

            if is_running(_dom=dom):
                state = GuestState.running.value

            else:
                state = GuestState.booting.value

        elif state == libvirt.VIR_DOMAIN_BLOCKED:
            state = GuestState.blocked.value

        elif state == libvirt.VIR_DOMAIN_PAUSED:
            state = GuestState.paused.value

        elif state == libvirt.VIR_DOMAIN_SHUTDOWN:
            state = GuestState.shutdown.value

        elif state == libvirt.VIR_DOMAIN_SHUTOFF:
            state = GuestState.shutoff.value

        elif state == libvirt.VIR_DOMAIN_CRASHED:
            state = GuestState.crashed.value

        elif state == libvirt.VIR_DOMAIN_PMSUSPENDED:
            state = GuestState.pm_suspended.value

        else:
            state = GuestState.no_state.value

        return state

    @staticmethod
    def guest_state_report(dom=None):
        try:
            _uuid = dom.UUIDString()
            state = Guest.get_state(dom=dom)

            log = u' '.join([u'域', dom.name(), u', UUID', _uuid, u'的状态改变为'])

            if state == GuestState.running.value:
                log += u' Running。'
                guest_event_emit.running(uuid=_uuid)

            elif state == GuestState.booting.value:
                log += u' Booting。'
                guest_event_emit.booting(uuid=_uuid)

            elif state == GuestState.blocked.value:
                log += u' Blocked。'
                guest_event_emit.blocked(uuid=_uuid)

            elif state == GuestState.paused.value:
                log += u' Paused。'
                guest_event_emit.paused(uuid=_uuid)

            elif state == GuestState.shutdown.value:
                log += u' Shutdown。'
                guest_event_emit.shutdown(uuid=_uuid)

            elif state == GuestState.shutoff.value:
                log += u' Shutoff。'
                guest_event_emit.shutoff(uuid=_uuid)

            elif state == GuestState.crashed.value:
                log += u' Crashed。'
                guest_event_emit.crashed(uuid=_uuid)

            elif state == GuestState.pm_suspended.value:
                log += u' PM_Suspended。'
                guest_event_emit.pm_suspended(uuid=_uuid)

            else:
                log += u' NO_State。'

                guest_event_emit.no_state(uuid=_uuid)

            log_emit.info(log)

        except Exception as e:
            log_emit.warn(e.message)

    @staticmethod
    def update_xml(dom):
        assert isinstance(dom, libvirt.virDomain)

        xml = dom.XMLDesc(flags=libvirt.VIR_DOMAIN_XML_SECURE)
        if xml is None:
            return

        else:
            guest_event_emit.update(uuid=dom.UUIDString(), xml=xml)

    @staticmethod
    def create(conn, msg):
        try:
            guest = Guest(uuid=msg['uuid'], name=msg['name'], template_path=msg['template_path'], disk=msg['disks'][0],
                          xml=msg['xml'], storage_mode=msg['storage_mode'], dfs_volume=msg['dfs_volume'])

            q_creating_guest.put({
                'storage_mode': guest.storage.storage_mode,
                'dfs_volume': guest.storage.dfs_volume,
                'uuid': guest.uuid,
                'template_path': guest.template_path,
                'system_image_path': guest.system_image_path
            })

            guest.generate_system_image()

            dom = guest.define_by_xml(conn=conn)
            assert isinstance(dom, libvirt.virDomain)

            log = u' '.join([u'域', guest.name, u', UUID', guest.uuid, u'定义成功.'])
            log_emit.info(msg=log)

            guest_event_emit.creating(uuid=guest.uuid, progress=92)

            disk_info = guest.storage.image_info(path=guest.system_image_path)

            # 由该线程最顶层的异常捕获机制，处理其抛出的异常
            guest.execute_os_template_initialize_operates(
                dom=conn.lookupByUUIDString(uuidstr=guest.uuid),
                os_template_initialize_operates=msg['os_template_initialize_operates'], os_type=msg['os_type'])

            extend_data = dict()
            extend_data.update({'disk_info': disk_info})

            guest_event_emit.creating(uuid=guest.uuid, progress=97)

            dom.create()
            log = u' '.join([u'域', guest.name, u', UUID', guest.uuid, u'启动成功.'])
            log_emit.info(msg=log)

            Guest.quota(dom=dom, msg=msg)

            response_emit.success(_object=msg['_object'], action=msg['action'], uuid=msg['uuid'],
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

        except:
            log_emit.error(traceback.format_exc())
            response_emit.failure(_object=msg['_object'], action=msg.get('action'), uuid=msg.get('uuid'),
                                  passback_parameters=msg.get('passback_parameters'))

    @classmethod
    def reboot(cls, dom=None):
        assert isinstance(dom, libvirt.virDomain)
        # https://libvirt.org/html/libvirt-libvirt-domain.html#virDomainReboot
        dom.reboot()

    @classmethod
    def force_reboot(cls, dom=None, msg=None):
        assert isinstance(dom, libvirt.virDomain)
        assert isinstance(msg, dict)

        dom.destroy()
        dom.create()
        cls.quota(dom=dom, msg=msg)

    @classmethod
    def shutdown(cls, dom=None):
        assert isinstance(dom, libvirt.virDomain)
        # https://libvirt.org/html/libvirt-libvirt-domain.html#virDomainShutdown
        dom.shutdown()

    @classmethod
    def force_shutdown(cls, dom=None):
        assert isinstance(dom, libvirt.virDomain)
        # https://libvirt.org/html/libvirt-libvirt-domain.html#virDomainDestroy
        dom.destroy()

    @classmethod
    def boot(cls, dom=None, msg=None):
        assert isinstance(dom, libvirt.virDomain)
        assert isinstance(msg, dict)

        if not dom.isActive():

            # https://libvirt.org/html/libvirt-libvirt-domain.html#virDomainCreate
            dom.create()

            cls.quota(dom=dom, msg=msg)

    @classmethod
    def suspend(cls, dom=None):
        assert isinstance(dom, libvirt.virDomain)
        # https://libvirt.org/html/libvirt-libvirt-domain.html#virDomainSuspend
        dom.suspend()

    @classmethod
    def resume(cls, dom=None):
        assert isinstance(dom, libvirt.virDomain)
        # https://libvirt.org/html/libvirt-libvirt-domain.html#virDomainResume
        dom.resume()

    @classmethod
    def delete(cls, dom=None, msg=None):
        assert isinstance(dom, libvirt.virDomain)
        assert isinstance(msg, dict)

        root = ET.fromstring(dom.XMLDesc())

        if dom.isActive():
            dom.destroy()

        dom.undefine()

        system_image = None
        dfs_volume = None
        path = None

        for _disk in root.findall('devices/disk'):
            if 'vda' == _disk.find('target').get('dev'):
                system_image = _disk

        if msg['storage_mode'] in [StorageMode.ceph.value, StorageMode.glusterfs.value]:
            # 签出系统镜像路径
            path_list = system_image.find('source').attrib['name'].split('/')

            if msg['storage_mode'] == StorageMode.glusterfs.value:
                dfs_volume = path_list[0]
                path = '/'.join(path_list[1:])

        elif msg['storage_mode'] in [StorageMode.local.value, StorageMode.shared_mount.value]:
            path = system_image.find('source').attrib['file']

        Storage(storage_mode=msg['storage_mode'], dfs_volume=dfs_volume).delete_image(path=path)

    @classmethod
    def reset_password(cls, dom=None, msg=None):
        assert isinstance(dom, libvirt.virDomain)
        assert isinstance(msg, dict)
        # https://libvirt.org/html/libvirt-libvirt-domain.html#virDomainSetUserPassword
        dom.setUserPassword(msg['user'], msg['password'])

    @classmethod
    def attach_disk(cls, dom=None, msg=None):
        assert isinstance(dom, libvirt.virDomain)
        assert isinstance(msg, dict)

        if 'xml' not in msg:
            err = u'添加磁盘缺少 xml 参数'
            raise KeyError(err)

        flags = libvirt.VIR_DOMAIN_AFFECT_CONFIG
        if dom.isActive():
            flags |= libvirt.VIR_DOMAIN_AFFECT_LIVE

        # https://libvirt.org/html/libvirt-libvirt-domain.html#virDomainAttachDeviceFlags
        dom.attachDeviceFlags(xml=msg['xml'], flags=flags)
        cls.quota(dom=dom, msg=msg)

    @classmethod
    def detach_disk(cls, dom=None, msg=None):
        assert isinstance(dom, libvirt.virDomain)
        assert isinstance(msg, dict)

        if 'xml' not in msg:
            err = u'分离磁盘缺少 xml 参数'
            raise KeyError(err)

        flags = libvirt.VIR_DOMAIN_AFFECT_CONFIG
        if dom.isActive():
            flags |= libvirt.VIR_DOMAIN_AFFECT_LIVE

        # https://libvirt.org/html/libvirt-libvirt-domain.html#virDomainDetachDeviceFlags
        dom.detachDeviceFlags(xml=msg['xml'], flags=flags)

    @staticmethod
    def quota(dom=None, msg=None):
        assert isinstance(dom, libvirt.virDomain)
        assert isinstance(msg, dict)

        for disk in msg['disks']:
            libvirt_qemu.qemuMonitorCommand(dom, json.dumps({
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
    def update_ssh_key(dom=None, msg=None):
        assert isinstance(dom, libvirt.virDomain)
        assert isinstance(msg, dict)

        if not dom.isActive():
            log = u'欲更新 SSH-KEY 的目标虚拟机未处于活动状态。'
            log_emit.warn(log)
            return

        from utils import QGA

        libvirt_qemu.qemuAgentCommand(dom, json.dumps({
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

            exec_ret = libvirt_qemu.qemuAgentCommand(dom, json.dumps({
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

            exec_ret = json.loads(exec_ret)
            status_ret = QGA.get_guest_exec_status(dom=dom, pid=exec_ret['return']['pid'])
            exec_ret_str = base64.b64decode(json.loads(status_ret)['return']['out-data'])
            ret_s.append(json.loads(exec_ret_str))

        return ret_s

    @staticmethod
    def create_snapshot(dom=None, msg=None):
        extend_data = dict()

        try:
            assert isinstance(dom, libvirt.virDomain)
            assert isinstance(msg, dict)
            snap_xml = """
                <domainsnapshot>
                </domainsnapshot>
            """

            snap_flags = 0
            snap_flags |= libvirt.VIR_DOMAIN_SNAPSHOT_CREATE_ATOMIC

            ret = dom.snapshotCreateXML(xmlDesc=snap_xml, flags=snap_flags)

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
            log_emit.error(traceback.format_exc())
            response_emit.failure(_object=msg['_object'], action=msg.get('action'), uuid=msg.get('uuid'),
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

    @staticmethod
    def delete_snapshot(dom=None, msg=None):
        extend_data = dict()

        try:
            assert isinstance(dom, libvirt.virDomain)
            assert isinstance(msg, dict)

            snapshot = dom.snapshotLookupByName(name=msg['snapshot_id'])
            snapshot.delete()

            response_emit.success(_object=msg['_object'], action=msg['action'], uuid=msg['uuid'],
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

        except:
            log_emit.error(traceback.format_exc())
            response_emit.failure(_object=msg['_object'], action=msg.get('action'), uuid=msg.get('uuid'),
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

    @staticmethod
    def revert_snapshot(dom=None, msg=None):
        extend_data = dict()

        try:
            assert isinstance(dom, libvirt.virDomain)
            assert isinstance(msg, dict)

            snap_flags = 0
            snap_flags |= libvirt.VIR_DOMAIN_SNAPSHOT_REVERT_FORCE
            snapshot = dom.snapshotLookupByName(name=msg['snapshot_id'])

            try:
                dom.revertToSnapshot(snap=snapshot, flags=0)

            except libvirt.libvirtError, e:
                # 给予一次重新恢复的机会
                if e.get_error_code() == libvirt.VIR_ERR_SYSTEM_ERROR:
                    dom.revertToSnapshot(snap=snapshot, flags=snap_flags)

            # 如果恢复后的 Guest 为 Running 状态，则同步其系统时间。
            if dom.isActive():
                # https://qemu.weilnetz.de/doc/qemu-ga-ref.html#index-guest_002dset_002dtime
                libvirt_qemu.qemuAgentCommand(dom, json.dumps({
                        'execute': 'guest-set-time',
                        'arguments': {
                            'time': int(ji.Common.ts() * (10**9))
                        }
                    }),
                    3,
                    libvirt_qemu.VIR_DOMAIN_QEMU_AGENT_COMMAND_NOWAIT)

            response_emit.success(_object=msg['_object'], action=msg['action'], uuid=msg['uuid'],
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

        except:
            log_emit.error(traceback.format_exc())
            response_emit.failure(_object=msg['_object'], action=msg.get('action'), uuid=msg.get('uuid'),
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

    @staticmethod
    def convert_snapshot(msg=None):

        pattern_progress = re.compile(r'\((\d+(\.\d+)?)/100%\)')

        extend_data = dict()

        try:
            assert isinstance(msg, dict)

            snapshot_path = msg['snapshot_path']
            template_path = msg['template_path']

            if msg['storage_mode'] == StorageMode.glusterfs.value:

                storage = Storage(storage_mode=msg['storage_mode'], dfs_volume=msg['dfs_volume'])

                if not storage.gf.isdir(os.path.dirname(template_path)):
                    storage.gf.makedirs(os.path.dirname(template_path), 0755)

                snapshot_path = '/'.join(['gluster://127.0.0.1', msg['dfs_volume'], snapshot_path])
                template_path = '/'.join(['gluster://127.0.0.1', msg['dfs_volume'], template_path])

            cmd = ' '.join(['/usr/bin/qemu-img', 'convert', '--force-share', '-O', 'qcow2', '-s', msg['snapshot_id'],
                            snapshot_path, template_path])

            qemu_img_convert = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

            fcntl.fcntl(qemu_img_convert.stdout, fcntl.F_SETFL,
                        fcntl.fcntl(qemu_img_convert.stdout, fcntl.F_GETFL) | os.O_NONBLOCK)

            while qemu_img_convert.returncode is None:
                line = None

                try:
                    line = qemu_img_convert.stdout.readline()
                except IOError as e:
                    pass

                if line is not None:
                    p = pattern_progress.match(line.strip())

                    if p is not None:
                        fields = p.groups()
                        guest_event_emit.snapshot_converting(uuid=msg['uuid'],
                                                             os_template_image_id=msg['os_template_image_id'],
                                                             progress=int(fields[0].split('.')[0]))

                time.sleep(0.5)
                qemu_img_convert.send_signal(signal.SIGUSR1)
                qemu_img_convert.poll()

            if qemu_img_convert.returncode != 0:
                raise CommandExecFailed(u'创建自定义模板失败，命令执行退出异常。')

            response_emit.success(_object=msg['_object'], action=msg['action'], uuid=msg['uuid'],
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

        except:
            log_emit.error(traceback.format_exc())
            response_emit.failure(_object=msg['_object'], action=msg.get('action'), uuid=msg.get('uuid'),
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

    @staticmethod
    def allocate_bandwidth(dom=None, msg=None):
        extend_data = dict()

        """
        https://libvirt.org/html/libvirt-libvirt-domain.html#virDomainModificationImpact
        """

        try:
            assert isinstance(dom, libvirt.virDomain)
            assert isinstance(msg, dict)

            bandwidth = msg['bandwidth'] / 1000 / 8
            mac = ET.fromstring(dom.XMLDesc()).findall('devices/interface')[0].find('mac').attrib['address']

            interface_bandwidth = dom.interfaceParameters(mac, 0)
            interface_bandwidth['inbound.average'] = bandwidth
            interface_bandwidth['outbound.average'] = bandwidth

            dom.setInterfaceParameters(mac, interface_bandwidth, libvirt.VIR_DOMAIN_AFFECT_CONFIG)

            if dom.isActive():
                dom.setInterfaceParameters(mac, interface_bandwidth, libvirt.VIR_DOMAIN_AFFECT_LIVE)

            response_emit.success(_object=msg['_object'], action=msg['action'], uuid=msg['uuid'],
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

        except:
            log_emit.error(traceback.format_exc())
            response_emit.failure(_object=msg['_object'], action=msg.get('action'), uuid=msg.get('uuid'),
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

    @staticmethod
    def adjust_ability(dom=None, msg=None):
        extend_data = dict()

        try:
            assert isinstance(dom, libvirt.virDomain)
            assert isinstance(msg, dict)

            cpu = msg['cpu'].__str__()
            memory = msg['memory'].__str__()

            xml = ET.fromstring(dom.XMLDesc())

            origin_ability = xml.find('vcpu').text + '核' + (int(xml.find('memory').text) / 1024 ** 2).__str__() + 'GiB'
            new_ability = cpu + '核' + memory + 'GiB'

            xml.find('vcpu').text = cpu

            xml.find('memory').set('unit', 'GiB')
            xml.find('memory').text = memory

            xml.find('currentMemory').set('unit', 'GiB')
            xml.find('currentMemory').text = memory

            xml_str = ET.tostring(xml, encoding='utf8', method='xml')

            if dom.isActive():
                raise RuntimeError(u'虚拟机非关闭状态。')

            else:
                if dom.connect().defineXML(xml=xml_str):
                    log = u' '.join([u'域', dom.name(), u', UUID', dom.UUIDString(), u'配置从', origin_ability,
                                     '变更为', new_ability])
                    log_emit.info(msg=log)

                else:
                    raise RuntimeError(u'变更配置失败。')

            response_emit.success(_object=msg['_object'], action=msg['action'], uuid=msg['uuid'],
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

        except:
            log_emit.error(traceback.format_exc())
            response_emit.failure(_object=msg['_object'], action=msg.get('action'), uuid=msg.get('uuid'),
                                  data=extend_data, passback_parameters=msg.get('passback_parameters'))

    @staticmethod
    def migrate(dom=None, msg=None):
        assert isinstance(dom, libvirt.virDomain)
        assert isinstance(msg, dict)

        # https://rk4n.github.io/2016/08/10/qemu-post-copy-and-auto-converge-features/
        flags = libvirt.VIR_MIGRATE_PERSIST_DEST | \
            libvirt.VIR_MIGRATE_UNDEFINE_SOURCE | \
            libvirt.VIR_MIGRATE_COMPRESSED | \
            libvirt.VIR_MIGRATE_PEER2PEER | \
            libvirt.VIR_MIGRATE_AUTO_CONVERGE

        root = ET.fromstring(dom.XMLDesc())

        if msg['storage_mode'] == StorageMode.local.value:
            # 需要把磁盘存放路径加入到两边宿主机的存储池中
            # 不然将会报 no storage pool with matching target path '/opt/Images' 错误
            flags |= libvirt.VIR_MIGRATE_NON_SHARED_DISK
            flags |= libvirt.VIR_MIGRATE_LIVE

            if not dom.isActive():
                err = u'非共享存储不支持离线迁移。'
                log_emit.warn(err)
                raise RuntimeError('Nonsupport offline migrate with storage of non sharing mode.')

            ssh_client = Utils.ssh_client(hostname=msg['duri'].split('/')[2], user='root')

            for _disk in root.findall('devices/disk'):
                _file_path = _disk.find('source').get('file')
                disk_info = Storage.image_info_by_local(path=_file_path)
                disk_size = disk_info['virtual-size']
                stdin, stdout, stderr = ssh_client.exec_command(
                    ' '.join(['qemu-img', 'create', '-f', 'qcow2', _file_path, str(disk_size)]))

                for line in stdout:
                    log_emit.info(line)

                for line in stderr:
                    log_emit.error(line)

        elif msg['storage_mode'] in [StorageMode.shared_mount.value, StorageMode.ceph.value,
                                     StorageMode.glusterfs.value]:
            if dom.isActive():
                flags |= libvirt.VIR_MIGRATE_LIVE
                flags |= libvirt.VIR_MIGRATE_TUNNELLED

            else:
                flags |= libvirt.VIR_MIGRATE_OFFLINE

        # duri like qemu+ssh://destination_host/system
        if dom.migrateToURI(duri=msg['duri'], flags=flags) == 0:
            if msg['storage_mode'] == StorageMode.local.value:
                for _disk in root.findall('devices/disk'):
                    _file_path = _disk.find('source').get('file')
                    if _file_path is not None:
                        os.remove(_file_path)

        else:
            raise RuntimeError('Unknown storage mode.')

