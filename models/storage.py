#!/usr/bin/env python
# -*- coding: utf-8 -*-


import json
import os
import shutil
import threading

from gluster import gfapi

from utils import Utils
from models.status import StorageMode
from jimvn_exception import CommandExecFailed


__author__ = 'James Iter'
__date__ = '2017/4/25'
__contact__ = 'james.iter.cn@gmail.com'
__copyright__ = '(c) 2017 by James Iter.'


class Storage(object):
    storage_mode = None
    gf = None
    dfs_volume = None
    thread_mutex_lock = threading.Lock()

    def __init__(self, **kwargs):
        self.set_storage_mode(storage_mode=kwargs.get('storage_mode', None))
        self.set_dfs_volume(dfs_volume=kwargs.get('dfs_volume', None))

        if self.storage_mode == StorageMode.glusterfs.value:
            self.init_gfapi()

    @classmethod
    def set_storage_mode(cls, storage_mode):
        cls.storage_mode = storage_mode

    @classmethod
    def set_dfs_volume(cls, dfs_volume):
        cls.dfs_volume = dfs_volume

    @classmethod
    def init_gfapi(cls):
        cls.thread_mutex_lock.acquire()

        if cls.gf is None:
            cls.gf = gfapi.Volume('127.0.0.1', cls.dfs_volume)
            cls.gf.mount()

        cls.thread_mutex_lock.release()

        return cls.gf

    @classmethod
    def make_image_by_glusterfs(cls, path=None, size=None):

        if not cls.gf.isdir(os.path.dirname(path)):
            cls.gf.makedirs(os.path.dirname(path), 0755)

        path = '/'.join(['gluster://127.0.0.1', cls.dfs_volume, path])

        cmd = ' '.join(['/usr/bin/qemu-img', 'create', '-f', 'qcow2', path, size.__str__() + 'G'])
        exit_status, output = Utils.shell_cmd(cmd)

        if exit_status != 0:
            err = u' '.join([u'路径', path, u'创建磁盘时，命令执行退出异常：', str(output)])
            raise CommandExecFailed(err)

    @staticmethod
    def make_image_by_local(path=None, size=None):

        if not os.path.isdir(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path), 0755)

        cmd = ' '.join(['/usr/bin/qemu-img', 'create', '-f', 'qcow2', path, size.__str__() + 'G'])
        exit_status, output = Utils.shell_cmd(cmd)

        if exit_status != 0:
            err = u' '.join([u'路径', path, u'创建磁盘时，命令执行退出异常：', str(output)])
            raise CommandExecFailed(err)

    @classmethod
    def make_image(cls, path=None, size=None):
        if cls.storage_mode == StorageMode.glusterfs.value:
            cls.make_image_by_glusterfs(path=path, size=size)

        elif cls.storage_mode in [StorageMode.local.value, StorageMode.shared_mount.value]:
            cls.make_image_by_local(path=path, size=size)

    @classmethod
    def resize_image_by_glusterfs(cls, path=None, size=None):
        path = '/'.join(['gluster://127.0.0.1', cls.dfs_volume, path])

        cmd = ' '.join(['/usr/bin/qemu-img', 'resize', '-f', 'qcow2', path, size.__str__() + 'G'])
        exit_status, output = Utils.shell_cmd(cmd)

        if exit_status != 0:
            err = u' '.join([u'路径', path, u'磁盘扩容时，命令执行退出异常：', str(output)])
            raise CommandExecFailed(err)

    @staticmethod
    def resize_image_by_local(path=None, size=None):
        cmd = ' '.join(['/usr/bin/qemu-img', 'resize', '-f', 'qcow2', path, size.__str__() + 'G'])
        exit_status, output = Utils.shell_cmd(cmd)

        if exit_status != 0:
            err = u' '.join([u'路径', path, u'磁盘扩容时，命令执行退出异常：', str(output)])
            raise CommandExecFailed(err)

    @classmethod
    def resize_image(cls, path=None, size=None):
        if cls.storage_mode == StorageMode.glusterfs.value:
            cls.resize_image_by_glusterfs(path=path, size=size)

        elif cls.storage_mode in [StorageMode.local.value, StorageMode.shared_mount.value]:
            cls.resize_image_by_local(path=path, size=size)

    @classmethod
    def copy_file_by_glusterfs(cls, src=None, dst=None):
        if not cls.gf.isdir(os.path.dirname(dst)):
            cls.gf.makedirs(os.path.dirname(dst), 0755)

        cls.gf.copyfile(src=src, dst=dst)

    @staticmethod
    def copy_file_by_local_path(src=None, dst=None):
        system_image_path_dir = os.path.dirname(dst)

        if not os.path.exists(system_image_path_dir):
            os.makedirs(system_image_path_dir, 0755)

        elif not os.path.isdir(system_image_path_dir):
            os.rename(system_image_path_dir, system_image_path_dir + '.bak')
            os.makedirs(system_image_path_dir, 0755)

        shutil.copyfile(src=src, dst=dst)

    @classmethod
    def copy_file(cls, src=None, dst=None):
        if cls.storage_mode in [StorageMode.ceph.value, StorageMode.glusterfs.value]:
            if cls.storage_mode == StorageMode.glusterfs.value:
                cls.copy_file_by_glusterfs(src=src, dst=dst)

        elif cls.storage_mode in [StorageMode.local.value, StorageMode.shared_mount.value]:
            cls.copy_file_by_local_path(src=src, dst=dst)

    @classmethod
    def delete_image_by_glusterfs(cls, path=None):
        cls.gf.remove(path)

    @staticmethod
    def delete_image_by_local(path=None):
        os.remove(path)

    @classmethod
    def delete_image(cls, path=None):
        if cls.storage_mode == StorageMode.glusterfs.value:
            cls.delete_image_by_glusterfs(path=path)

        elif cls.storage_mode in [StorageMode.local.value, StorageMode.shared_mount.value]:
            cls.delete_image_by_local(path=path)

    @classmethod
    def image_info_by_glusterfs(cls, path=None):
        path = '/'.join(['gluster://127.0.0.1', cls.dfs_volume, path])
        cmd = ' '.join(['/usr/bin/qemu-img', 'info', '--output=json', '-f', 'qcow2', path, '2>/dev/null'])
        exit_status, output = Utils.shell_cmd(cmd)

        if exit_status != 0:
            err = u' '.join([u'路径', path, u'磁盘扩容时，命令执行退出异常：', str(output)])
            raise CommandExecFailed(err)

        return json.loads(output)

    @staticmethod
    def image_info_by_local(path=None):
        cmd = ' '.join(['/usr/bin/qemu-img', 'info', '--output=json', '-f', 'qcow2', path, '2>/dev/null'])
        exit_status, output = Utils.shell_cmd(cmd)

        if exit_status != 0:
            err = u' '.join([u'路径', path, u'磁盘扩容时，命令执行退出异常：', str(output)])
            raise CommandExecFailed(err)

        return json.loads(output)

    @classmethod
    def image_info(cls, path=None):
        if cls.storage_mode == StorageMode.glusterfs.value:
            return cls.image_info_by_glusterfs(path=path)

        elif cls.storage_mode in [StorageMode.local.value, StorageMode.shared_mount.value]:
            return cls.image_info_by_local(path=path)

    @classmethod
    def getsize_by_glusterfs(cls, path=None):
        return cls.gf.getsize(path=path)

    @staticmethod
    def getsize_by_local(path=None):
        return os.path.getsize(filename=path)

    @classmethod
    def getsize(cls, path=None):
        if cls.storage_mode == StorageMode.glusterfs.value:
            return cls.getsize_by_glusterfs(path=path)

        elif cls.storage_mode in [StorageMode.local.value, StorageMode.shared_mount.value]:
            return cls.getsize_by_local(path=path)
