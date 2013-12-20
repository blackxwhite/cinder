# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
"""
Driver for Linux servers running LVM.

"""

import os
import re
import socket

from oslo.config import cfg

from cinder import exception
from cinder.image import image_utils
from cinder.openstack.common import log as logging
from cinder.openstack.common import processutils
from cinder import utils
from cinder.volume import driver
from cinder.volume.drivers import lvm

LOG = logging.getLogger(__name__)

volume_opts = [
    cfg.StrOpt('volume_group',
               default='cinder-volumes',
               help='Name for the VG that will contain exported volumes'),
    cfg.StrOpt('volume_mount_root',
               default='/vol',
               help='Root directory used to mount filesystem on LVM volumes.'),
    cfg.StrOpt('volume_mount_opts',
               default=None,
               help='Options for mounting local filesystem'),
    cfg.StrOpt('volume_filesystem',
               default='ext3',
               help='Filesystem the volume should be formatted.'),
    cfg.StrOpt('volume_filesystem_args',
               default=None,
               help='Arguments used for filesystem formatting.'),
    cfg.StrOpt('nfs_export_opts',
               default='rw,sync,no_root_squash',
               help='NFS exporting options.'),
    cfg.StrOpt('pool_size',
               default=None,
               help='Size of thin provisioning pool '
                    '(None uses entire cinder VG)'),
    cfg.IntOpt('lvm_mirrors',
               default=0,
               help='If set, create lvms with multiple mirrors. Note that '
                    'this requires lvm_mirrors + 2 pvs with available space'),
    cfg.StrOpt('lvm_type',
               default='default',
               help='Type of LVM volumes to deploy; (default or thin)'),
]

CONF = cfg.CONF
CONF.register_opts(volume_opts)


class LVMNFSDriver(lvm.LVMVolumeDriver):
    """Executes commands relating to NFS volumes.

    We make use of model provider properties as follows:

    ``provider_location``
      if present, contains the NFS volume information
      i.e. '<ip>:<path>'
    """

    def __init__(self, *args, **kwargs):
        super(LVMNFSDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(volume_opts)
        self.backend_name =\
            self.configuration.safe_get('volume_backend_name') or 'LVM_NFS'
        self.protocol = 'NFS'

    def set_execute(self, execute):
        super(LVMNFSDriver, self).set_execute(execute)

    def check_for_setup_error(self):
        super(LVMNFSDriver, self).check_for_setup_error()
        self.configuration.volume_mount_root = self.configuration.volume_mount_root.rstrip('/')
        if not os.path.exists(self.configuration.volume_mount_root) or not os.path.isdir(self.configuration.volume_mount_root):
            exception_message = (_("Missing root directory %s for filesystem mounting.")
                                 % self.configuration.volume_mount_root)
            raise exception.VolumeBackendAPIException(data=exception_message)

    def _make_filesystem(self, volume_path):
        if self.configuration.volume_filesystem_args:
            cmd = ['mkfs', '-t', self.configuration.volume_filesystem,
                   '-o', self.configuration.volume_filesystem_args, volume_path]
        else:
            cmd = ['mkfs', '-t', self.configuration.volume_filesystem, volume_path]
        self._execute(*cmd, run_as_root=True)

    def _mount_filesystem(self, volume_path, mount_path):
        if self.configuration.volume_mount_opts: 
            cmd = ['mount', '-o', self.configuration.volume_mount_opts, volume_path, mount_path]
        else:
            cmd = ['mount', volume_path, mount_path]
        self._execute(*cmd, run_as_root=True)

    def _export_filesystem(self, path):
        if self.configuration.nfs_export_opts:
            cmd = ['exportfs', '-o', self.configuration.nfs_export_opts, "*:%s" % path]
        else:
            cmd = ['exportfs', "*:%s" % path]
        self._execute(*cmd, run_as_root=True)

    def _create_mount_point(self, path):
        cmd = ['mkdir', path]
        self._execute(*cmd, run_as_root=True)

    def create_volume(self, volume):
        """Creates a logical volume. Can optionally return a Dictionary of
        changes to the volume object to be persisted."""
        super(LVMNFSDriver, self).create_volume(volume)
        volume_path = "/dev/%s/%s" % (self.configuration.volume_group, volume['name'])
        self._make_filesystem(volume_path)

    def ensure_export(self, context, volume):
        """Synchronously recreates an export for a logical volume."""
        mount_path = "%s/%s" % (self.configuration.volume_mount_root,
                                volume['name'])
        volume_path = "/dev/%s/%s" % (self.configuration.volume_group,
                                      volume['name'])

        if not os.path.exists(mount_path):
            self._create_mount_point(mount_path)
        if not os.path.ismount(mount_path):
            self._mount_filesystem(volume_path, mount_path)
        self._export_filesystem(mount_path)

    def create_export(self, context, volume):
        """Creates an export for a logical volume."""
        mount_path = "%s/%s" % (self.configuration.volume_mount_root,
                                volume['name'])
        volume_path = "/dev/%s/%s" % (self.configuration.volume_group,
                                      volume['name'])
        model_update = {}

        if not os.path.exists(mount_path):
            self._create_mount_point(mount_path)
        if not os.path.ismount(mount_path):
            self._mount_filesystem(volume_path, mount_path)
        self._export_filesystem(mount_path)

        model_update['provider_location'] = self._nfs_location(
            self.configuration.iscsi_ip_address, mount_path)
        return model_update

    def remove_export(self, context, volume):
        """Removes an export for a logical volume."""
        mount_path = "%s/%s" % (self.configuration.volume_mount_root,
                                volume['name'])
        volume_path = "/dev/%s/%s" % (self.configuration.volume_group,
                                      volume['name'])

        cmd = ['exportfs', '-u', "*:%s" % mount_path]
        self._execute(*cmd, run_as_root=True)
        try:
            cmd = ['umount', mount_path]
            self._execute(*cmd, run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            if 'not mounted' in exc.stderr or 'not found' in exc.stderr:
                LOG.info(_("Mount point is not mounted, ignored."))
            else:
                raise
        try:
            cmd = ['rmdir', mount_path]
            self._execute(*cmd, run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            if 'No such file or directory' in exc.stderr:
                LOG.info(_("Mount point is not existed, ignored."))
            else:
                raise

    def initialize_connection(self, volume, connector):
        """Allow connection to connector and return connection info."""
        data = {'export': volume['provider_location'],
                'name': volume['name']}
        return {
            'driver_volume_type': 'nfs',
            'data': data
        }

    def terminate_connection(self, volume, connector, **kwargs):
        """Disallow connection from connector"""
        pass

    def _nfs_location(self, ip, path):
        return "%s:%s" % (ip, path)
