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

import math
import os
import re

from oslo.config import cfg

from cinder import exception
from cinder import flags
from cinder.image import image_utils
from cinder.openstack.common import log as logging
from cinder import utils
from cinder.volume import driver
from cinder.volume.drivers import lvm

LOG = logging.getLogger(__name__)

volume_opts = [
    cfg.StrOpt('volume_group',
               default='cinder-volumes',
               help='Name for the VG that will contain exported volumes'),
    cfg.StrOpt('volume_clear',
               default='zero',
               help='Method used to wipe old volumes (valid options are: '
                    'none, zero, shred)'),
    cfg.IntOpt('volume_clear_size',
               default=0,
               help='Size in MiB to wipe at start of old volumes. 0 => all'),
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
]

FLAGS = flags.FLAGS
FLAGS.register_opts(volume_opts)


class LVMNFSDriver(lvm.LVMVolumeDriver):
    """Executes commands relating to NFS volumes.

    We make use of model provider properties as follows:

    ``provider_location``
      if present, contains the NFS volume information
      i.e. '<ip>:<path>'

    ``provider_auth``
      if present, contains a space-separated triple:
      '<auth method> <auth username> <auth password>'.
      `CHAP` is the only auth_method in use at the moment.
    """

    def __init__(self, *args, **kwargs):
        #self.tgtadm = iscsi.get_target_admin()
        super(LVMNFSDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(volume_opts)

    def set_execute(self, execute):
        super(LVMNFSDriver, self).set_execute(execute)
        #self.tgtadm.set_execute(execute)

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
        self._try_execute(*cmd, run_as_root=True)

    def _mount_filesystem(self, volume_path, mount_path):
        if self.configuration.volume_mount_opts: 
            cmd = ['mount', '-o', self.configuration.volume_mount_opts, volume_path, mount_path]
        else:
            cmd = ['mount', volume_path, mount_path]
        self._try_execute(*cmd, run_as_root=True)

    def _export_filesystem(self, path):
        if self.configuration.nfs_export_opts:
            cmd = ['exportfs', '-o', self.configuration.nfs_export_opts, "*:%s" % path]
        else:
            cmd = ['exportfs', "*:%s" % path]
        self._try_execute(*cmd, run_as_root=True)

    def _create_mount_point(self, path):
        cmd = ['mkdir', path]
        self._try_execute(*cmd, run_as_root=True)

    def create_volume(self, volume):
        """Creates a logical volume. Can optionally return a Dictionary of
        changes to the volume object to be persisted."""
        super(LVMNFSDriver, self).create_volume(volume)
        volume_path = "/dev/%s/%s" % (FLAGS.volume_group, volume['name'])
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
        self._try_execute(*cmd, run_as_root=True)
        try:
            cmd = ['umount', mount_path]
            self._try_execute(*cmd, run_as_root=True)
        except exception.ProcessExecutionError as exc:
            if 'not mounted' in exc.stderr or 'not found' in exc.stderr:
                LOG.info(_("Mount point is not mounted, ignored."))
            else:
                raise
        try:
            cmd = ['rmdir', mount_path]
            self._try_execute(*cmd, run_as_root=True)
        except exception.ProcessExecutionError as exc:
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

    def get_volume_stats(self, refresh=False):
        """Get volume status.

        If 'refresh' is True, run update the stats first."""
        if refresh:
            self._update_volume_status()

        return self._stats

    def _update_volume_status(self):
        """Retrieve status info from volume group."""

        LOG.debug(_("Updating volume status"))
        data = {}

        # Note(zhiteng): These information are driver/backend specific,
        # each driver may define these values in its own config options
        # or fetch from driver specific configuration file.
        backend_name = self.configuration.safe_get('volume_backend_name')
        data["volume_backend_name"] = backend_name or 'LVM_NFS'
        data["vendor_name"] = 'Open Source'
        data["driver_version"] = self.VERSION
        data["storage_protocol"] = 'NFS'

        data['total_capacity_gb'] = 0
        data['free_capacity_gb'] = 0
        data['reserved_percentage'] = self.configuration.reserved_percentage
        data['QoS_support'] = False

        try:
            out, err = self._execute('vgs', '--noheadings', '--nosuffix',
                                     '--unit=G', '-o', 'name,size,free',
                                     self.configuration.volume_group,
                                     run_as_root=True)
        except exception.ProcessExecutionError as exc:
            LOG.error(_("Error retrieving volume status: "), exc.stderr)
            out = False

        if out:
            volume = out.split()
            data['total_capacity_gb'] = float(volume[1].replace(',', '.'))
            data['free_capacity_gb'] = float(volume[2].replace(',', '.'))

        self._stats = data

    def _nfs_location(self, ip, path):
        return "%s:%s" % (ip, path)


class ThinLVMNFSDriver(LVMNFSDriver):
    """Subclass for thin provisioned LVM's."""

    VERSION = '1.0'

    def __init__(self, *args, **kwargs):
        super(ThinLVMNFSDriver, self).__init__(*args, **kwargs)

    def check_for_setup_error(self):
        """Returns an error if prerequisites aren't met"""
        out, err = self._execute('lvs', '--option',
                                 'name', '--noheadings',
                                 run_as_root=True)
        pool_name = "%s-pool" % FLAGS.volume_group
        if pool_name not in out:
            if not FLAGS.pool_size:
                out, err = self._execute('vgs', FLAGS.volume_group,
                                         '--noheadings', '--options',
                                         'name,size', run_as_root=True)
                size = re.sub(r'[\.][\d][\d]', '', out.split()[1])
            else:
                size = "%s" % FLAGS.pool_size

            pool_path = '%s/%s' % (FLAGS.volume_group, pool_name)
            out, err = self._execute('lvcreate', '-T', '-L', size,
                                     pool_path, run_as_root=True)

    def _do_lvm_snapshot(self, src_lvm_name, dest_vref, is_cinder_snap=True):
            if is_cinder_snap:
                new_name = self._escape_snapshot(dest_vref['name'])
            else:
                new_name = dest_vref['name']

            self._try_execute('lvcreate', '-s', '-n', new_name,
                              src_lvm_name, run_as_root=True)

    def create_volume(self, volume):
        """Creates a logical volume. Can optionally return a Dictionary of
        changes to the volume object to be persisted."""
        sizestr = self._sizestr(volume['size'])
        vg_name = ("%s/%s-pool" % (FLAGS.volume_group, FLAGS.volume_group))
        self._try_execute('lvcreate', '-T', '-V', sizestr, '-n',
                          volume['name'], vg_name, run_as_root=True)

    def delete_volume(self, volume):
        """Deletes a logical volume."""
        if self._volume_not_present(volume['name']):
            return True
        self._try_execute('lvremove', '-f', "%s/%s" %
                          (FLAGS.volume_group,
                           self._escape_snapshot(volume['name'])),
                          run_as_root=True)

    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume."""
        LOG.info(_('Creating clone of volume: %s') % src_vref['id'])
        orig_lv_name = "%s/%s" % (FLAGS.volume_group, src_vref['name'])
        self._do_lvm_snapshot(orig_lv_name, volume, False)

    def create_snapshot(self, snapshot):
        """Creates a snapshot of a volume."""
        orig_lv_name = "%s/%s" % (FLAGS.volume_group, snapshot['volume_name'])
        self._do_lvm_snapshot(orig_lv_name, snapshot)

    def get_volume_stats(self, refresh=False):
        """Get volume status.
        If 'refresh' is True, run update the stats first."""
        if refresh:
            self._update_volume_status()

        return self._stats

    def _update_volume_status(self):
        """Retrieve status info from volume group."""

        LOG.debug(_("Updating volume status"))
        data = {}

        backend_name = self.configuration.safe_get('volume_backend_name')
        data["volume_backend_name"] = backend_name or self.__class__.__name__
        data["vendor_name"] = 'Open Source'
        data["driver_version"] = self.VERSION
        data["storage_protocol"] = 'NFS'
        data['reserved_percentage'] = self.configuration.reserved_percentage
        data['QoS_support'] = False
        data['total_capacity_gb'] = 'infinite'
        data['free_capacity_gb'] = 'infinite'
        self._stats = data
