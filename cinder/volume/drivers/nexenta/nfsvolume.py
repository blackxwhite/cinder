# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright 2011 Nexenta Systems, Inc.
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
:mod:`nexenta.nfsvolume` -- Driver to store NFS volumes on Nexenta Appliance
=====================================================================

.. automodule:: nexenta.nfsvolume
.. moduleauthor:: Blake Lai <blackxwhite@gmail.com>
"""

from oslo.config import cfg

from cinder import exception
from cinder.openstack.common import log as logging
from cinder import units
from cinder.volume import driver
from cinder.volume.drivers import nexenta
from cinder.volume.drivers.nexenta import jsonrpc
from cinder.volume.drivers.nexenta import options

LOG = logging.getLogger(__name__)

NEXENTA_NFSVOLUME_OPTIONS = [
    cfg.StrOpt('nexenta_volume',
               default='cinder',
               help='pool on SA that will hold all volumes'),
    cfg.BoolOpt('nexenta_reserve',
                default=False,
                help='flag to create volumes with size reserved'),
    cfg.BoolOpt('nexenta_compress',
                default=True,
                help='flag to create volumes with compression feature'),
    cfg.BoolOpt('nexenta_dedup',
                default=False,
                help='flag to create volumes with deduplication feature'),
]

CONF = cfg.CONF
CONF.register_opts(options.NEXENTA_CONNECTION_OPTIONS)
CONF.register_opts(NEXENTA_NFSVOLUME_OPTIONS)


class NexentaNfsVolumeDriver(driver.VolumeDriver):  # pylint: disable=R0921
    """Executes volume driver commands on Nexenta Appliance."""

    VERSION = '1.0.0'

    def __init__(self, *args, **kwargs):
        super(NexentaNfsVolumeDriver, self).__init__(*args, **kwargs)
        self.nms = None
        if self.configuration:
            self.configuration.append_config_values(
                options.NEXENTA_CONNECTION_OPTIONS)
            self.configuration.append_config_values(
                NEXENTA_NFSVOLUME_OPTIONS)

    def do_setup(self, context):
        protocol = self.configuration.nexenta_rest_protocol
        auto = protocol == 'auto'
        if auto:
            protocol = 'http'
        url = '%s://%s:%s/rest/nms/' % (protocol,
                                        self.configuration.nexenta_host,
                                        self.configuration.nexenta_rest_port)
        self.nms = jsonrpc.NexentaJSONProxy(
            url, self.configuration.nexenta_user,
            self.configuration.nexenta_password, auto=auto)

    def check_for_setup_error(self):
        """Verify that the volume for our zvols exists.

        :raise: :py:exc:`LookupError`
        """
        if not self.nms.volume.object_exists(
                self.configuration.nexenta_volume):
            raise LookupError(_('Volume %s does not exist in Nexenta SA'),
                              self.configuration.nexenta_volume)

    def _get_folder_name(self, volume_name):
        """Return folder name that corresponds given volume name."""
        return '%s/%s' % (self.configuration.nexenta_volume, volume_name)

    def _get_clone_snap_name(self, volume):
        """Return name for snapshot that will be used to clone the volume."""
        return 'cinder-clone-snap-%(id)s' % volume

    def create_volume(self, volume):
        """Create a folder on appliance.

        :param volume: volume reference
        """
        quota = '%sG' % (volume['size'])
        reservation = ('%sG' % (volume['size']) if self.configuration.nexenta_reserve else 'none')
        compression = ('on' if self.configuration.nexenta_compress else 'off')
        dedup = ('on' if self.configuration.nexenta_dedup else 'off')
        
        try:
            self.nms.folder.create_with_props(
                self.configuration.nexenta_volume,
                volume['name'],
                { 'quota': quota, 'reservation': reservation, 'compression': compression, 'dedup': dedup })
        except nexenta.NexentaException as exc:
            if 'out of space' in exc.args[1]:
                raise exception.VolumeSizeExceedsAvailableQuota()
            else:
                raise

    def delete_volume(self, volume):
        """Destroy a folder on appliance.

        :param volume: volume reference
        """
        try:
            self.nms.folder.destroy(self._get_folder_name(volume['name']), '')
        except nexenta.NexentaException as exc:
            if 'folder has children' in exc.args[1]:
                raise exception.VolumeIsBusy(volume_name=volume['name'])
            elif 'does not exist' in exc.args[1]:
                LOG.warn(_('Got error trying to delete volume'
                       ' %(folder_name)s, assuming it is '
                       'already deleted: %(exc)s'),
                      {'folder_name': self._get_folder_name(volume['name']), 'exc': exc})
            else:
                raise

    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume.

        :param volume: new volume reference
        :param src_vref: source volume reference
        """
        snapshot = {'volume_name': src_vref['name'],
                    'name': self._get_clone_snap_name(volume)}
        LOG.debug(_('Creating temp snapshot of the original volume: '
                    '%(volume_name)s@%(name)s'), snapshot)
        self.create_snapshot(snapshot)
        try:
            cmd = 'zfs send %(src_vol)s@%(src_snap)s | zfs recv %(volume)s' % {
                'src_vol': self._get_folder_name(src_vref['name']),
                'src_snap': snapshot['name'],
                'volume': self._get_folder_name(volume['name'])
            }
            LOG.debug(_('Executing zfs send/recv on the appliance'))
            self.nms.appliance.execute(cmd)
            LOG.debug(_('zfs send/recv done, new volume %s created'),
                      volume['name'])
        finally:
            try:
                # deleting temp snapshot of the original volume
                self.delete_snapshot(snapshot)
            except (nexenta.NexentaException, exception.SnapshotIsBusy):
                LOG.warning(_('Failed to delete temp snapshot '
                              '%(volume)s@%(snapshot)s'),
                            {'volume': src_vref['name'],
                             'snapshot': snapshot['name']})
        try:
            # deleting snapshot resulting from zfs recv
            self.delete_snapshot({'volume_name': volume['name'],
                                  'name': snapshot['name']})
        except (nexenta.NexentaException, exception.SnapshotIsBusy):
            LOG.warning(_('Failed to delete zfs recv snapshot '
                          '%(volume)s@%(snapshot)s'),
                        {'volume': volume['name'],
                         'snapshot': snapshot['name']})

    def create_snapshot(self, snapshot):
        """Create snapshot of existing folder on appliance.

        :param snapshot: shapshot reference
        """
        self.nms.folder.create_snapshot(
            self._get_folder_name(snapshot['volume_name']),
            snapshot['name'], '')

    def create_volume_from_snapshot(self, volume, snapshot):
        """Create new volume from other's snapshot on appliance.

        :param volume: reference of volume to be created
        :param snapshot: reference of source snapshot
        """
        quota = '%sG' % (volume['size'])
        reservation = ('%sG' % (volume['size']) if self.configuration.nexenta_reserve else 'none')
        compression = ('on' if self.configuration.nexenta_compress else 'off')
        dedup = ('on' if self.configuration.nexenta_dedup else 'off')

        self.nms.folder.clone(
            '%s@%s' % (self._get_folder_name(snapshot['volume_name']),
                       snapshot['name']),
            self._get_folder_name(volume['name']))

        try:
            self.nms.folder.set_child_prop(self._get_folder_name(volume['name']), 'quota', quota)
            self.nms.folder.set_child_prop(self._get_folder_name(volume['name']), 'reservation', reservation)
            self.nms.folder.set_child_prop(self._get_folder_name(volume['name']), 'compression', compression)
            self.nms.folder.set_child_prop(self._get_folder_name(volume['name']), 'dedup', dedup)
        except nexenta.NexentaException as exc:
            if 'size is greater than available space' in exc.args[1]:
                self.delete_volume(volume)
                raise exception.VolumeSizeExceedsAvailableQuota()
            else:
                raise

    def delete_snapshot(self, snapshot):
        """Delete volume's snapshot on appliance.

        :param snapshot: shapshot reference
        """
        try:
            self.nms.snapshot.destroy(
                '%s@%s' % (self._get_folder_name(snapshot['volume_name']),
                           snapshot['name']),
                '')
        except nexenta.NexentaException as exc:
            if 'snapshot has dependent clones' in exc.args[1]:
                raise exception.SnapshotIsBusy(snapshot_name=snapshot['name'])
            else:
                raise

    def local_path(self, volume):
        """Return local path to existing local volume.

        We never have local volumes, so it raises NotImplementedError.

        :raise: :py:exc:`NotImplementedError`
        """
        raise NotImplementedError

    def _get_provider_location(self, volume):
        """Returns volume nfs-formatted provider location string."""
        return '%(host)s:/volumes/%(name)s' % {
            'host': self.configuration.nexenta_host,
            'name': self._get_folder_name(volume['name'])
        }

    def _do_export(self, _ctx, volume, ensure=False):
        """Do all steps to get folder exported as NFS volume.

        :param volume: reference of volume to be exported
        :param ensure: if True, ignore errors caused by already existing
            resources
        """
        folder_name = self._get_folder_name(volume['name'])

        try:
            self.nms.netstorsvc.share_folder(
                'svc:/network/nfs/server:default',
                folder_name,
                { 'read_write': '*', 'extra_options': 'anon=0' })
        except nexenta.NexentaException as exc:
            if not ensure:
                raise
            else:
                LOG.info(_('Ignored NFS share folder creation error "%s"'
                           ' while ensuring export'), exc)

    def create_export(self, _ctx, volume):
        """Create new export for folder.

        :param volume: reference of volume to be exported
        :return: nfs-formatted provider location string
        """
        self._do_export(_ctx, volume, ensure=False)
        return {'provider_location': self._get_provider_location(volume)}

    def ensure_export(self, _ctx, volume):
        """Recreate parts of export if necessary.

        :param volume: reference of volume to be exported
        """
        self._do_export(_ctx, volume, ensure=True)

    def remove_export(self, _ctx, volume):
        """Destroy all resources created to export folder.

        :param volume: reference of volume to be unexported
        """
        folder_name = self._get_folder_name(volume['name'])

        try:
            self.nms.netstorsvc.unshare_folder(
                'svc:/network/nfs/server:default',
                folder_name,
                '0')
        except nexenta.NexentaException as exc:
            # We assume that folder is unshared
            LOG.warn(_('Got error trying to unshare folder'
                       ' %(folder_name)s, assuming it is '
                       'already gone: %(exc)s'),
                     {'folder_name': folder_name, 'exc': exc})

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

    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume."""
        raise NotImplementedError()

    def get_volume_stats(self, refresh=False):
        """Get volume stats.

        If 'refresh' is True, run update the stats first.
        """
        if refresh:
            self._update_volume_stats()

        return self._stats

    def _update_volume_stats(self):
        """Retrieve stats info for Nexenta device."""

        # NOTE(jdg): Aimon Bustardo was kind enough to point out the
        # info he had regarding Nexenta Capabilities, ideally it would
        # be great if somebody from Nexenta looked this over at some point

        LOG.debug(_("Updating volume stats"))
        data = {}
        backend_name = self.__class__.__name__
        if self.configuration:
            backend_name = self.configuration.safe_get('volume_backend_name')
        data["volume_backend_name"] = backend_name or self.__class__.__name__
        data["vendor_name"] = 'Nexenta'
        data["driver_version"] = self.VERSION
        data["storage_protocol"] = 'NFS'

        stats = self.nms.volume.get_child_props(
            self.configuration.nexenta_volume, 'health|size|used|available')
        total_unit = stats['size'][-1]
        total_amount = float(stats['size'][:-1])
        free_unit = stats['available'][-1]
        free_amount = float(stats['available'][:-1])

        if total_unit == "T":
            total_amount *= units.KiB
        elif total_unit == "M":
            total_amount /= units.KiB
        elif total_unit == "B":
            total_amount /= units.MiB

        if free_unit == "T":
            free_amount *= units.KiB
        elif free_unit == "M":
            free_amount /= units.KiB
        elif free_unit == "B":
            free_amount /= units.MiB

        data['total_capacity_gb'] = total_amount
        data['free_capacity_gb'] = free_amount

        data['reserved_percentage'] = 0
        data['QoS_support'] = False
        self._stats = data
