# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright (c) 2012 NetApp, Inc.
# Copyright (c) 2012 OpenStack Foundation.
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
Volume driver for NetApp NFS volume storage.

"""

import suds
from suds import client
from suds.sax import text
import copy
import sys
import time
import uuid

from cinder import exception
from cinder.openstack.common import excutils
from cinder.openstack.common import log as logging
from cinder.openstack.common import timeutils
from cinder import units
from cinder import utils
from cinder.volume import driver
from cinder.volume.drivers.netapp.api import NaApiError
from cinder.volume.drivers.netapp.api import NaElement
from cinder.volume.drivers.netapp.api import NaServer
from cinder.volume.drivers.netapp.options import netapp_7mode_opts
from cinder.volume.drivers.netapp.options import netapp_basicauth_opts
from cinder.volume.drivers.netapp.options import netapp_cluster_opts
from cinder.volume.drivers.netapp.options import netapp_connection_opts
from cinder.volume.drivers.netapp.options import netapp_provisioning_opts
from cinder.volume.drivers.netapp.options import netapp_transport_opts
from cinder.volume.drivers.netapp import ssc_utils
from cinder.volume.drivers.netapp.utils import get_volume_extra_specs
from cinder.volume.drivers.netapp.utils import provide_ems
from cinder.volume.drivers.netapp.utils import set_safe_attr
from cinder.volume.drivers.netapp.utils import validate_instantiation
from cinder.volume import volume_types
from oslo.config import cfg


LOG = logging.getLogger(__name__)


netapp_nfsvolume_opts = [
    cfg.IntOpt('netapp_snapshot_reserve',
                 default=5,
                 help='Percentage of space reserving for snapshots'),
    cfg.StrOpt('netapp_aggregate_name',
               default=None,
               help='Aggregate to use for for provisioning on'
                    ' 7 mode'), ]


CONF = cfg.CONF
CONF.register_opts(netapp_connection_opts)
CONF.register_opts(netapp_transport_opts)
CONF.register_opts(netapp_basicauth_opts)
CONF.register_opts(netapp_cluster_opts)
CONF.register_opts(netapp_7mode_opts)
CONF.register_opts(netapp_provisioning_opts)
CONF.register_opts(netapp_nfsvolume_opts)


class NetAppVolume(object):
    """Represents a Volume on NetApp storage."""

    def __init__(self, handle, name, size, metadata_dict):
        self.handle = handle
        self.name = name
        self.size = size
        self.metadata = metadata_dict or {}

    def get_metadata_property(self, prop):
        """Get the metadata property of a Volume."""
        if prop in self.metadata:
            return self.metadata[prop]
        name = self.name
        msg = _("No metadata property %(prop)s defined for the"
                " Volume %(name)s")
        msg_fmt = {'prop': prop, 'name': name}
        LOG.debug(msg % msg_fmt)

    def __str__(self, *args, **kwargs):
        return 'NetApp Volume[handle:%s, name:%s, size:%s, metadata:%s]'\
               % (self.handle, self.name, self.size, self.metadata)


class NetAppDirectNfsVolumeDriver(driver.VolumeDriver):
    """NetApp Direct NFS volume driver."""

    VERSION = "1.0.0"

    IGROUP_PREFIX = 'openstack-'
    required_flags = ['netapp_transport_type', 'netapp_login',
                      'netapp_password', 'netapp_server_hostname',
                      'netapp_server_port', 'netapp_aggregate_name']

    def __init__(self, *args, **kwargs):
        super(NetAppDirectNfsVolumeDriver, self).__init__(*args, **kwargs)
        validate_instantiation(**kwargs)
        self.configuration.append_config_values(netapp_connection_opts)
        self.configuration.append_config_values(netapp_basicauth_opts)
        self.configuration.append_config_values(netapp_transport_opts)
        self.configuration.append_config_values(netapp_provisioning_opts)
        self.configuration.append_config_values(netapp_nfsvolume_opts)
        self.volume_table = {}

    def _create_client(self, **kwargs):
        """Instantiate a client for NetApp server.

        This method creates NetApp server client for api communication.
        """

        host_filer = kwargs['hostname']
        LOG.debug(_('Using NetApp filer: %s') % host_filer)
        self.client = NaServer(host=host_filer,
                               server_type=NaServer.SERVER_TYPE_FILER,
                               transport_type=kwargs['transport_type'],
                               style=NaServer.STYLE_LOGIN_PASSWORD,
                               username=kwargs['login'],
                               password=kwargs['password'])

    def _do_custom_setup(self):
        """Does custom setup depending on the type of filer."""
        raise NotImplementedError()

    def _check_flags(self):
        """Ensure that the flags we care about are set."""
        required_flags = self.required_flags
        for flag in required_flags:
            if not getattr(self.configuration, flag, None):
                msg = _('%s is not set') % flag
                raise exception.InvalidInput(reason=msg)

    def do_setup(self, context):
        """Setup the NetApp Volume driver.

        Called one time by the manager after the driver is loaded.
        Validate the flags we care about and setup NetApp
        client.
        """

        self._check_flags()
        self._create_client(
            transport_type=self.configuration.netapp_transport_type,
            login=self.configuration.netapp_login,
            password=self.configuration.netapp_password,
            hostname=self.configuration.netapp_server_hostname,
            port=self.configuration.netapp_server_port)
        self._do_custom_setup()

    def check_for_setup_error(self):
        """Check that the driver is working and can communicate.

        Discovers the volumes on the NetApp server.
        """

        self.volume_table = {}
        self._get_volume_list()
        LOG.debug(_("Success getting volume list from server"))

    def create_volume(self, volume):
        """Driver entry point for creating a new volume."""
        default_size = '104857600'  # 100 MB
        gigabytes = 1073741824L  # 2^30
        name = self._get_na_name(volume['name'])
        if int(volume['size']) == 0:
            size = default_size
        else:
            size = str(int(volume['size']) * gigabytes)
        metadata = {}
        metadata['OsType'] = 'linux'
        metadata['SpaceReserved'] = 'true'
        extra_specs = get_volume_extra_specs(volume)
        self._create_volume(name, size, metadata)
        LOG.debug(_("Created Volume with name %s") % name)
        handle = self._create_volume_handle(metadata)
        self._add_volume_to_table(NetAppVolume(handle, name, size, metadata))

    def delete_volume(self, volume):
        """Driver entry point for destroying existing volumes."""
        name = self._get_na_name(volume['name'])
        metadata = self._get_volume_attr(name, 'metadata')
        if not metadata:
            msg = _("No entry in volume table for volume/snapshot %(name)s.")
            msg_fmt = {'name': name}
            LOG.warn(msg % msg_fmt)
            return
        LOG.info(metadata)
        volume_offline = NaElement.create_node_with_children(
            'volume-offline',
            **{'name': metadata['Volume']})
        self.client.invoke_successfully(volume_offline, True)
        volume_destroy = NaElement.create_node_with_children(
            'volume-destroy',
            **{'name': metadata['Volume'],
            'force': 'true'})
        self.client.invoke_successfully(volume_destroy, True)
        LOG.debug(_("Destroyed volume %s") % name)
        self.volume_table.pop(name)

    def _do_export(self, volume, ensure=False):
        name = self._get_na_name(volume['name'])
        if ensure:
            volume_online = NaElement.create_node_with_children(
                    'volume-online',
                    **{'name': name})
            try:
                self.client.invoke_successfully(volume_online, True)
            except NaApiError as e:
                if "already online" not in e.message:
                    raise
        api = NaElement.create_node_with_children(
            'nfs-exportfs-list-rules',
            **{'pathname': '/vol/%s' % (name)})
        result = self.client.invoke_successfully(api, True)
        export_rules = result.get_child_by_name('rules')
        rules = export_rules.get_children() if export_rules else None
        rule_exists = True if rules else False
        if rule_exists:
            nfs_export = NaElement.create_node_with_children(
                'nfs-exportfs-modify-rule',
                **{'persistent': 'true'})
            rule = NaElement('rule')
        else:
            nfs_export = NaElement.create_node_with_children(
                'nfs-exportfs-append-rules',
                **{'persistent': 'true'})
            rule = NaElement('rules')
        nfs_export.add_child_elem(rule)
        rule_info = NaElement.create_node_with_children(
            'exports-rule-info',
            **{'pathname': '/vol/%s' % (name),
               'anon': '0'})
        rw = NaElement('read-write')
        #root = NaElement('root')
        rw.add_node_with_children(
                'exports-hostname-info',
                **{'all-hosts': 'true'})
        #root.add_node_with_children(
        #        'exports-hostname-info',
        #        **{'all-hosts': 'true'})
        rule_info.add_child_elem(rw)
        #rule_info.add_child_elem(root)
        rule.add_child_elem(rule_info)
        self.client.invoke_successfully(nfs_export, True)
        LOG.debug(_("Export nfs volume /vol/%s") % name)

    def ensure_export(self, context, volume):
        """Driver entry point to get the export info for an existing volume."""
        self._do_export(volume, True)
        handle = self._get_volume_attr(volume['name'], 'handle')
        return {'provider_location': handle}

    def create_export(self, context, volume):
        """Driver entry point to get the export info for a new volume."""
        self._do_export(volume)
        handle = self._get_volume_attr(volume['name'], 'handle')
        return {'provider_location': handle}

    def remove_export(self, context, volume):
        """Driver exntry point to remove an export for a volume."""
        name = self._get_na_name(volume['name'])
        api = NaElement.create_node_with_children(
            'nfs-exportfs-delete-rules',
            **{'persistent': 'true'})
        pathnames = NaElement('pathnames')
        pathnames.add_node_with_children(
            'pathname-info',
            **{'name': '/vol/%s' % (name)})
        api.add_child_elem(pathnames)
        self.client.invoke_successfully(api, True)
        LOG.debug(_("Remove nfs export of volume /vol/%s") % name)

    def initialize_connection(self, volume, connector):
        """Driver entry point to attach a volume to an instance."""
        initiator_name = connector['ip']
        properties = {}
        properties['export'] = volume['provider_location']
        properties['name'] = volume['name']

        return {
            'driver_volume_type': 'nfs',
            'data': properties,
        }

    def create_snapshot(self, snapshot):
        """Driver entry point for creating a snapshot."""
        vol_name = self._get_na_name(snapshot['volume_name'])
        snap_name = self._get_na_name(snapshot['name'])
        volume = self.volume_table[vol_name]
        #self._clone_volume(volume.name, snapshot_name, 'false')
        snapshot_create = NaElement.create_node_with_children(
            'snapshot-create',
            **{'volume': vol_name,
            'snapshot': snap_name})
        self.client.invoke_successfully(snapshot_create, True)

    def delete_snapshot(self, snapshot):
        """Driver entry point for deleting a snapshot."""
        vol_name = self._get_na_name(snapshot['volume_name'])
        snap_name = self._get_na_name(snapshot['name'])
        #self.delete_volume(snapshot)
        snapshot_delete = NaElement.create_node_with_children(
            'snapshot-delete',
            **{'volume': vol_name,
            'snapshot': snap_name})
        try:
            self.client.invoke_successfully(snapshot_delete, True)
        except NaApiError as e:
            if "does not exist" in e.message:
                LOG.info(_("Snapshot %s does not exist") % snap_name)
            else:
                raise
        LOG.debug(_("Snapshot %s deletion successful") % snapshot['name'])

    def create_volume_from_snapshot(self, volume, snapshot):
        """Driver entry point for creating a new volume from a snapshot."""
        vol_size = volume['size']
        snap_size = snapshot['volume_size']
        if vol_size != snap_size:
            msg = _('Cannot create volume of size %(vol_size)s from '
                    'snapshot of size %(snap_size)s')
            raise exception.VolumeBackendAPIException(data=msg % locals())
        name = self._get_na_name(snapshot['volume_name'])
        new_name = self._get_na_name(volume['name'])
        snap_name = self._get_na_name(snapshot['name'])
        self._clone_volume(name, new_name, snap_name)

    def terminate_connection(self, volume, connector, **kwargs):
        """Driver entry point to unattach a volume from an instance."""
        pass

    def _get_ontapi_version(self):
        """Gets the supported ontapi version."""
        ontapi_version = NaElement('system-get-ontapi-version')
        res = self.client.invoke_successfully(ontapi_version, False)
        major = res.get_child_content('major-version')
        minor = res.get_child_content('minor-version')
        return (major, minor)

    def _get_na_name(self, name):
        return name.replace('-','_')

    def _create_volume(self, name, size, metadata):
        """Creates an actual volume on filer."""
        req_size = int(float(size) * 100 / (100 - float(self.configuration.netapp_snapshot_reserve)))
        #volume = self._get_avl_volume_by_size(req_size)
        #if not volume:
        #    msg = _('Failed to get vol with required size for volume: %s')
        #    raise exception.VolumeBackendAPIException(data=msg % name)
        #path = '/vol/%s' % (volume['name'], name)
        volume_create = NaElement.create_node_with_children(
            'volume-create',
            **{'volume': name,
            'containing-aggr-name': self.configuration.netapp_aggregate_name,
            'size': '%d' % (req_size)})
        self.client.invoke_successfully(volume_create, True)
        snapshot_reserve = NaElement.create_node_with_children(
            'snapshot-set-reserve',
            **{'volume': name,
            'percentage': '%d' % (self.configuration.netapp_snapshot_reserve)})
        self.client.invoke_successfully(snapshot_reserve, True)
        metadata['Path'] = '/vol/%s' % (name)
        metadata['Volume'] = name
        metadata['Qtree'] = None

    def _get_avl_volume_by_size(self, size):
        """Get the available volume by size."""
        raise NotImplementedError()

    def _get_iscsi_service_details(self):
        """Returns iscsi iqn."""
        raise NotImplementedError()

    def _get_target_details(self):
        """Gets the target portal details."""
        raise NotImplementedError()

    def _create_volume_handle(self, metadata):
        """Returns volume handle based on filer type."""
        raise NotImplementedError()

    def _get_volume_list(self):
        """Gets the list of luns on filer."""
        raise NotImplementedError()

    def _extract_and_populate_volumes(self, api_volumes):
        """Extracts the volumes from api.
           Populates in the volume table.
        """
        for volume in api_volumes:
            meta_dict = self._create_volume_meta(volume)
            name = volume.get_child_content('name')
            handle = self._create_volume_handle(meta_dict)
            size = volume.get_child_content('size-total')
            discovered_volume = NetAppVolume(handle, name,
                                       size, meta_dict)
            self._add_volume_to_table(discovered_volume)

    def _configure_tunneling(self, do_tunneling=False):
        """Configures tunneling based on system type."""
        raise NotImplementedError()

    def _is_naelement(self, elem):
        """Checks if element is NetApp element."""
        if not isinstance(elem, NaElement):
            raise ValueError('Expects NaElement')

    def _check_allowed_os(self, os):
        """Checks if the os type supplied is NetApp supported."""
        if os in ['linux', 'aix', 'hpux', 'windows', 'solaris',
                  'netware', 'vmware', 'openvms', 'xen', 'hyper_v']:
            return True
        else:
            return False

    def _get_qos_type(self, volume):
        """Get the storage service type for a volume."""
        type_id = volume['volume_type_id']
        if not type_id:
            return None
        volume_type = volume_types.get_volume_type(None, type_id)
        if not volume_type:
            return None
        return volume_type['name']

    def _add_volume_to_table(self, volume):
        """Adds volume to cache table."""
        if not isinstance(volume, NetAppVolume):
            msg = _("Object is not a NetApp volume.")
            raise exception.VolumeBackendAPIException(data=msg)
        self.volume_table[volume.name] = volume

    def _get_volume_from_table(self, name):
        """Gets volume from cache table.

        Refreshes cache if volume not found in cache.
        """
        volume = self.volume_table.get(name)
        if volume is None:
            self._get_volume_list()
            volume = self.volume_table.get(name)
            if volume is None:
                raise exception.VolumeNotFound(volume_id=name)
        return volume

    def _clone_volume(self, name, new_name, snap_name=None):
        """Clone volume with the given name to the new name."""
        raise NotImplementedError()

    def _get_volume_by_args(self, **args):
        """Retrives volume with specified args."""
        raise NotImplementedError()

    def _get_volume_attr(self, name, attr):
        """Get the volume attribute if found else None."""
        try:
            attr = getattr(self._get_volume_from_table(name), attr)
            return attr
        except exception.VolumeNotFound as e:
            LOG.error(_("Message: %s"), e.msg)
        except Exception as e:
            LOG.error(_("Error getting volume attribute. Exception: %s"),
                      e.__str__())
        return None

    def _create_volume_meta(self, volume):
        raise NotImplementedError()

    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume."""
        vol_size = volume['size']
        name = self._get_na_name(src_vref['name'])
        src_vol = self.volume_table[name]
        src_vol_size = src_vref['size']
        if vol_size != src_vol_size:
            msg = _('Cannot clone volume of size %(vol_size)s from '
                    'src volume of size %(src_vol_size)s')
            raise exception.VolumeBackendAPIException(data=msg % locals())
        new_name = self._get_na_name(volume['name'])
        self._clone_volume(name, new_name)

    def get_volume_stats(self, refresh=False):
        """Get volume stats.

        If 'refresh' is True, run update the stats first.
        """

        if refresh:
            self._update_volume_stats()

        return self._stats

    def _update_volume_stats(self):
        """Retrieve stats info from volume group."""
        raise NotImplementedError()

    def _get_volume_options(self, volume_name):
        """Get the value for the volume option."""
        opts = []
        vol_option_list = NaElement("volume-options-list-info")
        vol_option_list.add_new_child('volume', volume_name)
        result = self.client.invoke_successfully(vol_option_list, True)
        options = result.get_child_by_name("options")
        if options:
            opts = options.get_children()
        return opts

    def _get_vol_option(self, volume_name, option_name):
        """Get the value for the volume option."""
        value = None
        options = self._get_volume_options(volume_name)
        for opt in options:
            if opt.get_child_content('name') == option_name:
                value = opt.get_child_content('value')
                break
        return value


class NetAppDirect7modeNfsVolumeDriver(NetAppDirectNfsVolumeDriver):
    """NetApp 7-mode NFS volume driver."""

    def __init__(self, *args, **kwargs):
        super(NetAppDirect7modeNfsVolumeDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(netapp_7mode_opts)

    def _do_custom_setup(self):
        """Does custom setup depending on the type of filer."""
        self.vfiler = self.configuration.netapp_vfiler
        #self.volume_list = self.configuration.netapp_volume_list
        #if self.volume_list:
            #self.volume_list = self.volume_list.split(',')
            #self.volume_list = [el.strip() for el in self.volume_list]
        (major, minor) = self._get_ontapi_version()
        self.client.set_api_version(major, minor)
        if self.vfiler:
            self.client.set_vfiler(self.vfiler)
        self.vol_refresh_time = None
        self.vol_refresh_interval = 1800
        self.vol_refresh_running = False
        self.vol_refresh_voluntary = False
        # Setting it infinite at set up
        # This will not rule out backend from scheduling
        self.total_gb = 'infinite'
        self.free_gb = 'infinite'

    def check_for_setup_error(self):
        """Check that the driver is working and can communicate."""
        api_version = self.client.get_api_version()
        if api_version:
            major, minor = api_version
            if major == 1 and minor < 9:
                msg = _("Unsupported ONTAP version."
                        " ONTAP version 7.3.1 and above is supported.")
                raise exception.VolumeBackendAPIException(data=msg)
        else:
            msg = _("Api version could not be determined.")
            raise exception.VolumeBackendAPIException(data=msg)
        super(NetAppDirect7modeNfsVolumeDriver, self).check_for_setup_error()

    def _get_filer_volumes(self, volume=None):
        """Returns list of filer volumes in api format."""
        vol_request = NaElement('volume-list-info')
        if volume:
            vol_request.add_new_child('volume', volume)
        res = self.client.invoke_successfully(vol_request, True)
        volumes = res.get_child_by_name('volumes')
        if volumes:
            return volumes.get_children()
        return []

    #def _get_avl_volume_by_size(self, size):
    #    """Get the available volume by size."""
    #    vols = self._get_filer_volumes()
    #    for vol in vols:
    #        avl_size = vol.get_child_content('size-available')
    #        state = vol.get_child_content('state')
    #        if float(avl_size) >= float(size) and state == 'online':
    #            avl_vol = dict()
    #            avl_vol['name'] = vol.get_child_content('name')
    #            avl_vol['block-type'] = vol.get_child_content('block-type')
    #            avl_vol['type'] = vol.get_child_content('type')
    #            avl_vol['size-available'] = avl_size
    #            if self.volume_list:
    #                if avl_vol['name'] in self.volume_list:
    #                    return avl_vol
    #            elif self._get_vol_option(avl_vol['name'], 'root') != 'true':
    #                    return avl_vol
    #    return None

    def _create_volume_handle(self, metadata):
        """Returns lun handle based on filer type."""
        if self.vfiler:
            owner = '%s:%s' % (self.configuration.netapp_server_hostname,
                               self.vfiler)
        else:
            owner = self.configuration.netapp_server_hostname
        return '%s:%s' % (owner, metadata['Path'])

    def _get_volume_list(self):
        """Gets the list of volumes on filer."""
        volume_list = []
        api = NaElement('volume-list-info-iter-start')
        result = self.client.invoke_successfully(api, True)
        records = int(result.get_child_content('records'))
        tag = result.get_child_content('tag')
        api = NaElement.create_node_with_children(
                'volume-list-info-iter-next',
                **{'maximum': '20', 'tag': tag})
        result = self.client.invoke_successfully(api, True)
        records = int(result.get_child_content('records'))
        while records:
            volumes = result.get_child_by_name('volumes')
            vols = volumes.get_children()
            for vol in vols:
                name = vol.get_child_content('name')
                vol_contain = NaElement.create_node_with_children(
                        'volume-container',
                        **{'volume': vol.get_child_content('name')})
                result = self.client.invoke_successfully(vol_contain, True)
                contain_aggr = result.get_child_by_name('containing-aggregate')
                aggregate_name = contain_aggr.get_content()
                if aggregate_name == self.configuration.netapp_aggregate_name:
                    volume_list.append(vol)
            api = NaElement.create_node_with_children(
                    'volume-list-info-iter-next',
                    **{'maximum': '20', 'tag': tag})
            result = self.client.invoke_successfully(api, True)
            records = int(result.get_child_content('records'))
        api = NaElement.create_node_with_children(
                'volume-list-info-iter-end',
                **{'tag': tag})
        self.client.invoke_successfully(api, True)
        self._extract_and_populate_volumes(volume_list)

    def _clone_volume(self, name, new_name, snap_name=None):
        """Clone volume with the given handle to the new name."""
        if snap_name:
            clone_start = NaElement.create_node_with_children(
                'volume-clone-create',
                **{'parent-volume': name, 'volume': new_name,
                'parent-snapshot': snap_name})
        else:
            clone_start = NaElement.create_node_with_children(
                'volume-clone-create',
                **{'parent-volume': name, 'volume': new_name})
        result = self.client.invoke_successfully(clone_start, True)
        clone_split = NaElement.create_node_with_children(
            'volume-clone-split-start',
            **{'volume': new_name})
        result = self.client.invoke_successfully(clone_split, True)
        self._check_clone_status(new_name)
        cloned_volume = self._get_volume_by_args(volume=new_name)
        if cloned_volume:
            clone_meta = self._create_volume_meta(cloned_volume)
            handle = self._create_volume_handle(clone_meta)
            self._add_volume_to_table(
                NetAppVolume(handle, new_name,
                          cloned_volume.get_child_content('size'),
                          clone_meta))
        else:
            raise NaApiError('ENOVOLUMEENTRY', 'No volume entry found on the filer')

    #def _set_space_reserve(self, path, enable):
    #    """Sets the space reserve info."""
    #    space_res = NaElement.create_node_with_children(
    #        'lun-set-space-reservation-info',
    #        **{'path': path, 'enable': enable})
    #    self.client.invoke_successfully(space_res, True)

    def _check_clone_status(self, new_name):
        """Checks for the job till completed."""
        clone_status = NaElement.create_node_with_children(
            'volume-clone-split-status',
            **{'volume': new_name})
        running = True
        while running:
            try:
                result = self.client.invoke_successfully(clone_status, True)
                status = result.get_child_by_name('clone-split-details')
                detail_info = status.get_children()
                if detail_info:
                    time.sleep(1)
                else:
                    running = False
            except NaApiError as e:
                if "not a clone" in e.message:
                    running = False
                else:
                    raise

    def _get_aggr_by_args(self, **args):
        """Retrives aggregate with specified args."""
        volume_info = NaElement.create_node_with_children('aggr-list-info', **args)
        result = self.client.invoke_successfully(volume_info, True)
        aggregates = result.get_child_by_name('aggregates')
        if aggregates:
            infos = aggregates.get_children()
            if infos:
                return infos[0]
        return None

    def _get_volume_by_args(self, **args):
        """Retrives volume with specified args."""
        volume_info = NaElement.create_node_with_children('volume-list-info', **args)
        result = self.client.invoke_successfully(volume_info, True)
        volumes = result.get_child_by_name('volumes')
        if volumes:
            infos = volumes.get_children()
            if infos:
                return infos[0]
        return None

    def _create_volume_meta(self, volume):
        """Creates volume metadata dictionary."""
        self._is_naelement(volume)
        meta_dict = {}
        self._is_naelement(volume)
        meta_dict['Path'] = '/vol/%s' % (volume.get_child_content('name'))
        meta_dict['Volume'] = volume.get_child_content('name')
        meta_dict['OsType'] = volume.get_child_content('multiprotocol-type')
        meta_dict['SpaceReserved'] = volume.get_child_content(
            'space-reserve-enabled')
        return meta_dict

    def _configure_tunneling(self, do_tunneling=False):
        """Configures tunneling for 7 mode."""
        if do_tunneling:
            self.client.set_vfiler(self.vfiler)
        else:
            self.client.set_vfiler(None)

    def _update_volume_stats(self):
        """Retrieve status info from volume group."""
        LOG.debug(_("Updating volume stats"))
        data = {}
        netapp_backend = 'NetApp_NFSVolume_7mode_direct'
        backend_name = self.configuration.safe_get('volume_backend_name')
        data["volume_backend_name"] = (
            backend_name or 'NetApp_NFSVolume_7mode_direct')
        data["vendor_name"] = 'NetApp'
        data["driver_version"] = self.VERSION
        data["storage_protocol"] = 'NFS'
        data['reserved_percentage'] = 0
        data['QoS_support'] = False
        self._get_capacity_info(data)
        provide_ems(self, self.client, data, netapp_backend,
                    server_type="7mode")
        self._stats = data

    def _get_capacity_info(self, data):
        """Calculates the capacity information for the filer."""
        if (self.vol_refresh_time is None or self.vol_refresh_voluntary or
                timeutils.is_newer_than(self.vol_refresh_time,
                                        self.vol_refresh_interval)):
            try:
                job_set = set_safe_attr(self, 'vol_refresh_running', True)
                if not job_set:
                    LOG.warn(
                        _("Volume refresh job already running. Returning..."))
                    return
                self.vol_refresh_voluntary = False
                self._refresh_capacity_info()
                self.vol_refresh_time = timeutils.utcnow()
            except Exception as e:
                LOG.warn(_("Error refreshing vol capacity. Message: %s"), e)
            finally:
                set_safe_attr(self, 'vol_refresh_running', False)
        data['total_capacity_gb'] = self.total_gb
        data['free_capacity_gb'] = self.free_gb

    def _refresh_capacity_info(self):
        """Gets the latest capacity information."""
        LOG.info(_("Refreshing capacity info for %s."), self.client)
        total_bytes = 0
        free_bytes = 0
        #vols = self._get_filer_volumes()
        #for vol in vols:
        #    volume = vol.get_child_content('name')
        #    if self.volume_list and not volume in self.volume_list:
        #        continue
        #    state = vol.get_child_content('state')
        #    inconsistent = vol.get_child_content('is-inconsistent')
        #    invalid = vol.get_child_content('is-invalid')
        #    if (state == 'online' and inconsistent == 'false'
        #            and invalid == 'false'):
        #        total_size = vol.get_child_content('size-total')
        #        if total_size:
        #            total_bytes = total_bytes + int(total_size)
        #        avl_size = vol.get_child_content('size-available')
        #        if avl_size:
        #            free_bytes = free_bytes + int(avl_size)
        aggr_info = self._get_aggr_by_args(aggregate=self.configuration.netapp_aggregate_name)
        if aggr_info:
            total_bytes = long(aggr_info.get_child_content('size-total'))
            free_bytes = long(aggr_info.get_child_content('size-available'))
        else:
            raise NaApiError('ENOAGGRENTRY', 'No aggregate entry found on the filer')
        self.total_gb = total_bytes / units.GiB
        self.free_gb = free_bytes / units.GiB
