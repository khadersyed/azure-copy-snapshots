#!/usr/bin/env python

"""
Copyright (c) 2017 Sprinklr Inc.
Primary author: khader.syed@sprinklr.com

Class for Azure Managed Disks
"""

from __future__ import print_function

import time
from datetime import datetime

from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.models import (
    DiskCreateOption,
    StorageAccountTypes
)

from .sdk_auth import AzureSDKAuth

class AzureManagedDisksClient(AzureSDKAuth):
    """ Class for all azure rest calls related to managed disks"""

    def __init__(self):
        """ Init our class using environment variables, assuming they are set """

        super(AzureManagedDisksClient, self).__init__()

        self.__compute_client = ComputeManagementClient(
            self.credentials,
            self.subscription_id
        )

    def list_disks(self, unlocked_disks_only=False):
        """ List all managed disks """
        all_disks = self.__compute_client.disks.list()

        disks_response = []

        for disk in all_disks:
            disk_dict = {}
            disk_dict['name'] = disk.name
            disk_dict['disk_type'] = disk.account_type.value
            disk_dict['location'] = disk.location
            disk_dict['encryption'] = disk.encryption_settings
            disk_dict['disk_size_in_gb'] = disk.disk_size_gb
            disk_dict['resource_group'] = disk.id.split('/')[4].lower()
            try:
                disk_dict['virtual_machine'] = disk.owner_id.split('/')[8]
            except AttributeError:
                disk_dict['virtual_machine'] = None

            disks_response.append(disk_dict)

        if unlocked_disks_only:
            return [disk for disk in disks_response if not disk['virtual_machine']]
        else:
            return disks_response

    def list_unlocked_disks(self):
        """ List all unlocked disks """
        return self.list_disks(True)

    def delete_unlocked_disks(self):
        """ List all managed disks """
        unlocked_disks = self.list_unlocked_disks()

        for disk in unlocked_disks:
            self.delete_disk(disk)

    def delete_disk(self, disk):
        """ Delete a given managed disk """
        print("Deleting: {}".format(disk['name']))
        async_delete_disk = self.__compute_client.disks.delete(
            disk['resource_group'],
            disk['name']
        )

        result = async_delete_disk.result()
        if result.error:
            print(result.error)

    def list_snapshots(self):
        """ List all managed disk snapshots"""
        snapshots_raw = self.__compute_client.snapshots.list()

        snapshots = []
        for snapshot in snapshots_raw:
            snapshot_dict = {}
            snapshot_dict['name'] = snapshot.name
            snapshot_dict['snapshot_type'] = snapshot.account_type.value
            snapshot_dict['location'] = snapshot.location
            snapshot_dict['snapshot_time'] = snapshot.time_created.isoformat()
            snapshot_dict['tags'] = snapshot.tags
            snapshot_dict['resource_group'] = snapshot.id.split('/')[4].lower()

            snapshots.append(snapshot_dict)

        return snapshots

    def delete_old_snapshots(self, age_in_days=None):
        """ Delete all snapshots older then age_in_days """
        time_format = "%Y-%m-%dT%H:%M:%S.%f"
        all_snapshots = self.list_snapshots()

        if not age_in_days:
            age_in_days = 3 # default of 3 days

        for snapshot in all_snapshots:
            snapshot_time = datetime.strptime(snapshot['snapshot_time'][:-6], time_format)
            current_time = datetime.now(snapshot_time.tzinfo)
            age = current_time - snapshot_time
            if age.days > age_in_days:
                self.delete_snapshot(snapshot)

    def delete_snapshot(self, snapshot):
        """ Deletes a snapshot when the snapshot id is provided """
        print(snapshot['name'], snapshot['snapshot_time'])

        async_delete_snapshot = self.__compute_client.snapshots.delete(
            snapshot['resource_group'],
            snapshot['name']
        )
        result = async_delete_snapshot.result()
        if result.error:
            print(result.error)

    def list_snapshots_for_vm(self, vm_name=None, disk_name=None):
        """ Get list of snapshots for a specific vm"""
        snapshots_raw = self.__compute_client.snapshots.list()

        vm_snapshots = [snapshot for snapshot in snapshots_raw
                        if snapshot.tags['vm_name'] == vm_name]

        vm_disks = {snapshot.tags['disk_name'] for snapshot in vm_snapshots}

        snapshots_for_vm = {}
        for disk in vm_disks:
            snapshots_for_vm[disk] = []

        for snapshot in vm_snapshots:
            snapshot_info = {}
            snapshot_info['name'] = snapshot.name
            snapshot_info['id'] = snapshot.id
            snapshot_info['location'] = snapshot.location
            snapshot_info['mount_point'] = snapshot.tags['mount_point']
            snapshot_info['snapshot_time'] = snapshot.time_created.isoformat()
            snapshot_info['resource_group'] = snapshot.id.split('/')[4].lower()
            snapshot_info['disk_size_in_gb'] = snapshot.disk_size_gb

            snapshots_for_vm[snapshot.tags['disk_name']].append(snapshot_info)

        if not disk_name:
            return snapshots_for_vm
        else:
            return snapshots_for_vm[disk_name]

    def generate_sas_uri(self, resource_group_name, snapshot_name, expiry_in_seconds=86400):
        """ Generates a SAS URI that can be used to copy to storage accounts """
        access_type = "Read"
        async_generate_uri = self.__compute_client.snapshots.grant_access(
            resource_group_name,
            snapshot_name,
            access_type,
            expiry_in_seconds
        )

        return async_generate_uri

    def get_snapshot_sas_uris(self, uri_expiry_time_in_seconds=86400):
        """ Method returns sas uris for all managed disk snapshots """
        time_format = "%Y-%m-%dT%H:%M:%S.%f"
        snapshot_age_in_days = 1 # we want to only copy snapshots that are less than a day old

        snapshot_copy_tracker = []
        for snapshot in self.list_snapshots():
            snapshot_name = snapshot['name']
            resource_group_name = snapshot['resource_group']

            snapshot_time = datetime.strptime(snapshot['snapshot_time'][:-6], time_format)
            current_time = datetime.now(snapshot_time.tzinfo)
            snapshot_age = current_time - snapshot_time

            snapshot_info = {}
            if snapshot_age.days < snapshot_age_in_days:
                snapshot_info['name'] = snapshot_name
                snapshot_info['sas_generate_start_time'] = datetime.utcnow()
                snapshot_info['tags'] = snapshot['tags']
                sas_uri_wait = self.generate_sas_uri(
                    resource_group_name,
                    snapshot_name,
                    uri_expiry_time_in_seconds
                )
                snapshot_info['async_sas_uri_object'] = sas_uri_wait
                snapshot_copy_tracker.append(snapshot_info)

        while True:
            remaining_count = 0
            for index, snapshot_info in enumerate(snapshot_copy_tracker):
                if not 'sas_uri' in snapshot_info:
                    if snapshot_info['async_sas_uri_object'].done():
                        sas_uri = snapshot_info['async_sas_uri_object'].result().access_sas
                        snapshot_copy_tracker[index]['sas_uri'] = sas_uri
                        snapshot_copy_tracker[index]['sas_generate_end_time'] = datetime.utcnow()
                    else:
                        remaining_count += 1
                time.sleep(2)
            if not remaining_count:
                print("sas uris generated.")
                break

        for index, snapshot in enumerate(snapshot_copy_tracker):
            snapshot_copy_tracker[index].pop('async_sas_uri_object')

        return snapshot_copy_tracker

    def vhd_snapshot(self, resource_group=None, vm_name=None, disk=None):
        """ Attached a restored disk to a VM """
        virtual_machine = self.__compute_client.virtual_machines.get(
            resource_group,
            vm_name
        )

        vm_luns = [disk.lun for disk in virtual_machine.storage_profile.data_disks]
        next_lun = max(vm_luns)+1

        virtual_machine.storage_profile.data_disks.append({
            'lun': next_lun,
            'name': disk.name,
            'create_option': DiskCreateOption.attach,
            'managed_disk': {
                'id': disk.id
            }
        })
