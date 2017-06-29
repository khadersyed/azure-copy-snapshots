#!/usr/bin/env python

"""
Copyright (c) 2017 Sprinklr Inc.
Primary author: khader.syed@sprinklr.com

To manipulate most things to do with Azure Managed Disks
"""

from __future__ import print_function

import time
from datetime import datetime

from azure.mgmt.compute import ComputeManagementClient

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

    def vhd_snapshot(self, snapshot_info):
        """ Attached a restored disk to a VM """
        blob_uri = "https://{}.blob.core.windows.net/{}/{}".format(
            snapshot_info['dest_storage_account'],
            snapshot_info['dest_container'],
            snapshot_info['dest_blob']
        )

        print("Snapshot {} from blob {}".format(snapshot_info['tags']['disk_name'], blob_uri))
        async_vhd_snapshot = self.__compute_client.snapshots.create_or_update(
            snapshot_info['dest_resource_group'],
            snapshot_info['tags']['disk_name'],
            {
                'location': snapshot_info['location'],
                'creation_data': {
                    'create_option': 'Copy',
                    'blob_uri': blob_uri
                }
            }
        )

        return async_vhd_snapshot.result()
