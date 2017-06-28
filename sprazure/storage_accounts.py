#!/usr/bin/env python

"""
Copyright (c) 2017 Sprinklr Inc.
Authors & Contributors:
    khader.syed@sprinklr.com

To manipulate most things to do with Azure Storage Accounts
"""

from __future__ import print_function

import sys
import time
import socket
from datetime import datetime, timedelta

from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import (
    PageBlobService,
    ContainerPermissions
)

from msrestazure.azure_exceptions import CloudError
from azure.common import AzureConflictHttpError

from .sdk_auth import AzureSDKAuth

class AzureStorageAccountsClient(AzureSDKAuth):
    """ For all storage account related actions """

    def __init__(self, subscription_id=None):
        """ Init this class with credentials and other riff raff"""

        super(AzureStorageAccountsClient, self).__init__()

        if not subscription_id:
            subscription_id = self.subscription_id

        self.__storage_client = StorageManagementClient(
            self.credentials,
            subscription_id
        )

        self.storage_accounts()
        self.time_format = "%Y-%m-%dT%H:%M:%S.%f"

    def list_storage_accounts(self):
        """ Get a list of storage accounts and the resource groups they are in """
        storage_accounts = []
        for storage_account in self.__storage_client.storage_accounts.list():
            sa_dict = {}
            sa_dict[storage_account.name] = {}
        
    def get_resource_group(self, storage_account_name=None):
        """ Returns resource group for a given storage account name """
        storage_account_ids = [s.id for s in self.storage_accounts if s.name == storage_account_name]

        for s_id in storage_account_ids:
            return s_id.split('/')[4]

    def __get_storage_key(self, storage_account=None):
        """ Method to return key given a storage account and resource group """
        resource_group_name = storage_account.id.split('/')[4]
        try:
            storage_account_keys = self.__storage_client.storage_accounts.list_keys(
                resource_group_name,
                storage_account.name
            ).keys
        except CloudError:
            print("Error with storage account: {}".format(
                storage_account.name))
        for key in storage_account_keys:
            if key.key_name == 'key1':
                primary_key = key.value

        return primary_key

    def list_unlocked_blobs(self, delete_blobs=False):
        """ List unlocked/unused and available disks"""
        for storage_account in self.storage_accounts:
            if ('backupstore' in storage_account.name or
                    'blobstore' in storage_account.name or
                    'snapshots' in storage_account.name):
                continue

            primary_key = self.__get_storage_key(storage_account)
            page_blob_service = PageBlobService(
                storage_account.name, primary_key)
            for container in page_blob_service.list_containers():
                marker = None
                blobs = []
                while True:
                    batch = page_blob_service.list_blobs(
                        container.name, marker=marker)
                    blobs.extend(batch)
                    if not batch.next_marker:
                        break
                    marker = batch.next_marker
                for blob in blobs:
                    if (blob.name.endswith("vhd") and
                            blob.properties.lease.status == "unlocked" and
                            delete_blobs):
                        print("Deleting unlocked disk {}/vhds/{}".format(
                            storage_account.name, blob.name))
                        try:
                            page_blob_service.delete_blob(container.name, blob.name)
                        except AzureConflictHttpError as blob_delete_error:
                            print(blob_delete_error)

    def delete_unlocked_blobs(self):
        """ List all managed disks """
        self.list_unlocked_blobs(delete_blobs=True)

    def __delete_storage_account(self, storage_account=None):
        """ Delete a storage account """
        resource_group_name = storage_account.id.split('/')[4]

        self.__storage_client.storage_accounts.delete(
            resource_group_name, storage_account.name)

    @staticmethod
    def __get_source_blob_url_future(blob_service, container_name, blob_name):
        sas_token = blob_service.generate_container_shared_access_signature(
            container_name,
            ContainerPermissions.READ,
            datetime.utcnow() + timedelta(hours=24),
        )
        return blob_service.make_blob_url(container_name, blob_name, sas_token=sas_token)

    @staticmethod
    def __get_sas_token(blob_service, container_name):
        return blob_service.generate_container_shared_access_signature(
            container_name,
            ContainerPermissions.READ,
            datetime.utcnow() + timedelta(hours=24)
        )

    @staticmethod
    def __get_source_blob_url(disk_url, snapshot_time, sas_token):
        """ Returns source blob url for snapshots included"""
        return '{}?snapshot={}&{}'.format(disk_url, snapshot_time, sas_token)

    def get_blob_service(self, resource_group_name, storage_account_name):
        """ Returns storage blob service object for a given resource group and storage account """
        storage_account = self.__storage_client.storage_accounts.get_properties(
            resource_group_name,
            storage_account_name
        )
        storage_account_key = self.__get_storage_key(storage_account)

        return PageBlobService(storage_account_name, storage_account_key)

    def get_blob_copy_status(self, resource_group_name, blob_name,
                             container_name, storage_account_name):
        """ Method to get the copy status of a blob """
        blob_service = self.get_blob_service(
            resource_group_name,
            storage_account_name
        )

        blob_properties = blob_service.get_blob_properties(
            container_name,
            blob_name
        )

        if blob_properties.properties.copy.status != "pending":
            blob_size = self.get_blob_size_in_bytes(
                blob_name,
                container_name,
                blob_service
            )
            return (
                blob_size,
                blob_properties.properties.copy.status,
                blob_properties.properties.last_modified
            )
        else:
            return None

    def get_blob_size_in_bytes(self, blob_name, container_name, blob_service):
        """ Returns the size used by a blob in bytes """
        blob_size_in_bytes = 124 + len(blob_name) * 2

        metadata = blob_service.get_blob_metadata(container_name, blob_name)

        for key, value in metadata.items():
            blob_size_in_bytes += 3 + len(key) + len(value)

        page_ranges = blob_service.get_page_ranges(container_name, blob_name)
        for page_range in page_ranges:
            blob_size_in_bytes += 12 + page_range.end - page_range.start

        return blob_size_in_bytes
