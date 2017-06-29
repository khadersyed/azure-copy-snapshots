#!/usr/bin/env python

"""
Copyright (c) 2017 Sprinklr Inc.
Authors & Contributors:
    khader.syed@sprinklr.com

To manipulate most things to do with Azure Storage Accounts
"""

from __future__ import print_function

from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import PageBlobService

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

        self.storage_accounts = self.list_storage_accounts()

    def list_storage_accounts(self):
        """ Get a list of storage accounts and the resource groups they are in """
        storage_accounts = {}
        for storage_account in self.__storage_client.storage_accounts.list():
            sa_dict = {}
            sa_dict['location'] = storage_account.location
            sa_dict['resource_group'] = storage_account.id.split('/')[4]

            storage_accounts[storage_account.name] = sa_dict

        return storage_accounts

    def __get_storage_key(self, storage_account_name=None):
        """ Method to return key given a storage account and resource group """
        resource_group_name = self.storage_accounts['storage_account_name']['resource_group']

        storage_account_keys = self.__storage_client.storage_accounts.list_keys(
            resource_group_name,
            storage_account_name
        ).keys

        for key in storage_account_keys:
            if key.key_name == 'key1':
                primary_key = key.value

        return primary_key

    def get_blob_service(self, storage_account_name):
        """ Returns storage blob service object for a given resource group and storage account """
        storage_account_key = self.__get_storage_key(storage_account_name)

        return PageBlobService(storage_account_name, storage_account_key)

    def get_blob_copy_status(self, storage_account_name,
                             container_name, blob_name):
        """ Method to get the copy status of a blob """
        blob_service = self.get_blob_service(
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

    @staticmethod
    def get_blob_size_in_bytes(blob_name, container_name, blob_service):
        """ Returns the size used by a blob in bytes """
        blob_size_in_bytes = 124 + len(blob_name) * 2

        metadata = blob_service.get_blob_metadata(container_name, blob_name)

        for key, value in metadata.items():
            blob_size_in_bytes += 3 + len(key) + len(value)

        page_ranges = blob_service.get_page_ranges(container_name, blob_name)
        for page_range in page_ranges:
            blob_size_in_bytes += 12 + page_range.end - page_range.start

        return blob_size_in_bytes
