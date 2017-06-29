#!/usr/bin/env python

"""
Copyright (c) 2017 Sprinklr Inc.
Primary author: khader.syed@sprinklr.com

Script to copy managed disk snapshots to secondary region
and update information about copied snapshots to elasticsearch
"""

from __future__ import print_function

import argparse
from datetime import datetime

import elasticsearch
from elasticsearch.helpers import scan

from sprazure.managed_disks import AzureManagedDisksClient
from sprazure.storage_accounts import AzureStorageAccountsClient

from azure.common import AzureMissingResourceHttpError

ES_INDEX = "backup_copies"
CONTAINER_NAME = "snapshots"

def parse_cli_arguments():
    """ Returns response based on arguments """
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--destination-subscription-id",
                        help="ID of the destination subscription", required=True)
    parser.add_argument("-n", "--destination-account-name",
                        help="Destination storage account name")
    parser.add_argument("-k", "--ops-es-host",
                        help="IP or Hostname of Elasticsearch host", required=True)
    parser.add_argument("-c", "--check-copy-status", action="store_true",
                        help="Check status of any pending copies and update Elasticsearch")

    args = parser.parse_args()

    # we need an elasticsearch host - to add state
    # or at the very least check prior copy status
    es_host = args.ops_es_host
    subscription_id = args.destination_subscription_id
    storage_account_name = args.destination_account_name

    es_conn = elasticsearch.Elasticsearch(
        [{'host':es_host},],
        timeout=300,
        retry_on_timeout=True,
    )

    if args.check_copy_status:
        check_copy_status(subscription_id, storage_account_name, es_conn)
    elif args.destination_account_name:
        managed_disk_client = AzureManagedDisksClient()

        snapshot_sas_uris = managed_disk_client.get_snapshot_sas_uris()
        copy_snapshots(
            subscription_id,
            storage_account_name,
            snapshot_sas_uris,
            es_conn
        )
    else:
        parser.print_help()

def copy_snapshots(subscription_id, storage_account_name,
                   snapshot_copy_data, es_conn):
    """ This here method will copy snapshots to storage accounts in another subscription """
    storage_accounts_client = AzureStorageAccountsClient(subscription_id)
    dest_storage_account_info = storage_accounts_client.storage_accounts[storage_account_name]
    dest_location = dest_storage_account_info['location']
    dest_resource_group = dest_storage_account_info['resource_group']

    dest_blob_service = storage_accounts_client.get_blob_service(
        storage_account_name
    )

    try:
        dest_blob_service.get_container_properties(CONTAINER_NAME)
    except AzureMissingResourceHttpError:
        dest_blob_service.create_container(CONTAINER_NAME)

    epoch_time = datetime.now().strftime('%s')

    for index, snapshot in enumerate(snapshot_copy_data):
        doc_type = snapshot['tags']['service']
        doc_id = snapshot['name']

        # check if this snapshot entry exists in es already
        try:
            es_conn.get(
                index=ES_INDEX,
                doc_type=doc_type,
                id=doc_id
            )
        except elasticsearch.exceptions.NotFoundError:
            # use source disk name to create destination vhs name
            dest_blob_name = "{}-{}-{}.vhd".format(
                snapshot['tags']['vm_name'],
                snapshot['tags']['mount_point'],
                epoch_time
            )
            snapshot_copy_data[index]['dest_blob'] = dest_blob_name
            snapshot_copy_data[index]['dest_storage_account'] = storage_account_name
            snapshot_copy_data[index]['dest_container'] = CONTAINER_NAME
            snapshot_copy_data[index]['dest_subscription_id'] = subscription_id
            snapshot_copy_data[index]['dest_location'] = dest_location
            snapshot_copy_data[index]['dest_resource_group'] = dest_resource_group

            dest_blob_service.copy_blob(
                CONTAINER_NAME,
                dest_blob_name,
                snapshot['sas_uri'],
                metadata=snapshot['tags']
            )
            snapshot_copy_data[index]['snapshot_copy_start_time'] = datetime.utcnow()
            snapshot_copy_data[index]['snapshot_copy_status'] = "pending"
            snapshot_copy_data[index].pop('sas_uri')

            # push this dictionary to the es host
            es_conn.index(
                index=ES_INDEX,
                doc_type=doc_type,
                id=doc_id,
                body=snapshot_copy_data[index]
            )

def check_copy_status(subscription_id, storage_account_name, es_conn):
    """
    This method checks for any snapshots that haven't been updated,
    checks their current status and updates Elasticsearch
    """
    time_format = "%Y-%m-%dT%H:%M:%S.%f" # time_format to convert string back to datetime
    es_conn.indices.refresh(index=ES_INDEX)

    # let's get all documents in the index which are pending
    pending_copies = scan(
        es_conn,
        index=ES_INDEX,
        query={"query": {"match": {'snapshot_copy_status':'pending'}}}
    )

    storage_accounts_client = AzureStorageAccountsClient(subscription_id)
    managed_disks_client = AzureManagedDisksClient(subscription_id)
    dest_blob_service = storage_accounts_client.get_blob_service(storage_account_name)

    for copy in pending_copies:
        snapshot_copy_info = copy['_source']

        copy_status_details = storage_accounts_client.get_blob_copy_status(
            snapshot_copy_info['dest_storage_account'],
            snapshot_copy_info['dest_container'],
            snapshot_copy_info['dest_blob']
        )

        if copy_status_details:
            (blob_size, copy_status, copy_last_modified) = copy_status_details
            snapshot_copy_info['snapshot_copy_end_time'] = copy_last_modified.strftime(time_format)
            snapshot_copy_info['snapshot_copy_status'] = copy_status
            snapshot_copy_info['snapshot_blob_size_in_bytes'] = blob_size

            copy_start_time = (
                datetime.strptime(snapshot_copy_info['snapshot_copy_start_time'], time_format)
            )
            snapshot_copy_time = copy_last_modified.replace(tzinfo=None) - copy_start_time

            snapshot_copy_info['snapshot_copy_time_in_seconds'] = (
                (snapshot_copy_time.days*86400)
                +snapshot_copy_time.seconds
                +(snapshot_copy_time.microseconds/(10**6))
            )

            print(copy['_id'])
            es_conn.index(
                index=ES_INDEX,
                doc_type=copy['_type'],
                id=copy['_id'],
                body=snapshot_copy_info
            )
            if copy_status == "success":
                vhd_snapshot = managed_disks_client.vhd_snapshot(snapshot_copy_info)
                print("Snapshot created - {}".format(vhd_snapshot.name))
                dest_blob_service.delete_blob(CONTAINER_NAME, snapshot_copy_info['dest_blob'])


if __name__ == "__main__":
    parse_cli_arguments()
