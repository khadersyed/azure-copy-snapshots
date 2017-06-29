# Copy Snapshots to Another Subscription in Azure

## How does it work?

### To create sas uris and copy snapshots, do the following
```
copy_snapshots.py -i <destination_subscription_id> -k localhost -n mysnapshotstore
```

### To check the copy status and update elasticsearch on successful copy
```
copy_snapshots.py -i <destination_subscription_id> -k localhost -n mysnapshotstore -c
```

### What do I need?

You need Python 2.7, Azure Python SDK >=2.0.0 and Elasticsearch setup locally
or remotely

## How does this work?

RTFC as they would say, in ye olde days

Or alternately, let me explain, as one should:

* We get a list of all current snapshots
* Then we create sas uris for all of the aforementioned snapshots
* We ship/copy the blobs to the other subscription - which could have a
  storage account in a different region than the one in the source region
* We store all the data about the snapshots we are copying, the snapshot start
  time and everything else in the elasticsearch index called `backup_copies`
* To check the status of the snapshot copies, and update the elasticsearch
  index for any finished copies, pass the `-c` option
* On a successful copy, a disk snapshot is triggered and the copied blob/vhd is
  deleted
* And that's basically it...maybe

Remember to test the restoration of your backups, folks!!
