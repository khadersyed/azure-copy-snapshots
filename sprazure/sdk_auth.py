#!/usr/bin/env python

"""
Copyright (c) 2017 Sprinklr Inc.
Primary author: khader.syed@sprinklr.com

Azure REST API Authentication
"""

from __future__ import print_function

import os
import sys

from azure.common.credentials import ServicePrincipalCredentials

class AzureSDKAuth(object):
    """ Sets Authorization Header"""

    def __init__(self):
        """ Init this class with credentials and other riff raff"""
        # args = self.parse_cli_arguments()

        try:
            self.credentials = ServicePrincipalCredentials(
                client_id=os.environ["AZURE_CLIENT_ID"],
                secret=os.environ["AZURE_SECRET"],
                tenant=os.environ["AZURE_TENANT"]
            )
            self.subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
        except KeyError:
            print("\nOh no! One or more of the following variables failed to load")
            print("AZURE_CLIENT_ID, AZURE_SECRET, AZURE_TENANT, AZURE_SUBSCRIPTION_ID\n")
            sys.exit(1)
