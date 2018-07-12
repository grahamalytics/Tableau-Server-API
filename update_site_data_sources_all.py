# -*- coding: utf-8 -*-
"""
Created on Wed May 30 17:01:57 2018

@author: glandry
"""
import argparse
import getpass
import sys
import re

import tableauserverclient as tsc


def main():
    #########################################
    # Create / Parse Command Line Arguments #
    #########################################
    # create parser object to handle program arguments
    parser = argparse.ArgumentParser(description='''Change owner and update connection details across ALL Oracle data
                                                    sources within a site on Tableau Server.''')

    # define command line arguments
    parser.add_argument("--tabserver", "-t", required=True,
                        help="Target Tableau Server URL")
    parser.add_argument("--site", "-s", required=True,
                        help="Target site for updating data source owner and connection credentials")
    parser.add_argument("--function", "-f", required=True, choices=["both", "conn"],
                        help="Option for changing BOTH data source owner and connection credentials, or just CONNection credentials.")

    # parse arguments from user
    args = parser.parse_args()

    # get Tableau Server login credentials from user
    user = input(">>>> Tableau Server username: ")
    pw = getpass.getpass(">>>> Tableau Server Password: ")

    # if user selects option to change both data source owner and connection details, we need to get the username of
    # the new datasource owner in order to make the change
    if args.function == "both":
        new_owner = input(">>>> Username of new data source owner: ")

    # get Data Source credentials from user
    ds_user = input("\n>>>> Data source connection username: ")
    ds_pw = getpass.getpass(">>>> Data source connection password: ")

    ############################
    # Log-in to Tableau Server #
    ############################
    # create tableau_auth and server objects prior to sign-in
    tableau_auth = tsc.TableauAuth(user, pw, site_id=args.site)

    # attempt to login to Tableau Server with URL and credentials specified by user
    print("\n>>>> Attempting Login to {0} site within Tableau Server at {1} .....".format(args.site, args.tabserver))

    try:
        server = tsc.Server(args.tabserver)
        server.auth.sign_in(tableau_auth)
        print(">>>> SUCCESS")

        server.use_server_version()
    except:
        raise

    #################################################################################
    # API CALL: get list of Tableau Server users & data sources for particular site #
    #################################################################################
    print("\n>>>> Attempting to query users and data sources for {0} site".format(args.site))

    try:
        # query list of users with access to site
        site_users, user_pagination = server.users.get()

        # in order to access owner_id, populate workbooks association with user
        for user in site_users:
            server.users.populate_workbooks(user)

        # query list of data sources within the site
        site_datasources, site_pagination = server.datasources.get()

        # in order to access DB connection info populate connections associated with data source
        for ds in site_datasources:
            server.datasources.populate_connections(ds)
    except:
        raise

    ############################
    # Get user_id of NEW owner #
    ############################
    # filter site_users for new data source owner then use regex to grab user_id
    new_owner_query = list(filter(lambda x: x.name == new_owner, site_users))

    # if new_owner_query is empty, target owner does not belong to site and program exits
    if len(new_owner_query) == 0:
        print("Target data source owner does not exist as user in {site}".format(site=args.site))
        sys.exit(1)
    elif len(new_owner_query) == 1:
        # compile re object which matches the response for "<User [user_id] "pattern
        p = re.compile("<User\s[a-zA-Z0-9-]*\s")

        # use the re object to extract portion of response containing user_id as a string
        new_owner_id = p.findall(str(new_owner_query[0]))[0]

        # parse the string so as to isolate only the user_id
        new_owner_id = new_owner_id.strip()  # remove leading/trailing whitespace
        new_owner_id = new_owner_id.split(" ")[1]  # split the string and grab user_id (element in 1st position)

        print(">>>> SUCCESS")

    else:
        print("New owner lookup returned multiple users, exiting program.")
        sys.exit(1)

    ############################################################
    # Change BOTH Data Source Owner and Connection Credentials #
    ############################################################
    if args.function == "both":
        ###############################################################
        # API CALL: update data source owner & connection information #
        ###############################################################
        print("\n>>>> Attempting to update BOTH data source owners and connection credentials")
        try:
            i = 0
            for ds in site_datasources:
                connection = ds.connections[0]

                # ensure that we"re only changing datsources which connect to database and do not already belong to new owner
                if ds.datasource_type.lower() == "oracle":
                    # change data source owner
                    ds.owner_id = new_owner_id
                    server.datasources.update(ds)

                    # change data source connection info, making sure to embed password
                    connection.username = ds_user
                    connection.password = ds_pw
                    connection.embed_password = True

                    # commit updates to data source connections
                    server.datasources.update_connection(ds, connection)

                    # keep count of how many data sources we"ve changed
                    i += 1
            print(">>>> SUCCESS: {0} of {1} data sources changed on {2} site".format(i, site_pagination.total_available, args.site))
        except:
            raise

    ##################################################
    # Change ONLY Data Source Connection Credentials #
    ##################################################
    else:
        print("\n>>>> Attempting to update ONLY data source connection credentials")
        try:
            i = 0
            for ds in site_datasources:
                connection = ds.connections[0]

                # ensure that we"re only changing datsources which connect to database and do not already belong to new owner
                if ds.datasource_type.lower() == "oracle":
                     # change data source connection info, making sure to embed password
                    connection.username = ds_user
                    connection.password = ds_pw
                    connection.embed_password = True

                    # commit updates to data source connections
                    server.datasources.update_connection(ds, connection)

                    # keep count of how many data sources we"ve changed
                    i += 1
            print(">>>> SUCCESS: {0} of {1} data sources changed on {2} site".format(i, site_pagination.total_available, args.site))
        except:
            raise

    #############################
    # Log-out of Tableau Server #
    #############################
    print("\n>>>> Logging out of Tableau Server and exiting program")
    server.auth.sign_out()
    sys.exit(1)


if __name__ == "__main__":
    main()