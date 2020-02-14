'''
Description: Pull user-specified data from the NHL-records site.

Return player- and team-related data by accessing the NHL website's
API.
'''

__version__ = '2.0'
__title__ = 'nhl_pull_data'
__author__ = 'Paul Hegedus'

import os
import sys
import requests
import psycopg2
import argparse
import pandas as pd
import pdb
#import subprocess
#import numpy as np
#import matplotlib.pyplot as plt

from configparser import ConfigParser
from pprint import pprint

def argsetup():
    '''
    Setup command line argument parser to read in config file.
    '''

    parser = argparse.ArgumentParser(description =
                'Read in player/team data from the NHL\'s website.')
    parser.add_argument('configf', help='configuration file')
    a = parser.parse_args()
    return a

def database_connect():
    '''
    Setup connection to the PostgreSQL database and 
    '''

    connection = psycopg2.connect(
                user = db_user,
                password = db_passwd,
                host = db_host,
                database = db_name
    )

    return connection

def request_data(url):
    '''
    Request data from specified URL pointing to a NHLStats API endpoint.

    The requested dataset is a dict containing key-value pairs:
        i.e.  {
                'copyright': 'NHL and the NHL Shield are registered...',
                'teams': [{team_data}, {team_data}, ... {team_data}]
              }
    This function returns the full dict, it does not separate out the actual 
    data found in the list of dicts that makes up the value from a key-value
    pair after the copyright info.
    '''

    r = requests.get(url)
    if r.status_code == 200:
        # successful request, return data
        return r.json()
    else:
        # print exception message since request failed
        r.raise_for_status()
        sys.exit('Exiting after failing to pull data from %s' % url)
        return None

def _teams(url):
    '''
    Overall function to get full dataset on all NHL teams, then parse down
    data to only what is required for our database.
    '''

    # actually get team data from NHL site
    team_dataset = request_data(url)
    # pull list of team data from returned JSON object
    for key in team_dataset.keys():
        if key == 'teams':
            team_list = team_dataset[key]
    # can now cycle thru each individual team
    for _ in team_list:
        # extract only some of the provided team data for each team
        # NOTE: Store data immediately for each team? Return list of data?...
        parse_teams(_)

def parse_teams(data):
    '''
    Parse the NHL team dataset to get data required for database, which is the following:
        - team_id
        - name
        - abbreviation
        - conference_id
        - division_id
        - franchise_id
        - active

    Inputted dataset is a dict specifying info for one specific NHL team.

    Return value TBD...may just store in database straight away...maybe call
    external function to convert to Pandas DataFrame and store in DB?
    '''

    # pdb.set_trace()
    # set variables for requisite data to pull (using inputted dict)
    team_id = data['id']
    team_name = data['name']
    abbreviation = data['abbreviation']
    conference_id = data['conference']['id']
    division_id = data['division']['id']
    franchise_id = data['franchise']['franchiseId']
    active = data['active']

    # store team data using database_connect()
    #    -new function that does something like conn = database_connect()
    #       to create a new connection. want to be able to do that easily so
    #       we can replicate it quickly and easily

    pprint('%s (%s): %s' % (team_name, abbreviation, team_id))
    pprint('Conf: %s ; Div: %s ; Franchise: %s ; Active? %s' %
                (conference_id, division_id, franchise_id, active))

#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

if __name__ == '__main__':
    # set variable with curr dir
    cwd = os.getcwd()

    # get command-line arguments
    args = argsetup()
    conf_file = args.configf

    # read in configuration file
    config = ConfigParser()
    config.read(conf_file)

    # get API endpoints from config file
    nhl_teams = config['LINKS']['teams']
    nhl_players = config['LINKS']['players']
    # get database info from config file
    db_user = config['DATABASE']['USER']
    db_passwd = config['DATABASE']['PASSWORD']
    db_host = config['DATABASE']['CONNECTION']
    db_name = config['DATABASE']['DB_NAME']

    # initiate data getting/parsing functions
    _teams(nhl_teams)

    # store the output