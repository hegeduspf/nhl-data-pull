'''
Description: Store team/player data from the NHL website in a database.

This program pulls team and player data from the NHL website's publicly
available API. It uses a configuration file read in from the command line
to request the data from the API and establish a connection to the requisite
database.
'''

__version__ = '2.0'
__title__ = 'nhl_data_pull'
__author__ = 'Paul Hegedus'

import os
import sys
import requests
import psycopg2
import argparse
import pandas as pd
import pdb
#import numpy as np
#import matplotlib.pyplot as plt

from configparser import ConfigParser
from collections import OrderedDict
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
    Setup connection to a PostgreSQL database using psycopg2 package.

    Database credentials are read in from the configuration file passed to
    the progam and set in main.
    '''

    connection = None
    try:
        # establish database connection
        connection = psycopg2.connect(
                    user = db_user,
                    password = db_passwd,
                    host = db_host,
                    database = db_name
        )
    except psycopg2.DatabaseError as e:
        # Report error
        pprint(e)
        sys.exit()

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
        sys.exit(f"Exiting after failing to pull data from {url}")
        return None

def _teams(url):
    '''
    Overall function to get complete dataset on all NHL teams, then parse down
    data to only what is required and store in our database.
    '''

    # get raw team data from NHL site
    team_dataset = request_data(url)

    # pull list of team data from returned JSON object/ignore copyright info
    for key in team_dataset.keys():
        if key == 'teams':
            team_list = team_dataset[key]

    # can now cycle thru each individual team
    for _ in team_list:
        #pdb.set_trace()
        insert_cmd = (
            f"INSERT INTO teams (id, name, abbreviation, conf_id, division_id," 
            f" franchise_id, active)"
            f" VALUES {parse_teams(_)}"
        )
        # load parsed team data into database using established connection
        store_team(db_connect, insert_cmd)

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

    # pprint(f"{team_name} ({abbreviation}): {team_id}")
    # pprint(
    #     f"Conference: {conference_id}; Division: {division_id}; "
    #     f"Franchise: {franchise_id}; Active: {active}"
    # )

    return team_id, team_name, abbreviation, conference_id, division_id, \
        franchise_id, active
    

def store_team(conn, cmd):
    '''
    Execute an SQL query using an established database connection.

    conn -> preexisting database connection [(i.e. a connection setup using 
        nhl_data_pull.database_connect()]
    cmd  -> SQL command to execute
    '''

    cursor = conn.cursor()
    try:
        cursor.execute(cmd)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        pprint(f"ERROR: {e}")
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()

#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

if __name__ == '__main__':
    # get command-line arguments
    args = argsetup()
    conf_file = args.configf

    # read in configuration file
    config = ConfigParser()
    config.read(conf_file)

    # get API endpoints from config file
    nhl_teams = config['LINKS']['teams']
    nhl_players = config['LINKS']['players']

    # get database credentials from config file
    db_user = config['DATABASE']['USER']
    db_passwd = config['DATABASE']['PASSWORD']
    db_host = config['DATABASE']['CONNECTION']
    db_name = config['DATABASE']['DB_NAME']

    # open database connection using config file settings
    db_connect = database_connect()

    # initiate NHL team data getting
    _teams(nhl_teams)