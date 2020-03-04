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
import logging
import requests
import psycopg2
import argparse
import pandas as pd
import pdb
#import numpy as np
#import matplotlib.pyplot as plt

from configparser import ConfigParser
from datetime import datetime
from pprint import pprint

def open_logs():
    '''
    Create a log file and setup parameters.
    '''
    
    # create log filename with timestamp
    now = datetime.now()
    date_format = now.strftime("%d%b%y_%H%M%S")
    log_file = f"/home/phegedus/LOGS/nhl_data_pull_{date_format}.log"

    # create and configure logger
    logging.basicConfig(filename=log_file,
                        format='[%(asctime)s] %(message)s',
                        filemode='w')
    logger = logging.getLogger()

    # set the threshold of logger to DEBUG
    logger.setLevel(logging.DEBUG)

    return logger

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

    log_file.info('Establishing connecting to the database...')
    connection = None
    try:
        # establish database connection
        connection = psycopg2.connect(
                    user = db_user,
                    password = db_passwd,
                    host = db_host,
                    database = db_name
        )
        log_file.info('Database connection successfully established.')
    except psycopg2.DatabaseError as e:
        # Report error
        log_file.error(e)
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

    log_file.info(f"Requesting data from {url}...")
    r = requests.get(url)
    if r.status_code == 200:
        # successful request, return data
        log_file.info('Successfully pulled data from provided URL...')
        return r.json()
    else:
        # print exception message since request failed
        r.raise_for_status()
        log_file.info('Exiting after failing to pull data from url')
        sys.exit(1)
        return None

def sql_insert(conn, cmd):
    '''
    Execute an SQL insert command using an established database connection.

    conn -> preexisting database connection [(i.e. a connection setup using 
        nhl_data_pull.database_connect()]
    cmd  -> SQL insert command to execute
    '''

    cursor = conn.cursor()
    try:
        cursor.execute(cmd)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        log_file.error(f"ERROR: {e}")
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()
    return 0

def _teams(url):
    '''
    Overall function to get complete dataset on all NHL teams, then parse down
    data to only what is required and store in our database.
        
    Required data:
        - team_id
        - name
        - abbreviation
        - conference_id
        - division_id
        - franchise_id
        - active
    '''

    # get raw team data from NHL site
    team_dataset = request_data(url)

    # pull list of team data from returned JSON object/ignore copyright info
    for key in team_dataset.keys():
        if key == 'teams':
            team_list = team_dataset[key]

    # can now cycle thru each individual team
    for team_data in team_list:
        #pdb.set_trace()
        team_id = team_data['id']
        team_name = team_data['name']
        abbreviation = team_data['abbreviation']
        conference_id = team_data['conference']['id']
        division_id = team_data['division']['id']
        franchise_id = team_data['franchise']['franchiseId']
        active = team_data['active']

        log_file.info(f"> Loading NHL Team data for {team_name} ({team_id})...")

        insert_cmd = (
            f"INSERT INTO teams (id, name, abbreviation, conf_id, division_id," 
            f" franchise_id, active) VALUES ({team_id}, $${team_name}$$, "
            f"$${abbreviation}$$, {conference_id}, {division_id}, "
            f"{franchise_id}, {active})"
        )
        # load parsed team data into database using established connection
        status = sql_insert(db_connect, insert_cmd)
        if status == 0: 
            log_file.info(f">> Successfully uploaded data for {team_name} "
                f"({team_id})...")

def _players(url, team_ids):
    '''
    Overall function to get complete dataset of NHL players, then parse down 
    data to what is required and store in our database.

    Data we need from an individual NHL player's API endpoint:
        - id                    
        - full_name             - rookie
        - link                  - shoots_catches
        - current_age           - position_code
        - nationality           - position_name
        - active                - position_type
    '''

    # get roster from each team in database
    if team_ids == 'ALL':
        team_list = []
        cmd = 'SELECT id, name FROM teams'
        cursor = db_connect.cursor()
        cursor.execute(cmd)
        # create list of team ids
        team_list = cursor.fetchall()
    else:
        team_list = team_ids.split()
    
    # pdb.set_trace()
    # get roster of players from each team
    for team_id, team_name in team_list:
        log_file.info(f"> Pulling NHL player data from {team_name} "
            f"({team_id})...")
        # create url to connect to api
        team_roster = f"{nhl_teams}/{team_id}/roster"
        # connect to api
        player_dataset = request_data(team_roster)
        # pull list of players from returned JSON object containing roster
        for key in player_dataset.keys():
            if key == 'roster':
                player_dataset = player_dataset[key]
        
        player_list = parse_roster(player_dataset)
        for endpoint in player_list:
            # generate player's link to NHL API
            url = f"{nhl_site}{endpoint}"

            # get player's data
            dataset = request_data(url)
            for key in dataset.keys():
                if key == 'people':
                    dataset = dataset[key][0]

            # parse out specific data we need for the database
            player_id = dataset['id']
            name = dataset['fullName']
            link = dataset['link']
            age = dataset['currentAge']
            nationality = dataset['nationality']
            active = dataset['active']
            rookie = dataset['rookie']
            shoots_catches = dataset['shootsCatches']
            position_code = dataset['primaryPosition']['abbreviation']
            position_name = dataset['primaryPosition']['name']
            position_type = dataset['primaryPosition']['type']
            season = '20192020'

            # pdb.set_trace()
            players_cmd = (
                f"INSERT INTO players (id, full_name, link, current_age, "
                f"nationality, active, rookie, shoots_catches, "
                f"position_code, position_name, position_type) VALUES "
                f"({player_id}, $${name}$$, $${link}$$, {age}, "
                f"$${nationality}$$, {active}, {rookie}, $${shoots_catches}$$,"
                f" $${position_code}$$, $${position_name}$$, "
                f"$${position_type}$$)"
            )
            team_players_cmd = (
                f"INSERT INTO team_players (team_id, player_id, season, "
                f"active) VALUES ({team_id}, {player_id}, $${season}$$, "
                f"{active})"
            )

            # load parsed player data into database
            player_status = sql_insert(db_connect, players_cmd)
            team_players_status = sql_insert(db_connect, team_players_cmd)

            # log results
            if player_status == 0:
                log_file.info(f">> Successfully uploaded data for {name} "
                    f"({player_id}) to players table...")
            if team_players_status == 0:
                log_file.info(f">>> Uploaded data for {name} ({player_id}) "
                    f"to team_players table...")

        log_file.info(f">> Completed player data pull for {team_name} "
            f"({team_id})...")

def parse_roster(roster):
    '''
    Sort through an individual NHL team's roster to get the link to the API
    endpoint where we will pull that player's specific data.

    Inputted data is a list dictionaries where each dict contains high level
    data about one player - this is where we will find the API endpoint.
    '''

    players_list = []
    for _ in roster:
        # compile a list of each player's individual API endpoint
        players_list.append(_['person']['link'])
    
    return players_list

#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

if __name__ == '__main__':
    # get command-line arguments
    args = argsetup()
    conf_file = args.configf

    # setup and test logging
    log_file = open_logs()
    now = datetime.now().strftime("%d%b%Y %H:%M:%S")
    try:
        log_file.info(f"Starting NHL Data Pull at {now}...")
    except:
        sys.exit(f"Logging failed to setup...exiting at time {now}...")

    # read in configuration file
    config = ConfigParser()
    config.read(conf_file)

    # setup environment variables from config file
    log_file.info('Setting up environment variables from config file...')
    nhl_site = config['LINKS']['site']
    nhl_base = config['LINKS']['base']
    nhl_teams = config['LINKS']['teams']
    nhl_players = config['LINKS']['players']
    nhl_teams_list = config['TEAMS']['LIST']
    nhl_players_teamIds = config['PLAYERS']['TEAM_ID']
    nhl_players_list = config['PLAYERS']['LIST']
    

    # get database credentials from config file
    log_file.info('Setting database credentials from config file...')
    db_user = config['DATABASE']['USER']
    db_passwd = config['DATABASE']['PASSWORD']
    db_host = config['DATABASE']['CONNECTION']
    db_name = config['DATABASE']['DB_NAME']

    # open database connection using config file settings
    db_connect = database_connect()

    # initiate NHL team data getting if told by config file
    if nhl_teams_list != 'NONE':
        log_file.info('Pulling NHL Team data from website and storing in '
            'database...')
        _teams(nhl_teams)

    # initiate NHL player data getting
    if nhl_players_list != 'NONE':
        log_file.info('Pulling NHL Player data and storing in database...')
        _players(nhl_players, nhl_players_teamIds)

    # close database connection
    db_connect.close()