'''

Description: Store junior hockey player data from the NHL website in a database.

This program pulls players' junior hockey data from the NHL website's publicly
available API. It uses a configuration file read in from the command line
to request the data from the API and establish a connection to the requisite
database.
'''

__version__ = '1.0'
__title__ = 'juniors_data_pull'
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

def open_logs(logs):
    '''
    Create a log file and setup parameters.
    '''
    
    # check if log directory from config file exists
    log_check = os.path.isdir(logs)

    # create the directory if it doesn't exist
    if not log_check:
        try:
            os.makedirs(logs)
        except:
            # couldn't create dir; default to {HOME}/logs
            home_dir = os.path.expanduser('~')
            logs = f"{home_dir}/logs"
            if not os.path.isdir(logs):
                os.makedirs(logs)

    # create log filename with timestamp
    now = datetime.now()
    date_format = now.strftime("%d%b%y_%H%M%S")
    log_file = f"{logs}/juniors_data_pull_{date_format}.log"

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
                    database = db_name,
                    port = db_port
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

    Note: Retry the connection twice if run into timeout error.
    '''

    log_file.info(f"Requesting data from {url}...")
    for _ in range(3):
        try:
            r = requests.get(url)
            if r.status_code == 200:
                # successful request, return data
                log_file.info(
                    f"Pulled data on {_ + 1} try from {url}..."
                )
                return r.json()
        except requests.exceptions.Timeout:
            # retry
            log_file.info(
              f"Connection to {url} timed out on try {_ + 1}...retrying"
            )
            continue
        except requests.exceptions.RequestException as e:
            log_file.error(e)
            sys.exit(1)

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

def sql_update(conn, cmd):
    '''
    Execute an SQL update command using an established database connection.

    conn -> preexisting database connection [(i.e. a connection setup using 
        nhl_data_pull.database_connect()]
    cmd  -> SQL update command to execute
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

def sql_select(conn, cmd, fetchall):
    '''
    Execute an SQL select command using an established database connection, and
    return one/all selected records depending on fetchall parameter.

    conn  -> preexisting database connection
    cmd   -> SQL select command to execute
    fetch -> Boolean that tells function whether to return all results or only
              one result
    '''

    cursor = conn.cursor()
    try:
        cursor.execute(cmd)
        if fetchall:
            result = cursor.fetchall()
        else:
            result = cursor.fetchone()
    except (Exception, psycopg2.DatabaseError) as e:
        log_file.error(f"ERROR: {e}")
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()
    return result

#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

if __name__ == '__main__':
    # get command-line arguments
    args = argsetup()
    conf_file = args.configf

    # read in configuration file
    config = ConfigParser()
    config.read(conf_file)

    # setup and test logging
    log_dir = config['DEFAULT']['LOGDIR']
    log_file = open_logs(log_dir)
    now = datetime.now().strftime("%d%b%Y %H:%M:%S")
    try:
        log_file.info(f"Starting Juniors Data Pull at {now}...")
    except:
        sys.exit(f"Logging failed to setup...exiting at time {now}...")

    # setup environment variables from config file
    log_file.info('Setting up environment variables from config file...')
    nhl_site = config['LINKS']['site']
    nhl_base = config['LINKS']['base']
    nhl_players = config['LINKS']['players']
    nhl_draft = config['LINKS']['draft']
    nhl_prospects = config['LINKS']['prospects']
    draft_year = config['DEFAULT']['DRAFT']

    # setup stats API endpoints from config file
    stats_byYear = config['STATS']['yearByYear']

    # get list of junior leagues to look at
    junior_leagues = config['JUNIORS']['LEAGUES']
    junior_leagues = junior_leagues.split()

    # get database credentials from config file
    log_file.info('Setting database credentials from config file...')
    db_user = config['DATABASE']['USER']
    db_passwd = config['DATABASE']['PASSWORD']
    db_host = config['DATABASE']['CONNECTION']
    db_name = config['DATABASE']['DB_NAME']
    db_port = config['DATABASE']['PORT']

    # open database connection using config file settings
    db_connect = database_connect()

    # pull data from {nhl_draft}/{draft_year}
    draft_data = request_data(f"{nhl_draft}/{draft_year}")
    
    # remove copyright info
    for key in draft_data.keys():
        if key == 'drafts':
            draft_data = draft_data[key][0]

    # cycle through each round of the draft
    draft_rounds = draft_data['rounds']
    for rnd in draft_rounds:
        # cycle through each pick of the round
        for pick in rnd['picks']:
            # select data points we need
            rnd = pick['round']
            rnd_pick = pick['pickInRound']
            overall_pick = pick['pickOverall']
            name = pick['prospect']['fullName']
            link = pick['prospect']['link']
            team_id = pick['team']['id']

            log_file.info(f"Checking Junior numbers for {name}...")

            # get prospect's individual info
            prospect_link = f"{nhl_site}/{link}"
            prospect_data = request_data(prospect_link)

            # pdb.set_trace()
            
            # remove copyright
            for key in prospect_data.keys():
                if key == 'prospects':
                    prospect_data = prospect_data[key][0]
            
            # get NHL Player ID to pull data for Junior seasons
            prospect_id = prospect_data['id']
            nhl_player_id = prospect_data['nhlPlayerId']
            first_name = prospect_data['firstName']
            last_name = prospect_data['lastName']
            dob = prospect_data['birthDate']
            height = prospect_data['height']
            weight = prospect_data['weight']
            shoots_catches = prospect_data['shootsCatches']
            position = prospect_data['primaryPosition']['name']

            # pull Junior season data for player
            junior_link = f"{nhl_players}/{nhl_player_id}/{stats_byYear}"
            season_data = request_data(junior_link)

            # remove copyright and parse down to just the season by season data
            for key in season_data.keys():
                if key == 'stats':
                    season_data = season_data[key][0]['splits']
            
            # cycle through player's seasons to parse out Junior hockey
            for season in season_data:
                # check if that season's stats are from a Junior league
                league = season['league']['name']
                if league in junior_leagues:
                    # get junior numbers for this season
                    player_id = nhl_player_id
                    team_name = season['team']['name']
                    year = season['season']
                    sequence = season['sequenceNumber']
                    games = season['stat']['games']
                    goals = season['stat']['goals']
                    assists = season['stat']['assists']
                    points = season['stat']['points']
                    pim = season['stat']['pim']

                    # print results
                    pprint(season)

            # exit after first pick for testing
            sys.exit()


    # cycle through each pick
        # get prospect id
        # pull data from {nhl_prospects}/id
        # get nhlPlayerId
        # pull data from {nhl_players}/id/{stats_byYear}
        # cycle through each season's data
            # get all Junior years for North American players leading up to {draft_year} NHL Entry Draft
            
    # use all North American players drafted for that year to reproduce the Projectinator and rank their NHL performance projection
        # North American Leagues to include in analysis:
            # OHL, WHL, and QMJHL
            # BCHL, AJHL, SJHL, MJHL, and OPJHL
            # USHL, NAHL, EJHL, and USPHL
            # CCHA, ECAC, and WCHA