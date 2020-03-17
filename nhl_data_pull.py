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

def _skaterStats_yearByYear():
    '''
    Pull year-by-year statistics for a player's NHL seasons.
    '''

    if stats_playersByYear == 'ALL':
        # get stats for all skaters in team_players table
        player_list = []
        # make sure we don't include goalies
        cmd = (
            f"SELECT DISTINCT player_id FROM team_players "
            f"EXCEPT "
            f"SELECT id FROM players WHERE position_code = 'G'"
        )
        cursor = db_connect.cursor()
        cursor.execute(cmd)
        for id in cursor.fetchall():
            player_list.append(id[0])
    else:
        # get stats for player IDs listed in config file
        player_list = stats_playersByYear.split()
    
    for player_id in player_list:
        # create a link to pull yearByYear stats for each player in list
        link = f"{nhl_players}/{player_id}/{stats_byYear}"
        
        # pull the data
        year_stats = request_data(link)

        # remove copyright statement
        for key in year_stats.keys():
            if key == 'stats':
                year_stats = year_stats[key][0]['splits']
    
        # keep only seasons with actual NHL data 
        # i.e. ignore Junior Hockey data for now
        nhl_years = []
        for year in year_stats:
            if year['league']['name'] == 'National Hockey League':
                nhl_years.append(year)
    
        log_file.info(nhl_years)

        # parse out specific data we need for the database for each NHL year
        for year in nhl_years:
            season = year['season']
            team_id = year['team']['id']
            toi = year['stat']['timeOnIce']
            games = year['stat']['games']
            assists = year['stat']['assists']
            goals = year['stat']['goals']
            pim = year['stat']['pim']
            shots = year['stat']['shots']
            hits = year['stat']['hits']
            pp_goals = year['stat']['powerPlayGoals']
            pp_points = year['stat']['powerPlayPoints']
            pp_toi = year['stat']['powerPlayTimeOnIce']
            even_toi = year['stat']['evenTimeOnIce']
            faceoff_pct = year['stat']['faceOffPct']
            shot_pct = year['stat']['shotPct']
            gw_goals = year['stat']['gameWinningGoals']
            ot_goals = year['stat']['overTimeGoals']
            sh_goals = year['stat']['shortHandedGoals']
            sh_points = year['stat']['shortHandedPoints']
            sh_toi = year['stat']['shortHandedTimeOnIce']
            blocked = year['stat']['blocked']
            plus_minus = year['stat']['plusMinus']
            points = year['stat']['points']
            shifts = year['stat']['shifts']
            sequence = year['sequence']
            # sequence essentially indicates whether it's a players first
            # stint with a team for that season
            # i.e. sequence = 1 for the player's first team that season;
            #    if the player is traded/sent down to the AHL, the season
            #    field remains the same, sequence is incremented, and player
            #    stats start from scratch for that new league or team.
            #    So if a player is traded mid-season, there are 2 sequences of
            #    stats for that season to account for...
            
            # sql command to insert skater data into skater_season_stats table
            season_stats_cmd = (
                f"INSERT INTO skater_season_stats (player_id, team_id, "
                f"season, time_on_ice, games, assists, goals, pim, shots, "
                f"hits, pp_goals, pp_points, pp_toi, even_toi, faceoff_pct, "
                f"shot_pct, gw_goals,ot_goals, sh_goals, sh_points, sh_toi, "
                f"blocked_shots, plus_minus, points, shifts, sequence) "
                f"VALUES ({player_id}, {team_id}, $${season}$$, $${toi}$$, "
                f"{games}, {assists}, {goals}, {pim}, {shots}, {hits}, "
                f"{pp_goals}, {pp_points}, $${pp_toi}$$, $${even_toi}$$, "
                f"{faceoff_pct}, {shot_pct}, {gw_goals}, {ot_goals}, "
                f"{sh_goals}, {sh_points}, $${sh_toi}$$, {blocked}, "
                f"{plus_minus}, {points}, {shifts}, {sequence})"
            )
            log_file.info(season_stats_cmd)


def _goalieStats_yearByYear():
    '''
    Pull goalie-specific year-by-year stats from NHL website.
    '''

    # put in same stuff from top of _skaterStats_yearByYear()

    # goalie-specific stats
    # wins = year['stat']['wins']
    # losses = year['stat']['losses']
    # ot_wins = year['stat']['ot']
    # shutouts = year['stat']['shutouts']
    # saves = year['stat']['saves']
    # pp_saves = year['stat']['powerPlaySaves']
    # sh_saves = year['stat']['shortHandedSaves']
    # even_saves = year['stat']['evenSaves']
    # pp_shots = year['stat']['powerPlayShots']
    # sh_shots = year['stat']['shortHandedShots']
    # even_shots = year['stat']['evenShots']
    # save_pct = year['stat']['savePercentage']
    # gaa = year['stat']['goalAgainstAverage']
    # starts = year['stat']['gamesStarted']
    # shots_against = year['stat']['shotsAgainst']
    # goals_against = year['stat']['goalsAgainst']
    # pp_save_pct = year['stat']['powerPlaySavePercentage']
    # sh_save_pct = year['stat']['shortHandedSavePercentage']
    # even_save_pct = year['stat']['evenStrengthSavePercentage']



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
    
    # setup stats API endpoints from config file
    stats_list = config['STATS']['LIST']
    stats_byYear = config['STATS']['yearByYear']
    stats_playersByYear = config['STATS']['playersByYear']

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

    if stats_list != 'NONE':
        log_file.info('Pulling year-by-year stats for NHL players...')
        _skaterStats_yearByYear()

    # close database connection
    db_connect.close()