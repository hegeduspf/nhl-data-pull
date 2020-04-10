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
from googlesearch import search
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
    for i in range(3):
        try:
            r = requests.get(url)
            if r.status_code == 200:
                # successful request, return data
                log_file.info(
                    f"Pulled data on {i + 1} try from {url}..."
                )
                return r.json()
            else:
                # bad request
                if i == 2:
                    # last attempt; return bad data if any
                    log_file.info(
                        f"Failed to pull data {i + 1} times from {url}..."
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

def get_player_id(name):
    '''
    Provided an NHL Player's name, return their NHL Player ID if they have one.

    Input: NHL Player's name (first and last)
    Output: NHL Player ID if found; NULL if not found
    '''

    log_file.info(f">>> Searching Google for {name}'s NHL Player ID...")
    # search google for player name
    query = name + ' NHL'
    player_search = search(
        query,
        tld = 'com',
        num = 10,
        start = 0,
        stop = 10,
        pause = 2
    )
    nhl_link = None
    for link in player_search:
        # check if it's the NHL website, which is where we get the Player ID
        if link.find('www.nhl.com/player') >= 0:
            # found link that has player ID
            nhl_link = link

    log_file.info(f">>> Found link to {name}'s NHL Player Profile: {nhl_link}")

    # only return id if found nhl site profile for player
    if nhl_link:
        # parse id out of the http link
        nhl_link = nhl_link.split('/')
        player_id = nhl_link[-1].split('-')
        player_id = player_id[-1]
        log_file.info(f">>> NHL Player ID found for {name}: {player_id}...")
    else:
        log_file.info(f">>> Couldn't find NHL Player ID for {name}...")
        player_id = 'NULL'

    return player_id

def _nhl_player_check(player):
    '''
    Given an NHL Player's ID, check if they have a corresponding record in the
    players table of the database.
    '''

    # check the players table in the database for record 
    cmd = (
        f"SELECT * FROM nhl_players WHERE id = {player}"
    )
    check = sql_select(db_connect, cmd, False)
    if check:
        # record exists for player in database
        log_file.info(f">> Found matching NHL Player Profile for {player}...")
        return 0
    else:
        # no record exists
        log_file.info(f">> No NHL Player Profile found for {player}...")
        return 1

def _nhl_player_create(player):
    '''
    Create a player profile to add to the NHL players table in the database.
    '''

    # create link to NHL player profile
    link = f"{nhl_players}/{player}"

    # pull data for NHL player
    data = request_data(link)

    # remove copyright statement
    for key in data.keys():
        if key == 'people':
            data = data[key][0]
    
    # setup data points
    first_name = data.get('firstName', 'NULL')
    last_name = data.get('lastName', 'NULL')   
    link = data.get('link', 'NULL')
    dob = data.get('birthDate', 'NULL')
    nationality = data.get('nationality', 'NULL')
    active = data.get('active', 'NULL')
    rookie = data.get('rookie', 'NULL')
    shoots_catches = data.get('shootsCatches', 'NULL')
    try:
        position_code = data.get('primaryPosition').get('abbreviation', 'NULL')
        position_name = data.get('primaryPosition').get('name', 'NULL')
        position_type = data.get('primaryPosition').get('type', 'NULL')
    except:
        position_code = 'NULL'
        position_name = 'NULL'
        position_type = 'NULL'

    # insert new record to players table
    players_insert_cmd = (
        f"INSERT INTO nhl_players (id, first_name, last_name, link, dob, "
        f"nationality, active, rookie, shoots_catches, position_code, "
        f"position_name, position_type) VALUES ({player}, $${first_name}$$, "
        f"$${last_name}$$, $${link}$$, $${dob}$$, $${nationality}$$, "
        f"{active}, {rookie}, $${shoots_catches}$$, "
        f"$${position_code}$$ ,$${position_name}$$, "
        f"$${position_type}$$)"
    )
    # insert the new player data into the database
    status = sql_insert(db_connect, players_insert_cmd)

    # log success
    if status == 0:
        log_file.info(f">> Created NHL Player profile for {player}...")
        return 0
    else:
        return 1

def _sequence_check(id, season, seq):
    '''
    Check whether a player, season, sequence instance exists in the database
    already. If so, increment sequence and return new value. Otherwise, return
    sequence as is.

    In order to correctly parse out junior stats data and upload it to the
    database, we need to distinguish between player instances with the same
    sequence number.

    Ex: Matt Auffrey's 2003-2004 season playing in juinors looks like:
            [season]      [league]      [sequence]
            20032004        U-18            1
            20032004       WJ18-A           2
            20032004        NAHL            2
    '''

    # log function start
    log_file.info(f">>> Checking whether player {id}'s {season} season with "
        f"sequence {seq} already exists in the database...")
    
    # check junior_skater_stats for record
    skater_check_cmd = (
        f"SELECT EXISTS("
        f"SELECT 1 FROM junior_skater_stats WHERE player_id = {id} AND "
        f"season = $${season}$$ AND sequence = {seq})"
    )
    skater_check = sql_select(db_connect, skater_check_cmd, False)
    skater_check = skater_check[0]
    
    # check junior_goalie_stats for record
    goalie_check_cmd = (
        f"SELECT EXISTS("
        f"SELECT 1 FROM junior_goalie_stats WHERE player_id = {id} AND "
        f"season = $${season}$$ AND sequence = {seq})"
    )
    goalie_check = sql_select(db_connect, goalie_check_cmd, False)
    goalie_check = goalie_check[0]

    if skater_check or goalie_check:
        # record already exists that player, season, and sequence
        log_file.info(f">>> Found existing record for player {id}'s {season} "
            f"season with sequence {seq}. New sequence number is {seq+1}..")
        # increment sequence number
        seq = seq + 1
    else:
        # record doesn't exist for that player, season, sequence
        log_file.info(f">>> No record found for player {id}'s {season} season "
            f"with sequnce {seq}...proceeding as normal...")
        
    return seq

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
    log_file.info(f"Getting junior hockey data for prospects selected in "
        f"{draft_year} NHL Entry Draft..."
    )
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
            rnd = pick.get('round', 'NULL')
            rnd_pick = pick.get('pickInRound', 'NULL')
            overall_pick = pick.get('pickOverall', 'NULL')
            name = pick.get('prospect').get('fullName', 'NULL')
            link = pick.get('prospect').get('link', 'NULL')
            team_id = pick.get('team').get('id', 'NULL')
            team_name = pick.get('team').get('name', 'NULL')
            prospect_id = pick.get('prospect').get('id', 'NULL')
            
            # reset nhl_player_id
            nhl_player_id = 'NULL'

            # get NHL Player ID to pull drafted player's info
            log_file.info(f"> Getting NHL Player ID for {name}")
            if prospect_id != 'NULL':
                # check if prospect data has NHL Player ID
                prospect_link = f"{nhl_site}/{link}"
                prospect_data = request_data(prospect_link)
                # remove copyright statement
                for key in prospect_data.keys():
                    if key == 'prospects':
                        prospect_data = prospect_data[key][0]
                nhl_player_id = prospect_data.get('nhlPlayerId', 'NULL')
            else:
                # search Google for player's ID from their NHL profile
                nhl_player_id = get_player_id(name)
                print(f"{name}'s NHL Player ID: {nhl_player_id}")
                log_file.info(f">> Prospect ID was null for {name}...found "
                    f"NHL Player ID on Google: {nhl_player_id}...")
            
            # track whether we need to skip a prospect because we can't find data
            skip_prospect = False

            # check whether we found NHL Player ID
            if nhl_player_id == 'NULL':      
                # couldn't find it, use previous draft pick to get current one's ID
                log_file.info(f">> No prospect profile for {name}...generating "
                    f"NHL Player ID using previous draft pick...")
                # break variable for nested loops
                breaking = False
                for i in range(1, 4):
                    if overall_pick == 1:
                        # first pick in draft; skip
                        log_file.warning(f">> First pick in draft and no "
                            f"prospect profile found...skipping...")
                        skip_prospect = True
                        break
                    select_previous_cmd = (
                        f"SELECT nhl_player_id FROM nhl_draft "
                        f"WHERE draft_year = $${draft_year}$$ AND "
                        f"overall_pick = {overall_pick - i}"
                    )
                    previous_pick = sql_select(db_connect, select_previous_cmd, False)

                    for j in range(1, i + 1):
                        # essentially keep adding one to previous player id to find current one
                        try:
                            nhl_player_id = previous_pick[0] + j
                        except:
                            # couldn't find previous pick; move to next attempt
                            log_file.info(
                                f">> Failed to find previous pick on attempt "
                                f"{j}...continuing with search...")
                            skip_prospect = True
                            continue

                        # check that name from NHL Player Profile matches draft pick we're looking at
                        player_link = f"{nhl_players}/{nhl_player_id}"
                        player_data = request_data(player_link)

                        # remove copyright
                        for key in player_data.keys():
                            if key == 'people':
                                player_data = player_data[key][0]
                        
                        # compare to name variable from draft data
                        full_name = player_data.get('fullName', 'NULL NULL')
                        full_name = full_name.split()
                        temp_name = name.split()
                        # some draft pick's names aren't capitalized to match their NHL profile
                        # and some draft pick's first names are their written in their native language
                        if full_name[1].upper() == temp_name[1].upper():
                            # last name's match, check first names
                            if full_name[0].upper() == temp_name[0].upper():
                                # found correct nhl_player_id; break from both loops
                                skip_prospect = False
                                breaking = True
                                break
                            elif full_name[0][0] == temp_name[0][0]:
                                # warn that first names don't match, but first letters do just
                                # in case it's the wrong player
                                log_file.warning(
                                    f"WARNING: {name}'s first name doesn't "
                                    f"match NHL Profile for {nhl_player_id} "
                                    f"but first letter and last name's do..."
                                )
                                skip_prospect = False
                                breaking = True
                                break
                            else:
                                # same last name, but not same person
                                skip_prospect = True
                        else:
                            # didn't find correct id; reset nhl_player_id
                            skip_prospect = True

                    # check whether to break from outer loop
                    if breaking:
                        break
            
            if skip_prospect:
                # couldn't find nhl_player_id that matches draft pick; log error and skip to next pick
                log_file.warning(f"WARNING: COULDN'T FIND A CORRESPONDING "
                    f"PLAYER ID FOR {name}...MOVING TO NEXT PICK")
                continue
            
            # get NHL Player profile data
            player_link = f"{nhl_players}/{nhl_player_id}"
            player_data = request_data(player_link)
            # remove copyright
            for key in player_data.keys():
                if key == 'people':
                    player_data = player_data[key][0]

            # set data points using player data
            first_name = player_data.get('firstName', 'NULL')
            last_name = player_data.get('lastName', 'NULL')
            dob = player_data.get('birthDate', 'NULL')
            country = player_data.get('birthCountry', 'NULL')
            shoots = player_data.get('shootsCatches', 'NULL')
            try:
                position = player_data.get('primaryPosition').get('name', 'NULL')
            except:
                position = 'NULL'

            # check if there's a corresponding NHL player profile in our database
            check = _nhl_player_check(nhl_player_id)
            if check == 1:
                # no record found, create one. Must be done b/c of foreign key references
                _nhl_player_create(nhl_player_id)

            # insert draft data into nhl_draft table of database
            draft_cmd = (
                f"INSERT INTO nhl_draft (nhl_player_id, draft_year, "
                f"overall_pick, round_number, round_pick, team_id, "
                f"prospect_id, first_name, last_name, dob, country, shoots, "
                f"position) VALUES ({nhl_player_id}, $${draft_year}$$, "
                f"{overall_pick}, {rnd}, {rnd_pick}, {team_id}, {prospect_id}, "
                f"$${first_name}$$, $${last_name}$$, $${dob}$$, $${country}$$, "
                f"$${shoots}$$, $${position}$$)"
            )
            draft_status = sql_insert(db_connect, draft_cmd)
            if draft_status == 0:
                log_file.info(f"> Draft data stored for {draft_year} Round "
                    f"{rnd} Pick {rnd_pick} - {first_name} {last_name}...")
        
            # pdb.set_trace()
            
            log_file.info(f">> Pulling Junior hockey seasons for {name}...")

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
                league = season.get('league').get('name', 'NULL')
                if league in junior_leagues and position != 'Goalie':
                    # get junior skater numbers for this season
                    year = season.get('season', 'NULL')
                    sequence = season.get('sequenceNumber', 'NULL')
                    games = season.get('stat').get('games', 'NULL')
                    goals = season.get('stat').get('goals', 'NULL')
                    assists = season.get('stat').get('assists', 'NULL')
                    points = season.get('stat').get('points', 'NULL')
                    pp_goals = season.get('stat').get('powerPlayGoals', 'NULL')
                    gw_goals = season.get('stat').get('gameWinningGoals', 'NULL')
                    sh_goals = season.get('stat').get('shortHandedGoals', 'NULL')
                    faceoff_pct = season.get('stat').get('faceOffPct', 'NULL')
                    time_on_ice = season.get('stat').get('timeOnIce', 'NULL')
                    pp_toi = season.get('stat').get('powerPlayTimeOnIce', 'NULL')
                    sh_toi = season.get('stat').get('shortHandedTimeOnIce', 'NULL')
                    even_toi = season.get('stat').get('evenTimeOnIce', 'NULL')
                    plus_minus = season.get('stat').get('plusMinus', 'NULL')
                    pim = season.get('stat').get('pim', 'NULL')

                    # make sure sequence number isn't already being used this season
                    sequence = _sequence_check(nhl_player_id, year, sequence)

                    # generate SQL insert command for junior skater stats
                    stats_cmd = (
                        f"INSERT INTO junior_skater_stats (player_id, season, "
                        f"league, games, goals, assists, points, pp_goals, "
                        f"gw_goals, sh_goals, faceoff_pct, time_on_ice, "
                        f"pp_toi, sh_toi, even_toi, plus_minus, pim, sequence) "
                        f"VALUES ({nhl_player_id}, $${year}$$, $${league}$$, "
                        f"{games}, {goals}, {assists}, {points}, {pp_goals}, "
                        f"{gw_goals}, {sh_goals}, {faceoff_pct}, "
                        f"$${time_on_ice}$$, $${pp_toi}$$, $${sh_toi}$$, "
                        f"$${even_toi}$$, {plus_minus}, {pim}, {sequence})"
                    )
                elif league in junior_leagues and position == 'Goalie':
                    # get Junior goalie stats for the season
                    year = season.get('season', 'NULL')
                    sequence = season.get('sequenceNumber', 'NULL')
                    games = season.get('stat').get('games', 'NULL')
                    wins = season.get('stat').get('wins', 'NULL')
                    losses = season.get('stat').get('losses', 'NULL')
                    ties = season.get('stat').get('ties', 'NULL')
                    ot_wins = season.get('stat').get('ot', 'NULL')
                    shutouts = season.get('stat').get('shutouts', 'NULL')
                    goals_against = season.get('stat').get('goalsAgainst', 'NULL')
                    gaa = season.get('stat').get('goalAgainstAverage', 'NULL')
                    shots_against = season.get('stat').get('shotsAgainst', 'NULL')
                    saves = season.get('stat').get('saves', 'NULL')
                    save_pct = season.get('stat').get('savePercentage', 'NULL')

                    # make sure sequence number isn't already being used this season
                    sequence = _sequence_check(nhl_player_id, year, sequence)

                    # generate SQL insert command for goalie junior stats
                    stats_cmd = (
                        f"INSERT INTO junior_goalie_stats (player_id, season, "
                        f"league, games, wins, losses, ties, ot_wins, shutouts, "
                        f"goals_against, gaa, shots_against, saves, save_pct, "
                        f"sequence) VALUES ({nhl_player_id}, $${year}$$, "
                        f"$${league}$$, {games}, {wins}, {losses}, {ties}, "
                        f"{ot_wins}, {shutouts}, {goals_against}, {gaa}, "
                        f"{shots_against}, {saves}, {save_pct}, {sequence})"
                    )
                else:
                    # league either isn't a Junior league or isn't one we're looking at
                    log_file.info(f">> Skipping {name}'s {season['season']} "
                        f"season in the {league}...")
                    # move on to next listed season
                    continue

                # insert Junior hockey data into junior_stats table
                stats_status = sql_insert(db_connect, stats_cmd)
                if stats_status == 0:
                    log_file.info(f">> Added Junior season stats for {name}'s "
                        f"{year} season in the {league}...")

            # all Junior seasons should have been found by now
            log_file.info(f">> Finished pulling Junior season stats for {name}...")

    # close database connection
    db_connect.close()

    # use all North American players drafted for that year to reproduce the Projectinator and rank their NHL performance projection
        # North American Leagues to include in analysis:
            # OHL, WHL, and QMJHL
            # BCHL, AJHL, SJHL, MJHL, and OPJHL
            # USHL, NAHL, EJHL, and USPHL
            # CCHA, ECAC, and WCHA