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
    log_file = f"{logs}/nhl_data_pull_{date_format}.log"

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

        # determine if a record exists for that team or not
        select_cmd = (
            f"SELECT * FROM teams WHERE id = {team_id}"
        )
        record_check = sql_select(db_connect, select_cmd, False)
        
        # insert/update table record accordingly
        if record_check:
            # record exists for that team, just update the data
            log_file.info(f"> Existing record found, updating NHL Team data "
                f"for {team_name} ({team_id})...")

            update_cmd = (
                f"UPDATE teams SET id = {team_id}, name = $${team_name}$$, "
                f"abbreviation = $${abbreviation}$$, conf_id = "
                f"{conference_id}, division_id = {division_id} "
                f"WHERE id = {team_id}"
            )
            # update team data in the database
            status = sql_update(db_connect, update_cmd)
        else:
            # did not find a record for that team, insert new one
            log_file.info(f"> No record found, inserting NHL Team data for "
                f"{team_name} ({team_id})...")

            insert_cmd = (
                f"INSERT INTO teams (id, name, abbreviation, conf_id, division_id," 
                f" franchise_id, active) VALUES ({team_id}, $${team_name}$$, "
                f"$${abbreviation}$$, {conference_id}, {division_id}, "
                f"{franchise_id}, {active})"
            )
            # insert the new team data into database
            status = sql_insert(db_connect, insert_cmd)
        
        # log successful upload; sql_insert/update already log database errors
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
        - dob                   - position_code
        - nationality           - position_name
        - active                - position_type
    '''

    # get roster from each team in database
    team_list = []
    if team_ids == 'ALL':
        cmd = 'SELECT id, name FROM teams'
    else:
        cmd = (
            f"SELECT id, name FROM teams "
            f"WHERE id IN({team_ids})"
        )
    # create list of team ids with database list
    team_list = sql_select(db_connect, cmd, True)

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
            first_name = dataset['fullName'].split()[0]
            last_name = dataset['fullName'].split()[1]
            link = dataset['link']
            dob = dataset['birthDate']
            nationality = dataset['nationality']
            active = dataset['active']
            rookie = dataset['rookie']
            shoots_catches = dataset['shootsCatches']
            position_code = dataset['primaryPosition']['abbreviation']
            position_name = dataset['primaryPosition']['name']
            position_type = dataset['primaryPosition']['type']
            season = current_season
            
            sequence = _get_player_sequence(endpoint, team_name)
            if sequence is None:
                # no NHL data found for this season
                continue

            # pdb.set_trace()

            # determine if a record exists for that player
            select_players_cmd = (
                f"SELECT * FROM players WHERE id = {player_id}"
            )
            players_check = sql_select(
                db_connect, select_players_cmd, False)

            # insert/update players table accordingly
            if players_check:
                # record exists for that player, just update the data
                log_file.info(f"> Existing record found, updating NHL Player "
                    f"data for {last_name} ({player_id})...")
                players_update_cmd = (
                    f"UPDATE players SET id = {player_id}, first_name = "
                    f"$${first_name}$$, last_name = $${last_name}$$, "
                    f"link = $${link}$$, dob = $${dob}$$, "
                    f"nationality = $${nationality}$$, active = {active}, "
                    f"rookie = {rookie}, shoots_catches = "
                    f"$${shoots_catches}$$, position_code = "
                    f"$${position_code}$$, position_name = "
                    f"$${position_name}$$, position_type = "
                    f"$${position_type}$$ WHERE id = {player_id}"
                )
                # update player data in the database
                players_status = sql_update(db_connect, players_update_cmd)
            else:
                # did not find a record for that player, insert new one
                log_file.info(f"> No record found, inserting NHL Player data "
                    f"for {last_name} ({player_id})...")
                players_insert_cmd = (
                    f"INSERT INTO players (id, first_name, last_name, link, "
                    f"dob, nationality, active, rookie, shoots_catches, "
                    f"position_code, position_name, position_type) VALUES "
                    f"({player_id}, $${first_name}$$, $${last_name}$$, "
                    f"$${link}$$, $${dob}$$, $${nationality}$$, "
                    f"{active}, {rookie}, $${shoots_catches}$$, "
                    f"$${position_code}$$ ,$${position_name}$$, "
                    f"$${position_type}$$)"
                )
                # insert the new player data into the database
                players_status = sql_insert(db_connect, players_insert_cmd)

            # determine if a corresponding record exists in the team_player
            # bridge table
            select_team_players_cmd = (
                f"SELECT * FROM team_players WHERE player_id = {player_id} AND "
                f"team_id = {team_id} AND season = $${season}$$ AND "
                f"sequence = {sequence}"
            )
            team_players_check = sql_select(
                db_connect, select_team_players_cmd, False
            )

            # insert/update team_players record accordingly
            if team_players_check:
                # record exists, just update the data
                log_file.info(
                    f"> Existing record found, updating team_players data for "
                    f"{last_name} ({player_id})'s {season} season with the "
                    f"{team_name} ({team_id})..."
                )
                team_players_update_cmd = (
                    f"UPDATE team_players SET player_id = {player_id}, "
                    f"team_id = {team_id}, season = $${season}$$, active = "
                    f"{active}, sequence = {sequence} WHERE player_id = "
                    f"{player_id} AND team_id = {team_id} AND season = "
                    f"$${season}$$ AND sequence = {sequence}"
                )
                team_players_status = sql_update(
                    db_connect, team_players_update_cmd
                )
            else:
                # did not find a corresponding record in team_players table
                log_file.info(
                    f"> No record found, inserting data into team_players for"
                    f"{last_name} ({player_id})'s {season} season with the "
                    f"{team_name} ({team_id})..."
                )
                team_players_insert_cmd = (
                    f"INSERT INTO team_players (player_id, team_id, season, "
                    f"active, sequence) VALUES ({player_id}, {team_id}, "
                    f"$${season}$$, {active}, {sequence})"
                )
                # insert the new team_players data into the database
                team_players_status = sql_insert(
                    db_connect, team_players_insert_cmd
                )

            # log successful upload; already logging database errors
            if players_status == 0:
                log_file.info(f">> Successfully uploaded data for {last_name} "
                    f"({player_id}) to players table...")
            if team_players_status == 0:
                log_file.info(f">>> Uploaded data for {last_name} ({player_id}) "
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
    Pull year-by-year statistics for a skater's NHL seasons. A skater is
    defined to by any NHL player that is not a goalie (i.e. any Forward
    or Defenseman).
    '''

    if stats_skatersByYear == 'ALL':
        # get stats for all skaters in team_players table
        player_list = []
        # make sure we don't include goalies
        cmd = (
            f"SELECT DISTINCT player_id FROM team_players "
            f"INNER JOIN players ON team_players.player_id = players.id "
            f"WHERE players.position_code != 'G'"
        )
        select_results = sql_select(db_connect, cmd, True)
        for id in select_results:
            player_list.append(id[0])
    else:
        # get stats for player IDs listed in config file
        player_list = stats_skatersByYear.split()
    
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
    
        log_file.info(
            f"{len(nhl_years)} NHL seasons found for player {player_id}"
        )

        # parse out specific data we need for the database for each NHL year
        for i, year in enumerate(nhl_years):
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
            sequence = year['sequenceNumber']
        
            # correctly set active to handle edge case of players that are
            # traded/reassigned mid-season
            if i < len(nhl_years) - 1:
                # past NHL season; active should be false
                active = False
            else:
                # current or last played NHL season for player
                if season == current_season:
                    active = True
                else:
                    # no NHL data for current season - in AHL/other league
                    active = False

            # ensure this season & sequence's data are in team_players
            _team_players_check(player_id, team_id, season, active, sequence)

            # determine if a record already exists for this skater/season
            select_cmd = (
                f"SELECT * FROM skater_season_stats WHERE player_id = "
                f"{player_id} AND team_id = {team_id} AND season = "
                f"$${season}$$ AND sequence = {sequence}"
            )
            skater_season_check = sql_select(db_connect, select_cmd, False)

            # insert/update skater_season_stats record accordingly
            if skater_season_check:
                # record exists for that skater and season, just update data
                log_file.info(
                    f"> Existing record found, updating Skater data for "
                    f"{player_id}'s {season} NHL season..."
                )
                update_cmd = (
                    f"UPDATE skater_season_stats SET player_id = {player_id}, "
                    f"team_id = {team_id}, season = $${season}$$, "
                    f"time_on_ice = $${toi}$$, games = {games}, assists = "
                    f"{assists}, goals = {goals}, pim = {pim}, shots = "
                    f"{shots}, hits = {hits}, pp_goals = {pp_goals}, "
                    f"pp_points = {pp_points}, pp_toi = $${pp_toi}$$, "
                    f"even_toi = $${even_toi}$$, faceoff_pct = {faceoff_pct}, "
                    f"shot_pct = {shot_pct}, gw_goals = {gw_goals}, "
                    f"ot_goals = {ot_goals}, sh_goals = {sh_goals}, "
                    f"sh_points = {sh_points}, sh_toi = $${sh_toi}$$, "
                    f"blocked_shots = {blocked}, plus_minus = {plus_minus}, "
                    f"points = {points}, shifts = {shifts}, sequence = "
                    f"{sequence} WHERE player_id = {player_id} AND team_id = "
                    f"{team_id} AND season = $${season}$$ AND sequence = "
                    f"{sequence}"
                )
                # update skater data in the database
                status = sql_update(db_connect, update_cmd)
            else:
                # did not find a record for skater and season, insert new one
                log_file.info(f"> No record found, updating Skater data for "
                    f"{player_id}'s {season} NHL season..."
                )
                insert_cmd = (
                    f"INSERT INTO skater_season_stats (player_id, team_id, "
                    f"season, time_on_ice, games, assists, goals, pim, "
                    f"shots, hits, pp_goals, pp_points, pp_toi, even_toi, "
                    f"faceoff_pct, shot_pct, gw_goals, ot_goals, sh_goals, "
                    f"sh_points, sh_toi, blocked_shots, plus_minus, points, "
                    f"shifts, sequence) VALUES ({player_id}, {team_id}, "
                    f"$${season}$$, $${toi}$$, {games}, {assists}, {goals}, "
                    f"{pim}, {shots}, {hits}, {pp_goals}, {pp_points}, "
                    f"$${pp_toi}$$, $${even_toi}$$, {faceoff_pct}, "
                    f"{shot_pct}, {gw_goals}, {ot_goals}, {sh_goals}, "
                    f"{sh_points}, $${sh_toi}$$, {blocked}, {plus_minus}, "
                    f"{points}, {shifts}, {sequence})"
                )
                # insert skater season stats into the database
                status = sql_insert(db_connect, insert_cmd)

            # log successful upload; already logging database errors
            if status == 0:
                if season == current_season:
                    log_file.info(f">> Finished loading all data for skater "
                        f"{player_id} with the {season} season...")
                else:
                    log_file.info(f">> Successfully inserted data for skater "
                        f"{player_id} ({season} season) to the " f"skater_season_stats table...")
    
    log_file.info(f">> Completed pulling yearByYear skater stats using list "
        f"from configuration file...")
        
def _goalieStats_yearByYear():
    '''
    Pull year-by-year statistics for a Goalie's NHL seasons.
    '''

    if stats_goaliesByYear == 'ALL':
        # get stats for all goalies in team_players table
        player_list = []
        # only include goalies
        cmd = (
            f"SELECT DISTINCT player_id FROM team_players "
            f"INNER JOIN players ON team_players.player_id = players.id "
            f"WHERE players.position_code = 'G'"
        )
        select_results = sql_select(db_connect, cmd, True)
        for id in select_results:
            player_list.append(id[0])
    else:
        # get stats for player IDs listed in config file
        player_list = stats_goaliesByYear.split()

    for player_id in player_list:
        # create link to player's yearByYear stats page
        link = f"{nhl_players}/{player_id}/{stats_byYear}"

        # pull the player's yearByYear data
        year_stats = request_data(link)

        # remove copyright statement
        for key in year_stats.keys():
            if key == 'stats':
                year_stats = year_stats[key][0]['splits']

        # keep only seasons with actual NHL data
        nhl_years = []
        for year in year_stats:
            if year['league']['name'] == 'National Hockey League':
                nhl_years.append(year)

        log_file.info(
            f"{len(nhl_years)} NHL seasons found for player {player_id}"
        )

        # parse out specific data we need for the database for each NHL year
        for i, year in enumerate(nhl_years):
            season = year['season']
            team_id = year['team']['id']
            toi = year['stat']['timeOnIce']
            games = year['stat']['games']
            starts = year['stat']['gamesStarted']
            wins = year['stat']['wins']
            losses = year['stat']['losses']
            shutouts = year['stat']['shutouts']
            saves = year['stat']['saves']
            pp_saves = year['stat']['powerPlaySaves']
            sh_saves = year['stat']['shortHandedSaves']
            even_saves = year['stat']['evenSaves']
            pp_shots = year['stat']['powerPlayShots']
            sh_shots = year['stat']['shortHandedShots']
            even_shots = year['stat']['evenShots']
            save_pct = year['stat']['savePercentage']
            gaa = year['stat']['goalAgainstAverage']
            shots_against = year['stat']['shotsAgainst']
            goals_against = year['stat']['goalsAgainst']
            sequence = year['sequenceNumber']
            
            # pre 2005-2006 OT games could end in ties & OT wins weren't tracked
            if season < '20052006':
                ties = year['stat']['ties']
                ot_wins = 'NULL'
            else:
                ties = 'NULL'
                ot_wins = year['stat']['ot']

            # individual save_pcts aren't saved if corresponding shot count is 0
            if pp_shots != 0:
                pp_save_pct = year['stat']['powerPlaySavePercentage']
            else:
                pp_save_pct = 0
            if sh_shots != 0:
                sh_save_pct = year['stat']['shortHandedSavePercentage']
            else:
                sh_save_pct = 0
            if even_shots != 0:
                even_save_pct = year['stat']['evenStrengthSavePercentage']
            else:
                even_save_pct = 0
        
            # correctly set active to handle edge case of players that are 
            # traded/reassigned mid-season
            if i < len(nhl_years) - 1:
                # past NHL season; active should be false
                active = False
            else:
                # current or last played NHL season
                if season == current_season:
                    active = True
                else:
                    # no NHL data for current season - in AHL/other league
                    active = False

            # ensure this season & sequence are in team_players table
            _team_players_check(player_id, team_id, season, active, sequence)

            # determine if a record already exists for this goalie/season
            select_cmd = (
                f"SELECT * FROM goalie_season_stats WHERE player_id = "
                f"{player_id} AND team_id = {team_id} AND season = "
                f"$${season}$$ AND sequence = {sequence}"
            )
            goalie_season_check = sql_select(db_connect, select_cmd, False)

            # insert/update goalie_season_stats record accordingly
            if goalie_season_check:
                # record exists for that goalie and season, just update data
                log_file.info(
                    f"> Existing record found, updating Goalie data for "
                    f"{player_id}'s {season} NHL season..."
                )
                update_cmd = (
                    f"UPDATE goalie_season_stats SET player_id = {player_id}, "
                    f"team_id = {team_id}, season = $${season}$$, "
                    f"time_on_ice = $${toi}$$, games = {games}, starts = "
                    f"{starts}, wins = {wins}, losses = {losses}, ties = "
                    f"{ties}, ot_wins = {ot_wins}, shutouts = {shutouts}, "
                    f"saves = {saves}, pp_saves = {pp_saves}, sh_saves = "
                    f"{sh_saves}, even_saves = {even_saves}, pp_shots = "
                    f"{pp_shots}, sh_shots = {sh_shots}, even_shots = "
                    f"{even_shots}, save_pct = {save_pct}, gaa = {gaa}, "
                    f"shots_against = {shots_against}, goals_against = "
                    f"{goals_against}, pp_save_pct = {pp_save_pct}, "
                    f"sh_save_pct = {sh_save_pct}, even_save_pct = "
                    f"{even_save_pct}, sequence = {sequence} WHERE "
                    f"player_id = {player_id} AND team_id = {team_id} AND "
                    f"season = $${season}$$ AND sequence = {sequence}"
                )
                # update skater data in the database
                status = sql_update(db_connect, update_cmd)
            else:
                # did not find a record for skater and season, insert new one
                log_file.info(f"> No record found, updating Goalie data for "
                    f"{player_id}'s {season} NHL season..."
                )
                insert_cmd = (
                    f"INSERT INTO goalie_season_stats (player_id, team_id, "
                    f"season, time_on_ice, games, starts, wins, losses, ties, "
                    f"ot_wins, shutouts, saves, pp_saves, sh_saves, "
                    f"even_saves, pp_shots, sh_shots, even_shots, save_pct, "
                    f"gaa, shots_against, goals_against, pp_save_pct, "
                    f"sh_save_pct, even_save_pct, sequence) VALUES ("
                    f"{player_id}, {team_id}, $${season}$$, $${toi}$$, "
                    f"{games}, {starts}, {wins}, {losses}, {ties}, "
                    f"{ot_wins}, {shutouts}, {saves}, {pp_saves}, {sh_saves}, "
                    f"{even_saves}, {pp_shots}, {sh_shots}, {even_shots}, "
                    f"{save_pct}, {gaa}, {shots_against}, {goals_against}, "
                    f"{pp_save_pct}, {sh_save_pct}, {even_save_pct}, "
                    f"{sequence})"
                )
                # insert goalie season stats into the database
                status = sql_insert(db_connect, insert_cmd)

            # log successful upload; already logging database errors
            if status == 0:
                if season == current_season:
                    log_file.info(f">> Finished loading all data for goalie "
                        f"{player_id} with the {season} season...")
                else:
                    log_file.info(f">> Successfully inserted data for goalie "
                        f"{player_id} ({season} season) to the "
                        f"goalie_season_stats table...")
    
    log_file.info(f">> Completed pulling yearByYear goalie stats using list "
        f"from configuration file...")

def _team_players_check(player, team, season, active, seq):
    '''
    Given a player's stats for a particular season and sequence, determine if
    there is a corresponding record in the team_players table.

    If there's not a matching record, add one from the given data. This new 
    record is necessary to be able to create a related stats record for that
    player/season in skater_season_stats.
    '''

    # check if record in team_players exists that matches provided column data
    cmd = (
        f"SELECT EXISTS("
        f"SELECT 1 FROM team_players WHERE player_id = {player} AND team_id "
        f"= {team} AND season = $${season}$$ AND sequence = {seq})"
    )
    check = sql_select(db_connect, cmd, False)
    # SQL command we run only returns True/False, so we have to parse that 
    # out of the tuple returned by psycopg's cursor.fetchone()
    check = check[0]

    if not check:
        # record doesn't exist in team_players; create it ourselves now
        insert_cmd = (
            f"INSERT INTO team_players (player_id, team_id, season, active, "
            f"sequence) VALUES ({player}, {team}, $${season}$$, {active}, "
            f"{seq})"
        )
        insert_status = sql_insert(db_connect, insert_cmd)
        if insert_status == 0:
            log_file.info(f">>> Added additional record to team_players for "
                f"player {player} ({season})..."
            )
        return 0
    else:
        # record exists
        return 1

def _get_player_sequence(url, team):
    '''
    Given the player's NHL API endpoint (i.e. /api/v1/people/8473563) and an NHL team_id, return the sequence number for the player's current season at
    that team.

    This is needed to determine whether a player has been traded mid-season,
    reassigned to the AHL and called up again, etc.
    '''

    # only want player id from the provided link
    player = url.split('/')[4]

    # create link to player's stats to get sequence number for curr season
    link = f"{nhl_players}/{player}/stats?stats=yearByYear"

    # request intial data using link
    log_file.info(f"Starting to get player sequence from {link}...")
    years = request_data(link)

    # only want current year data to find what sequence is for that team
    for key in years.keys():
        if key == 'stats':
            years = years[key][0]['splits']

    found = []
    # pdb.set_trace()
    for year in years:
        if year['season'] == current_season:
            found.append(year)
    # now find most recent team sequence number (if applicable)
    if len(found) >= 1:
        for i in found:
            if i['league']['name'] == 'National Hockey League' and \
                i['team']['name'] == team:
                seq = i['sequenceNumber']
    else:
        # less than/equal to zero - something went wrong
        log_file.warning(
            f"Could not find sequence data for {player}...likely no NHL stats "
            f"for season {current_season}...not adding to database."
        )
        return None
    
    # check whether sequence was found
    if 'seq' not in locals():
        log_file.warning(
            f"Could not find sequence data for {player}...likely no NHL stats "
            f"for season {current_season}...not adding to database."
        )
        return None
    else:
        log_file.info(f"Found player {player}'s team sequence: {seq}...")
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
        log_file.info(f"Starting NHL Data Pull at {now}...")
    except:
        sys.exit(f"Logging failed to setup...exiting at time {now}...")

    # setup environment variables from config file
    log_file.info('Setting up environment variables from config file...')
    nhl_site = config['LINKS']['site']
    nhl_base = config['LINKS']['base']
    nhl_teams = config['LINKS']['teams']
    nhl_players = config['LINKS']['players']
    nhl_teams_list = config['TEAMS']['LIST']
    nhl_players_teamIds = config['PLAYERS']['TEAM_ID']
    nhl_players_list = config['PLAYERS']['LIST']
    current_season = config['DEFAULT']['SEASON']

    # setup stats API endpoints from config file
    stats_list = config['STATS']['LIST']
    stats_byYear = config['STATS']['yearByYear']
    stats_skatersByYear = config['STATS']['skatersByYear']
    stats_goaliesByYear = config['STATS']['goaliesByYear']

    # get database credentials from config file
    log_file.info('Setting database credentials from config file...')
    db_user = config['DATABASE']['USER']
    db_passwd = config['DATABASE']['PASSWORD']
    db_host = config['DATABASE']['CONNECTION']
    db_name = config['DATABASE']['DB_NAME']
    db_port = config['DATABASE']['PORT']

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

    if stats_list == 'ALL':
        log_file.info('Pulling year-by-year stats for NHL skaters...')
        _skaterStats_yearByYear()
        log_file.info('Pulling year-by-year stats for NHL goalies...')
        _goalieStats_yearByYear()
    elif stats_list == 'SKATERS':
        # just get skaters season-by-season stats
        log_file.info('Pulling year-by-year stats for NHL skaters...')
        _skaterStats_yearByYear()
    elif stats_list == 'GOALIES':
        # just get goalies season-by-season stats
        log_file.info('Pulling year-by-year stats for NHL goalies...')
        _goalieStats_yearByYear()
    else:
        # as of now, do nothing
        log_file.info('Not getting any player stats...')

    # close database connection
    db_connect.close()