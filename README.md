# nhl-data-pull #

## Overview ##
**Program:** nhl_data_pull.py

**Author:** Paul Hegedus

**Description:** This program pulls team and player data from the NHL website's publicly
available API. It uses a configuration file read in from the command line
to request the data from the API and establish a connection to the requisite
database. This program is configured to work with a PostgreSQL database using the psycopg2 connector.

**Current Version:** v2.0

**Conventions:**
*   PEP 8 (spaces not tabs; leading _ on internal functions)

## Usage ##

`nhl_data_pull.py [-h] configf`

> Read in player/team data from the NHL's website.
>
> **Positional arguments:**
> - configf --> configuration file
> 
> **Optional arguments:**
> - -h, --help --> show this help message and exit

A default configuration file can be found at nhl-data-pull/config/nhl_data.ini. The settings in the default config file will download all NHL team and player data that is currently available with the current version of the program.

## Database Assumptions ##
This program works under the assumption that it is run in an environment with a configured Postgres database. The repository contains an SQL file to create the necessary tables in the database - located at nhl-data-pull/config/create_table.sql.

The default config file details the database credentials the program needs to connect and access the data. The config file provides some default info about the database. You can choose to either create a database and user as specified by the default settings of the config file, or set it up on your own and simply change the settings in the config file.

Default database settings are:
* **Name**: nhl_data
* **User**: nhl_user
* **Password**: ******* (**MUST BE CHANGED IN CONFIG FILE**)
* **Connection**: localhost

## Configuration File ##
The configuration file - located at nhl-data-pull/config/nhl_data.ini - is used as a command-line argument for the nhl_data_pull.py program.

### Format ###
Built based on the format used by Python's configparser() class. Allows for easy and flexible customization by end users. A more detailed explanation of INI-type config files can be found at https://docs.python.org/3/library/configparser.html.

### Available Sections ###
Each section of the config file is enclosed in hard brackets. Underneath each section are key-value pairs, which is where we pull the information we're looking for.

#### DEFAULT ####
Contains default settings that every subsequent section within the config file will have unless overridden within that subsection.
###### LIST ######
Set to NONE by default. LIST is used by the program to determine if a particular section should be run or not. Can additionally be set to ALL, in which case every section will be run.
###### SEASON ######
Used to set the current NHL season when running the program. Should be in the following format: '20192020'.
###### LOGDIR ######
Directory path where the logs are stored when the program is run. Default setting is '{HOME}/logs', but can/should be changed to whatever location is best suited to the user.

#### DATABASE ####
Contains the settings needed by the Psycopg2 Connector to establish a connection to the database in the program. All settings have default values set in nhl-data-pull/config/nhl_data.ini, so user must update this section accordingly if database parameters are not setup to match.
###### USER ######
Database username used to connect to the database and perform SQL Select, Insert, and Update commands throughout the program. Defaults as 'nhl_user'.
###### PASSWORD ######
Database password for the corresponding database user.
###### DB_NAME ######
Database name that contains the tables/will contain the tables to be updated with the NHL data. Default setting is 'nhl_data'.
###### CONNECTION ######
Database server address. Defaults as 'localhost'. Alternatively can be set to an IP address.
###### PORT ######
The port number used to connect to the database. Defaults as 5432.

#### LINKS ####
Contains the link to the NHL API and it's associated endpoints that are used throughout the program. A fully-detailed look at the NHL Stats API used throughout this program can be found at gitlab.com/dword4/nhlapi/-/blob/master/stats-api.md.
###### site ######
This is the link to the main NHL API site. Unless it's changed in the future, it should remain 'https://statsapi.web.nhl.com'.
###### base ######
This is the base link used for the subsequent API endpoints. It should incorporate the main NHL API site.
###### teams ######
This is the link to the NHL Teams data - generated using the base API and the teams API endpoint.
###### players ######
This is the link to the NHL Player data - generated using the base API and the players API endpoint.

#### TEAMS ####
Settings specific to the part of the program that downloads NHL Team-specific data to load into the database.
###### LIST ######
The DEFAULT section initially sets this to 'NONE'. Users can set LIST to 'ALL' in this section, while not specifying LIST in the other sections, to only run the NHL Teams portion of the program.

#### PLAYERS ####
Settings specific to the part of the program that downloads NHL Player-specific data to load into the database. Running this section also updates the team_players bridge table that relates the NHL Teams and NHL Players data within the database.
###### LIST ######
The DEFAULT section initially sets this to 'NONE'. Users can set LIST to 'ALL' in this section, while not specifying LIST in the other sections, to only run the NHL Players portion of the program.
###### TEAM_ID ######
Can be set to 'ALL' or a comma-separated list of Team IDs (i.e. 15, 54, 21). Setting to 'ALL' cycles through all NHL Teams and gets corresponding NHL Player data. Alternatively, if a list of Team IDs is provided in the config file, only NHL Players from those teams are added/updated in the database.

#### STATS ####
Settings specific to the part of the program that downloads NHL Stats to load into the database. 
###### LIST ######
Defaults to 'NONE' from the [DEFAULT] section. User can set it in this section to 'ALL', 'SKATERS', or 'GOALIES'. The NHL Stats API has different stats recorded for Goalies vs. Skaters (i.e. Forwards/Defenseman). Therefore, the user can download statistics for all players at once (LIST = ALL), just Goalies (LIST = GOALIES), or just Skaters (LIST = SKATERS).
###### yearByYear ######
The modifier added to an NHL Player profile's API endpoint to access individual player statistics for each season they've played. Defaults as 'stats?stats=yearByYear'.
###### skatersByYear ######
Can be set to 'ALL' or a space-separated list of Player IDs (i.e. 8476880 8477949 8475455). Setting to 'ALL' gets the individual stats for every Skater listed in the database. Alternatively, if a list of Player IDs is provided in the config file, the program only gets stats for those individual Skaters.
###### goaliesByYear ######
Can be set to 'ALL' or a space-separated list of Player IDs (i.e. 8471306 8469608 8473575). Setting to 'ALL' gets the individual stats for every Goalie listed in the database. Alternatively, if a list of Player IDs is provided in the config file, the program only gets stats for those individual Goalies.
