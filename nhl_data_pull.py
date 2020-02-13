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
import argparse
import pandas as pd
#import subprocess
#import numpy as np
#import matplotlib.pyplot as plt

from configparser import ConfigParser
from pprint import pprint

def argsetup():
    parser = argparse.ArgumentParser(description =
                'Read in player/team data from the NHL\'s website.')
    parser.add_argument('configf', help='configuration file')
    a = parser.parse_args()
    return a

def request_data(url):
    '''Request data from specified URL'''

    r = requests.get(url)
    if r.status_code == 200:
        # successful request, return data
        return r.json()
    else:
        # print exception message since request failed
        r.raise_for_status()
        sys.exit('Exiting after failing to pull data from %s' % url)
        return None


# def Store_Data( json):
#     '''Save the JSON data pulled from the NHL site.'''
#     json = json
#     for k in json.keys():
#         if k != 'copyright':
#             # create a Pandas DataFrame from the NHL data
#             df = pd.DataFrame(json[k])
#     # do some more data formatting      
#     pprint(df)
#     #print(json[k])
#     return df
# def Parse_Teams( df):
#     '''Parse the NHL team data into a more manageable format for
#     database use.'''
#     df = df
#     for i in df.index:
#         for name in df.columns:
#             rec = df[name][i]
#             if isinstance(rec, list) == True or \
#             isinstance(rec, dict) == True:
#                 pprint(rec)
#     return None

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

    # parse API endpoints from config file
    nhl_teams = config['LINKS']['teams']
    nhl_players = config['LINKS']['players']

    # pull variables from config file
 
    # set API endpoints based on config file vars
    #    a : argument
    #    b : API endpoint
    # for arg in vars(args):
    #     # check if argument was used (i.e., arg_check = True)
    #     arg_check = getattr(args, arg)
    #     if arg_check:
    #         temp = '/' + arg
    #         nhl_args = (arg, temp)
    #         break

    # Initiate MyData class with command-line args
    nhl_out = request_data(nhl_teams)

    # store the output
    if nhl_out:
        #df = d.Store_Data(nhl_out)
        #if arg == 'teams':
        #    d.Parse_Teams(df)
            for k in nhl_out.keys():
                if k != 'copyright':
                    pprint(nhl_out[k])
    else:
        sys.exit('No arguments provided for data pull...exiting!')
