'''
Description: Pull user-specified data from the NHL-records site.

Return player- and team-related data by accessing the NHL website's
API. 

Command-line Arguments:

'''

__version__ = '0.1'
__title__ = 'nhl_pull_data'
__author__ = 'Paul Hegedus'

import os
import sys
#import subprocess
import requests
import argparse
#import numpy as np
import pandas as pd
#import matplotlib.pyplot as plt

class MyData:
    '''Download and store the NHL data.'''

    def __init__(self, sites, base):
        '''Initialize MyData instance; self.sites is a tuple where 1st value
        is the name of the selected NHL data option, 2nd value is the API
        endpoint to pull the data from.'''
        self.sites = tuple(sites)
        self.base = str(base)

    def Pull(self):
        '''Download data from NHL site specified by self.sites.'''

        self.endpoint = self.sites[1]
        self.api = self.base + self.endpoint
        print('Pulling data from: ' + self.api)
        self.r = requests.get(self.api)
        if self.r.status_code == 200:
            # successful request, return data
            print('Status: ' + str(self.r.status_code))
            return self.r.json()
        else:
            # print exception message since request failed
            self.r.raise_for_status()
            sys.exit('Exiting after failing to pull data for ' + self.k)
            return None

    def Store_Data(self, json):
        '''Save the JSON data pulled from the NHL site.'''

        self.json = json
        for self.k in self.json.keys():
            if self.k != 'copyright':
                # create a Pandas DataFrame from the NHL data
                self.df = pd.DataFrame(self.json[self.k])
        # do some more data formatting      
        print(self.df)
        #print(self.json[self.k])
        return self.df

    def Parse_Teams(self, df):
        '''Parse the NHL team data into a more manageable format for
        database use.'''

        self.df = df
        for self.i in self.df.index:
            for self.name in self.df.columns:
                self.rec = self.df[self.name][self.i]
                if isinstance(self.rec, list) == True or \
                isinstance(self.rec, dict) == True:
                    print(self.rec)
        return None

def argsetup():
    parser = argparse.ArgumentParser(description =
                'Read in player/team data from the NHL\'s website.')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--teams', action='store_true',
                       help='pull data about all the NHL teams')
    group.add_argument('--players', action='store_true', dest='people',
                       help='pull data about past & present NHL players')
    group.add_argument('--conf', action='store_true', dest='conferences',
                       help='pull data about NHL conferences')
    group.add_argument('--div', action='store_true', dest='divisions',
                       help='pull data about NHL divisions')
    group.add_argument('--draft', action='store_true',
                       help='pull data about previous NHL Entry Drafts')
    group.add_argument('--prospects', action='store_true',
                       help='pull data about NHL Entry Draft prospects')
    group.add_argument('--game', action='store_true',
                       help='pull data about past NHL games')
    group.add_argument('--schedule', action='store_true',
                       help='pull data related to the NHL schedule')
    group.add_argument('--standings', action='store_true',
                       help='pull data related to the NHL standings')
    group.add_argument('--stats', action='store_true', dest='statTypes',
                       help='return list of specific player stat types')
    a = parser.parse_args()
    return a

#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

if __name__ == '__main__':
    # set variable with curr dir
    cwd = os.getcwd()

    # set base URL for NHL API
    nhl_base = 'https://statsapi.web.nhl.com/api/v1'

    # get command-line arguments
    args = argsetup()

    # set API endpoints based on argument; store in tuple
    #  (a, b)
    #    a : argument
    #    b : API endpoint
    #nhl_args = ()
    for arg in vars(args):
        # check if argument was used (i.e., arg_check = True)
        arg_check = getattr(args, arg)
        if arg_check:
            temp = '/' + arg
            nhl_args = (arg, temp)
            break

    # Initiate MyData class with command-line args
    d = MyData(nhl_args, nhl_base)

    # pull data from NHL site based on passed in arguments
    nhl_out = d.Pull()

    # store the output
    if nhl_out:
        df = d.Store_Data(nhl_out)
        if arg == 'teams':
            d.Parse_Teams(df)
    else:
        sys.exit('No arguments provided for data pull...exiting!')
