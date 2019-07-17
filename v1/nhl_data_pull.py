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
        '''Initialize MyData instance; ensure self.sites is a dict'''
        self.sites = dict(sites)
        self.base = str(base)

    def pull(self):
        '''Download data from NHL site specified by self.sites.'''

        for self.k in self.sites.keys():
            self.endpoint = self.sites[self.k]
            self.api = self.base + self.endpoint
            print('Pulling data from: ' + self.api)
            self.r = requests.get(self.api)
            if self.r.status_code == 200:
                print('Status: ' + str(self.r.status_code))
                return self.r.json()
            else:
                self.r.raise_for_status()
                sys.exit('Exiting after failing to pull data for ' + self.k)
                return None

    def store_data(self, json):
        '''Save the JSON data pulled from the NHL site.'''

        self.json = json
        for self.k in self.json.keys():
            if self.k != 'copyright':
                # create a Pandas DataFrame from the NHL data
                self.df = pd.DataFrame(self.json[self.k])

        # do some more data formatting      
        print(self.df)
        #print(self.json[self.k])
        return None

def argsetup():
    parser = argparse.ArgumentParser(description =
                'Read in player/team data from the NHL\'s website.')
    parser.add_argument('--teams', help='pull data about all the NHL teams',
                        action='store_true')
    #parser.add_argument('--people', help='pull data about all NHL players',
    #                    action='store_true')
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

    # set API endpoints based on arguments; store in dictionary
    #   key   : argument
    #   value : API endpoint
    nhl_args = {}
    for arg in vars(args):
        # check if argument is to be used
        arg_check = getattr(args, arg)
        if arg_check:
            temp = '/' + arg
            nhl_args.update({arg:temp})

    # Initiate MyData class with command-line args
    d = MyData(nhl_args, nhl_base)

    # pull data from NHL site based on passed in arguments
    nhl_out = d.pull()

    # store the output
    if nhl_out:
        d.store_data(nhl_out)
    else:
        sys.exit('No arguments provided for data pull...exiting!')

    # rest of API endpoints...should eventually be set
    # using command-line args
    nhl_confs = nhl_base + '/conferences'
    nhl_divs = nhl_base + '/divisions'
    nhl_draft = nhl_base + '/draft'
    nhl_prospect = nhl_draft + '/prospects'
    nhl_game = nhl_base + '/game'
    nhl_player = nhl_base + '/people'
    nhl_schedule = nhl_base + '/schedule'
    nhl_standings = nhl_base + '/standings'
    nhl_stats = nhl_base + '/statTypes'

