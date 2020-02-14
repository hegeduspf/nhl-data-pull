# nhl-data-pull #
***NOTE: THIS REPOSITORY IS STILL IN THE EARLY STAGES OF DEVELOPMENT!***
---
## Description ##
**Program:** nhl_data_pull.py

**Author:** Paul Hegedus

**Current Version:** v1.0

**Conventions:**
*   PEP 8 (spaces not tabs; leading _ on internal functions)
*   Class method names are capitalized, while class data attributes are not.
    This is to avoid accidental name conflicts resulting in a data attribute
    overriding a method attribute.

**Usage:** Read in player/team data from the NHL's website.

    python nhl_data_pull.py [-h]
                            (--teams | --players | --conf | --div | --draft | --prospects | --game | --schedule | --standings | --stats)

    optional arguments:

        -h, --help  show this help message and exit
        --teams     pull data about all the NHL teams
        --players    pull data about past & present NHL players
        --conf       pull data about NHL conferences
        --div        pull data about NHL divisions
        --draft      pull data about previous NHL Entry Drafts
        --prospects  pull data about NHL Entry Draft prospects
        --game       pull data about past NHL games
        --schedule   pull data related to the NHL schedule
        --standings  pull data related to the NHL standings
        --stats      return list of specific player stat types

