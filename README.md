# nhl-data-pull #
***NOTE: THIS REPOSITORY IS STILL IN THE EARLY STAGES OF DEVELOPMENT!***
---

## Overview ##
**Program:** nhl_data_pull.py

**Author:** Paul Hegedus

**Description:** This program pulls team and player data from the NHL website's publicly
available API. It uses a configuration file read in from the command line
to request the data from the API and establish a connection to the requisite
database.

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


