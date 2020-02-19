/* File used to create tables in the nhl_data 
PostgreSQL table. */

/* Drop Tables
DROP TABLE team_players;
DROP TABLE players;
DROP TABLE teams;
*/

/* Initial table creation */

CREATE TABLE "teams" (
    "id" int PRIMARY KEY,
    "name" varchar,
    "abbreviation" char(3),
    "conf_id" int UNIQUE NOT NULL,
    "division_id" int UNIQUE NOT NULL,
    "franchise_id" int UNIQUE NOT NULL,
    "active" boolean
);

CREATE TABLE "players" (
    "id" int PRIMARY KEY,
    "full_name" varchar,
    "link" varchar,
    "current_age" int,
    "nationality" char(3),
    "height" varchar,
    "weight" int,
    "active" boolean,
    "rookie" boolean,
    "shoots_catches" char(1),
    "position_code" char(2),
    "position_name" varchar,
    "position_type" varchar
);

CREATE TABLE "team_players" (
    "team_id" int,
    "player_id" int,
    PRIMARY KEY ("team_id", "player_id")
);

/* Add foreign key references */

ALTER TABLE "team_players" ADD FOREIGN KEY ("team_id") REFERENCES "teams" ("id");
ALTER TABLE "team_players" ADD FOREIGN KEY ("player_id") REFERENCES "players" ("id");
