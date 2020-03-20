/* File used to create tables in the nhl_data 
PostgreSQL table. */

/* Drop Tables */
-- DROP TABLE skater_season_stats;
-- DROP TABLE team_players;
-- DROP TABLE players;
-- DROP TABLE teams;


/* Initial table creation */

CREATE TABLE "teams" (
    "id" int PRIMARY KEY,
    "name" varchar,
    "abbreviation" char(3),
    "conf_id" int NOT NULL,
    "division_id" int NOT NULL,
    "franchise_id" int UNIQUE NOT NULL,
    "active" boolean
);

CREATE TABLE "players" (
    "id" int PRIMARY KEY,
    "full_name" varchar,
    "link" varchar,
    "current_age" int,
    "nationality" char(3),
    "active" boolean,
    "rookie" boolean,
    "shoots_catches" char(1),
    "position_code" char(2),
    "position_name" varchar,
    "position_type" varchar
);

CREATE TABLE "team_players" (
    "player_id" int,
    "team_id" int,
    "season" char(8),
    "active" boolean,
    "sequence" int,
    PRIMARY KEY ("player_id", "team_id", "season", "sequence")
);

CREATE TABLE "skater_season_stats" (
    "player_id" int,
    "team_id" int,
    "season" char(8),
    "time_on_ice" varchar,
    "games" int,
    "assists" int,
    "goals" int,
    "pim" int,
    "shots" int,
    "hits" int,
    "pp_goals" int,
    "pp_points" int,
    "pp_toi" varchar,
    "even_toi" varchar,
    "faceoff_pct" float8,
    "shot_pct" float8,
    "gw_goals" int,
    "ot_goals" int,
    "sh_goals" int,
    "sh_points" int,
    "sh_toi" varchar,
    "blocked_shots" int,
    "plus_minus" int,
    "points" int,
    "shifts" int,
    "sequence" int,
    PRIMARY KEY ("player_id", "team_id", "season", "sequence")
);

/* Add foreign key references */
ALTER TABLE "team_players" ADD FOREIGN KEY ("team_id") REFERENCES "teams" ("id");
ALTER TABLE "team_players" ADD FOREIGN KEY ("player_id") REFERENCES "players" ("id");
ALTER TABLE "skater_season_stats" ADD FOREIGN KEY ("player_id", "team_id", "season", "sequence") REFERENCES "team_players" ("player_id", "team_id", "season", "sequence");
