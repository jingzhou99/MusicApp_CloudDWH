import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN=config.get('IAM_ROLE','ARN')
LOG_DATA=config.get('S3','LOG_DATA')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
SONG_DATA=config.get('S3','SONG_DATA')

# DROP TABLES

staging_events_table_drop = 'DROP TABLE IF EXISTS "staging_events";'
staging_songs_table_drop = 'DROP TABLE IF EXISTS staging_songs;'
songplay_table_drop = 'DROP TABLE IF EXISTS "songplays";'
user_table_drop = 'DROP TABLE IF EXISTS "users";'
song_table_drop = 'DROP TABLE IF EXISTS "songs";'
artist_table_drop = 'DROP TABLE IF EXISTS "artists";'
time_table_drop = 'DROP TABLE IF EXISTS "time";'

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE "staging_events" (
    "artist" VARCHAR,
    "auth" VARCHAR,
    "firstName" VARCHAR,
    "gender" VARCHAR,
    "itemInSession" INT,
    "lastName" VARCHAR,
    "length" NUMERIC(9,5),
    "level" VARCHAR,
    "location" VARCHAR,
    "method" VARCHAR,
    "page" VARCHAR,
    "registration" BIGINT,
    "sessionId" INT,
    "song" VARCHAR,
    "status" INT,
    "ts" BIGINT,
    "userAgent" TEXT,
    "userId" INT
    );
""")

staging_songs_table_create = ("""
CREATE TABLE "staging_songs" (
    "num_songs" INT, 
    "artist_id" VARCHAR, 
    "artist_latitude" numeric(8,5), 
    "artist_longitude" numeric(8,5),
    "artist_location" VARCHAR, 
    "artist_name" VARCHAR,
    "song_id" VARCHAR, 
    "title" VARCHAR, 
    "duration" NUMERIC(9,5), 
    "year" INT
    );
""")


artist_table_create = ("""
CREATE TABLE "artists" (
    "artist_id" varchar PRIMARY KEY, 
    "name" varchar NOT NULL, 
    "location" varchar, 
    "latitude" numeric(8,5), 
    "longitude" numeric(8,5)
);
""")

song_table_create = ("""
CREATE TABLE "songs" (
    "song_id" varchar PRIMARY KEY, 
    "title" varchar NOT NULL, 
    "artist_id" varchar, 
    "year" int, 
    "duration" numeric(9,5),
    FOREIGN KEY ("artist_id") REFERENCES "artists"
) DISTKEY(song_id);
""")

user_table_create = ("""
CREATE TABLE "users" (
    "user_id" int PRIMARY KEY, 
    "first_name" varchar(20) NOT NULL, 
    "last_name" varchar(20) NOT NULL, 
    "gender" varchar(5), 
    "level" varchar
);
""")

time_table_create = ("""
CREATE TABLE "time" (
    "start_time" TIMESTAMPTZ PRIMARY KEY, 
    "hour" int2, 
    "day" int2, 
    "week" int2, 
    "month" int2, 
    "year" int2, 
    "weekday" int2
);
""")

songplay_table_create = ("""
CREATE TABLE "songplays" (
    "songplay_id" INT IDENTITY(0,1) PRIMARY KEY, 
    "start_time" TIMESTAMPTZ NOT NULL, 
    "user_id" int NOT NULL, 
    "level" varchar, 
    "song_id" varchar, 
    "artist_id" varchar, 
    "session_id" int NOT NULL, 
    "location" varchar, 
    "user_agent" varchar,
    FOREIGN KEY ("user_id") REFERENCES "users",
    FOREIGN KEY ("song_id") REFERENCES "songs",
    FOREIGN KEY ("start_time") REFERENCES "time"
) DISTKEY(song_id);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    json {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto';
""").format(SONG_DATA, ARN)


# FINAL TABLES

artist_table_insert = ("""
BEGIN transaction;
    DROP TABLE IF EXISTS temp_artists;
    
    -- only copy the distinct value from "staging_songs" to "temp_artists"
    CREATE TABLE temp_artists AS
    SELECT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM
        (SELECT
            ROW_NUMBER() OVER (PARTITION BY artist_id order by artist_id) AS id_rank,
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM
            staging_songs
        ) r
    WHERE r.id_rank = 1;

    -- compare, and remove duplicate value in target table ""
    DELETE FROM artists
    USING temp_artists
    WHERE temp_artists.artist_id = artists.artist_id;

    INSERT INTO artists (
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude
        )
    SELECT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM
        temp_artists;
        
    DROP TABLE temp_artists;

END transaction;
""")


song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title, 
    artist_id, 
    year, 
    duration
    ) 
SELECT
    s.song_id,
    s.title,
    s.artist_id,
    s.year,
    s.duration
FROM
    staging_songs s
;
""")

user_table_insert = ("""
BEGIN transaction;
    DROP TABLE IF EXISTS temp_users;

    --only copy distinct value from staging_events 
    CREATE TABLE temp_users AS
    SELECT
        userId,
        firstName,
        lastName,
        gender,
        level
    FROM
        (SELECT
            ROW_NUMBER() OVER (PARTITION BY userId order by ts DESC) AS id_rank,
            userId,
            firstName,
            lastName,
            gender,
            level
        FROM
            staging_events
        WHERE
            userId IS NOT NULL
        ) u
    WHERE u.id_rank = 1;
    
    -- remove duplicates in target table, before dump
    DELETE FROM "users"
    USING temp_users
    WHERE users.user_id = temp_users.userId;

    INSERT INTO "users" (
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level )
    SELECT
        userId,
        firstName,
        lastName,
        gender,
        level
    FROM
        temp_users;

    DROP TABLE temp_users;    
END transaction;
""")

time_table_insert = ("""
BEGIN transaction;
    --create temp table, put all the time data into it
    CREATE TEMP TABLE IF NOT EXISTS temp_table (like time);
    TRUNCATE temp_table;

    INSERT INTO temp_table
    SELECT * FROM time;

    INSERT INTO temp_table
    SELECT
        (timestamp 'epoch' + (e.ts/1000) * interval '1 second') AS "start_time", --13 digits int, which means ms precision, so devide by 1000
        date_part('hour',(timestamp 'epoch' + (e.ts/1000) * interval '1 second')) AS "hour",
        date_part('day',(timestamp 'epoch' + (e.ts/1000) * interval '1 second')) AS "day",
        date_part('week',(timestamp 'epoch' + (e.ts/1000) * interval '1 second')) AS "week",
        date_part('month',(timestamp 'epoch' + (e.ts/1000) * interval '1 second')) AS "month",
        date_part('year',(timestamp 'epoch' + (e.ts/1000) * interval '1 second')) AS "year",
        date_part('dow',(timestamp 'epoch' + (e.ts/1000) * interval '1 second')) AS "weekday"
    FROM    
        staging_events e
    WHERE
        e."page"='NextSong';
    
    --empty time table, and write back only distinct time data
    TRUNCATE time;

    INSERT INTO time
    SELECT
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday
    FROM
        (SELECT
             ROW_NUMBER()OVER(PARTITION BY start_time ORDER BY start_time) AS rank_num,
             temp_table.*     
         FROM 
             temp_table
        ) AS tt
    WHERE tt.rank_num = 1;
    
    DROP TABLE temp_table;

END transaction;
""")

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
    ) 
SELECT
    (timestamp 'epoch' + (e.ts/1000) * interval '1 second') AS start_time,
    e.userId as user_id,
    e.level AS level,
    sa.song_id AS song_id,
    sa.artist_id AS artist_id,
    e.sessionId AS session_id,
    e.location AS location,
    e.userAgent AS user_agent
FROM 
    staging_events AS e
JOIN 
    (SELECT
        s.song_id,
        s.title,
        s.duration,
        a.artist_id,
        a.name
    FROM songs AS s
    JOIN artists AS a ON s.artist_id = a.artist_id
    ) AS sa
    ON (e.song=sa.title AND e.length=sa.duration AND e.artist=sa.name)
WHERE
    e."page"='NextSong';
""")


# QUERY LISTS
# Although redshift does not enforce PrimaryKey, ForeignKey, here all the DDL DML operations are follow the strict order as regular RDBMS.
create_table_queries = [staging_events_table_create, staging_songs_table_create, artist_table_create, song_table_create, user_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [artist_table_insert, song_table_insert, user_table_insert, time_table_insert, songplay_table_insert]
