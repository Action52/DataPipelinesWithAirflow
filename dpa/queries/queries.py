def delete_table(table):
    return f"""
        DROP TABLE IF EXISTS {table};
    """


def staging_songs_table_create(table):
    return f"""
        CREATE TABLE IF NOT EXISTS {table}
            (
                song_id                   VARCHAR,
                num_songs                 VARCHAR,
                title                     VARCHAR,
                artist_name               VARCHAR,
                artist_latitude           VARCHAR,
                year                      VARCHAR,
                duration                  VARCHAR,
                artist_id                 VARCHAR,
                artist_longitude          VARCHAR,
                artist_location           VARCHAR
            ) DISTSTYLE EVEN;
        """


def staging_songs_table_copy(table, s3_path, iam_arn):
    return f"""
        COPY {table} 
        FROM '{s3_path}'
        CREDENTIALS 'aws_iam_role={iam_arn}'
        FORMAT AS JSON 'auto'
        REGION 'us-east-1' ;
    """


def staging_logs_table_create(table):
    return f"""
        CREATE TABLE IF NOT EXISTS {table} 
                (
                    artist            VARCHAR, 
                    auth              VARCHAR, 
                    firstname         VARCHAR, 
                    gender            VARCHAR, 
                    iteminsession     VARCHAR, 
                    lastName          VARCHAR,
                    length            VARCHAR, 
                    level             VARCHAR, 
                    location          VARCHAR, 
                    method            VARCHAR, 
                    page              VARCHAR, 
                    registration      VARCHAR,
                    sessionid         VARCHAR, 
                    song              VARCHAR, 
                    status            VARCHAR, 
                    ts                VARCHAR, 
                    useragent         VARCHAR, 
                    userid            INTEGER
                ) DISTSTYLE EVEN;
    """


def staging_logs_table_copy(table, s3_path, iam_arn, json_conf):
    return f"""
        COPY {table} 
        FROM '{s3_path}'
        CREDENTIALS 'aws_iam_role={iam_arn}'
        FORMAT JSON AS '{json_conf}'
        REGION 'us-east-1';
    """


def songplays_table_create(table):
    return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            songplay_id        INTEGER IDENTITY(0,1),
            start_time         VARCHAR,
            user_id            INTEGER,
            level              VARCHAR(10),
            song_id            VARCHAR(20),
            artist_id          VARCHAR(20),
            session_id         INTEGER,
            location           VARCHAR(200),
            user_agent         TEXT   
        ) DISTSTYLE EVEN;
    """


def songplays_table_insert(table, raw_songs_table, raw_logs_table):
    return f"""
        INSERT INTO {table} (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        WITH songs AS (
            SELECT 
                d.song_id AS song_id,
                d.title AS song_name
            FROM {raw_songs_table} d JOIN {raw_logs_table} s ON d.title = s.song
            WHERE 
                d.song_id IS NOT NULL AND
                s.song IS NOT NULL AND
                d.song_id != '' AND
                s.song != ''
        ), artists AS (
            SELECT 
                d.artist_id AS artist_id,
                d.artist_name AS artist_name
            FROM {raw_songs_table} d JOIN {raw_logs_table} s ON d.artist_name = s.artist
            WHERE 
                d.artist_name IS NOT NULL AND
                s.artist IS NOT NULL AND
                d.artist_name != '' AND
                s.artist != ''
        )
        SELECT
            ts,
            userid AS user_id,
            level,
            (SELECT song_id FROM songs 
                WHERE song_name = song AND song_name != '' AND song_name IS NOT NULL LIMIT 1) AS song_id,
            (SELECT artist_id FROM artists 
                WHERE artist_name = artist AND artist_name != '' AND artist_name IS NOT NULL LIMIT 1) AS artist_id,
            CAST(sessionid AS INTEGER),
            location,
            useragent as user_agent
        FROM {raw_logs_table}
        WHERE song_id IS NOT NULL AND artist_id IS NOT NULL;
    """


def dimension_user_create(table):
    return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            user_id       INTEGER NOT NULL,
            first_name    VARCHAR(30),
            last_name     VARCHAR(30) SORTKEY,
            gender        VARCHAR(3),
            level         VARCHAR(10)
        ) DISTSTYLE ALL;
    """


def dimension_time_create(table):
    return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            start_time  VARCHAR(20) NOT NULL SORTKEY,
            hour        VARCHAR(10),
            day         VARCHAR(10), 
            week        VARCHAR(10),
            month       VARCHAR(10), 
            year        VARCHAR(10), 
            weekday     VARCHAR(10)
        ) DISTSTYLE ALL;
    """


def dimension_song_create(table):
    return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            song_id       VARCHAR(20) NOT NULL,
            title         VARCHAR(200),
            artist_id     VARCHAR(20) SORTKEY,
            year          INTEGER,
            duration      FLOAT  
        ) DISTSTYLE ALL;
    """


def dimension_artist_create(table):
    return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            artist_id     VARCHAR(20) NOT NULL,
            name          VARCHAR(200) SORTKEY,
            location      VARCHAR(200),
            latitude      FLOAT,
            longitude     FLOAT
        ) DISTSTYLE ALL;
    """


def dimension_user_insert(table, raw_logs):
    return f"""
        INSERT INTO {table} (user_id, first_name, last_name, gender, level)
        WITH most_recent_status AS(
            SELECT *
            FROM {raw_logs}
            WHERE (userid, ts) IN
            (
                SELECT userid, MAX(ts) FROM {raw_logs}
                GROUP BY userid
            )
        )
        SELECT DISTINCT
            CAST(userid AS INTEGER) AS user_id, 
            firstname AS first_name, 
            lastname AS last_name, 
            gender, 
            level
        FROM most_recent_status
        WHERE user_id IS NOT NULL;
    """


def dimension_time_insert(table, raw_logs):
    return f"""
        INSERT INTO {table} (start_time, hour, day, week, month, year, weekday)
        WITH timestamps AS (
            SELECT 
                ts AS tsraw, 
                TIMESTAMP 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second' AS ts
            FROM {raw_logs}
        )
        SELECT DISTINCT
            tsraw AS start_time,
            EXTRACT(hour FROM ts) AS hour,
            EXTRACT(day FROM ts) AS day,
            EXTRACT(week FROM ts) AS week,
            EXTRACT(month FROM ts) AS month,
            EXTRACT(year FROM ts) AS year,
            EXTRACT(dow FROM ts) AS weekday
        FROM timestamps;
    """


def dimension_song_insert(table, raw_songs):
    return f"""
        INSERT INTO {table} (song_id, title, artist_id, year, duration)
        SELECT DISTINCT
            song_id,
            title,
            artist_id,
            CAST(year AS INTEGER) AS year,
            CAST(duration AS FLOAT) as duration
        FROM {raw_songs} 
        WHERE song_id is NOT NULL
    """


def dimension_artist_insert(table, raw_songs):
    return f"""
        INSERT INTO {table} (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT
            artist_id,
            artist_name AS name,
            artist_location AS location,
            CAST(artist_latitude AS FLOAT) AS latitude,
            CAST(artist_longitude AS FLOAT) AS longitude
        FROM {raw_songs}
        WHERE artist_id is NOT NULL
    """


def data_quality_check_unique(table, column):
    return f"""
        SELECT COUNT(DISTINCT {column}) AS uniq, COUNT({column}) AS total FROM {table};
    """
