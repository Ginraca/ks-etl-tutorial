import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

def validate(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No Songs Downloaded")
        return False

    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("There are duplicate Primary Keys")

    if df.isnull().values.any():
        raise Exception("There are one or more null data in the Data")
    
    # yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    # yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    # timestamps = df["timestamp"].tolist()
    # for timestamp in timestamps:
    #     if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
    #         raise Exception("There are Data that are more than 1 Day old")

    return True    

def spotify_etl():
    database_location = "sqlite:///my_played_tracks.sqlite"
    user_id = "ginraca"
    token = "BQBgjy57z3G8uvsNxtBmZFbc3u-KUK0c7TAjcDr2cfFY130mH5Y55iqgPK3_DGb7TSV2FkTMIzonmsn_k-rR2mPaCXXnkeWRDHuQgFS1J8TC_9J5NTe1sKBiMjAefIG9SBoI7GG6SPrl1deN4EQI6IRUYLEAkZfEX5abtSSHLtdf"

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=token)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix = int(yesterday.timestamp()) * 1000

    # Extract
    req = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix), headers=headers)

    data = req.json()

    # Create JSON File
    json_obj = json.dumps(data, indent=4)
    with open("spotify_result.json", "w") as outfile:
        outfile.write(json_obj)


    # Transform
    song_names = []
    artist_names = []
    played_at_lists = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["artists"][0]["name"])
        played_at_lists.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
    
    songs_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_lists,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(songs_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])

    if validate(song_df):
        print("Data is Validated, Please proceed")

    # Load
    db_engine = sqlalchemy.create_engine(database_location)
    connection = sqlite3.connect('my_played_tracks.sqlite')
    cursor = connection.cursor()

    sql_query = """
        CREATE TABLE if not exists my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
    """

    cursor.execute(sql_query)
    print("Database has been successfully opened")

    
    try:
        song_df.to_sql("my_played_tracks", db_engine, index=False, if_exists="append")
    except:
        print("Data already exists in the table")
    
    connection.close()
    print("Database has successfully been closed")