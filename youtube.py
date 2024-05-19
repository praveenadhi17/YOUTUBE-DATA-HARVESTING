import googleapiclient.discovery
import mysql.connector
import pandas as pd
import datetime
import re

#api_key connection
api_service_name = "youtube"
api_version = "v3"
api_key="AIzaSyCiGXrzeIMva_-CsLOJHPUNo3tnUUjm84w"
youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=api_key)

#channel info

def channel_data(channel_ids):
    list_data=[]
    # for i in channel_ids:
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=channel_ids)
    response = request.execute()

    for item in response['items']:
        data = {
            'channel_id': item['id'],
            'channel_name': item['snippet']['title'],
            'channel_des': item['snippet']['description'],
            'channel_playlistid': item['contentDetails']['relatedPlaylists']['uploads'],
            'channel_videocount': item['statistics']['videoCount'],
            'channel_viewcount': item['statistics']['viewCount'],
            'channel_Subscribercount': item['statistics']['subscriberCount']
        }
    
    list_data.append(data)
    list_data_1 = pd.DataFrame(list_data)
    return list_data_1

#videos id
def get_video_ids(channel_ids):
    video_ids=[]
    response=youtube.channels().list(id=channel_ids,
                                    part='contentDetails').execute()
    Playlist_ids=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                        part='snippet',
                                        playlistId=Playlist_ids,
                                        maxResults=50,
                                        pageToken=next_page_token
                                        ).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])    
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break 
    # video_ids_1=pd.DataFrame(video_ids)
    return video_ids


#video info

def video_info(Video_ids):
    video_data=[]
    for i in Video_ids:
        request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=i)
        response=request.execute()

        for item in response['items']:
            data=dict(Channel_name=item['snippet']['channelTitle'],
                    Channel_id=item['snippet']['channelId'],
                    Video_id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnails=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_status=item['contentDetails']['caption'])
            video_data.append(data)

    video_data_1=pd.DataFrame(video_data)
    return video_data_1
# b=video_info(e)
# video_details_df=pd.DataFrame(b)

# Comment info
def comment_info(video_ids_df):
    Comment_data=[]
    try:
        for i in video_ids_df:
            request=youtube.commentThreads().list(
                    part='snippet',
                    videoId=i,
                    maxResults=100)
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_id=item['snippet']['topLevelComment']['id'],
                        Video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
    except:
        pass
    Comment_data_1=pd.DataFrame(Comment_data)
    return Comment_data_1

#playlists details

def playlist_details(channel_ids):

        next_page_token=None
        All_data=[]

        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_ids,
                        maxResults=50,pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlists_id=item['id'],
                                        Title=item['snippet']['title'],
                                        channel_id=item['snippet']['channelId'],
                                        channel_name=item['snippet']['channelTitle'],
                                        Publishat=item['snippet']['publishedAt'],
                                        video_count=item['contentDetails']['itemCount']
                                        )
                        All_data.append(data)
                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        All_data_1=pd.DataFrame(All_data)

        return All_data_1


# Storeing datas to mysql database
def channeldata_table(channel_data_df):
    mydb = mysql.connector.connect(
                host="localhost",
                user="root",
                password="",
                database='youtubedata',
    )

    mycursor = mydb.cursor(buffered=True)

    # Check if the table exists
    mycursor.execute("SHOW TABLES LIKE 'channeldatas'")
    table_exists = mycursor.fetchone()

    if not table_exists:
        # Create the table if it does not exist
        create_query = '''CREATE TABLE IF NOT EXISTS channeldatas (
                            channel_name VARCHAR(100),
                            channel_id VARCHAR(80) PRIMARY KEY,
                            channel_Subscribercount BIGINT,
                            channel_viewcount BIGINT,
                            channel_videocount INT,
                            channel_des TEXT,
                            channel_playlistid VARCHAR(80)
                         )'''
        mycursor.execute(create_query)
        mydb.commit()

    # Iterate over DataFrame rows and insert data into MySQL table
    for index, row in channel_data_df.iterrows():
        # Check if the channel data already exists in the table
        mycursor.execute("SELECT * FROM channeldatas WHERE channel_id = %s", (row['channel_id'],))
        existing_data = mycursor.fetchone()

        if existing_data:
            print(f"Channel data with ID {row['channel_id']} already exists. Skipping insertion.")
        else:
            # Insert new data into the table
            insert_query = '''INSERT INTO channeldatas 
                                (channel_name, channel_id, channel_Subscribercount, channel_viewcount, channel_videocount, channel_des, channel_playlistid) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s)'''
            values = (row['channel_name'], row['channel_id'], row['channel_Subscribercount'], row['channel_viewcount'], row['channel_videocount'], row['channel_des'], row['channel_playlistid'])
            mycursor.execute(insert_query, values)
            print(f"Inserted new channel data with ID {row['channel_id']}.")

    mydb.commit()
    mycursor.close()
    mydb.close()

    print('Channel data insertion done.')


def playlistdata_table(Playlist_details_df):
    mydb = mysql.connector.connect(
                host="localhost",
                user="root",
                password="",
                database='youtubedata'
    )

    mycursor = mydb.cursor(buffered=True)

    # Check if the table exists
    mycursor.execute("SHOW TABLES LIKE 'playlistdatas'")
    table_exists = mycursor.fetchone()

    if not table_exists:
        # Create the table if it does not exist
        create_query = '''CREATE TABLE IF NOT EXISTS playlistdatas (
                            Playlists_id VARCHAR(100) PRIMARY KEY,
                            Title VARCHAR(1000),
                            channel_id VARCHAR(1000),
                            channel_name VARCHAR(1000),
                            Publishat TIMESTAMP,
                            video_count INT
                         )'''
        mycursor.execute(create_query)
        mydb.commit()

    # Iterate over DataFrame rows and insert data into MySQL table
    for index, row in Playlist_details_df.iterrows():
        # Check if the playlist data already exists in the table
        mycursor.execute("SELECT * FROM playlistdatas WHERE Playlists_id = %s", (row['Playlists_id'],))
        existing_data = mycursor.fetchone()

        if existing_data:
            print(f"Playlist data with ID {row['Playlists_id']} already exists. Skipping insertion.")
        else:
            # Insert new data into the table
            insert_query = '''INSERT INTO playlistdatas
                                (Playlists_id, Title, channel_id, channel_name, Publishat, video_count) 
                                VALUES (%s, %s, %s, %s, %s, %s)'''
            values = (row['Playlists_id'], row['Title'], row['channel_id'], row['channel_name'], row['Publishat'], row['video_count'])
            mycursor.execute(insert_query, values)
            print(f"Inserted new playlist data with ID {row['Playlists_id']}.")

    mydb.commit()
    mycursor.close()
    mydb.close()
    print('Playlist_details_df data insertion done.')



def parse_duration(duration_str):
    # Extract hours, minutes, and seconds from the duration string using regular expressions
    hours_match = re.search(r'(\d+)H', duration_str)
    minutes_match = re.search(r'(\d+)M', duration_str)
    seconds_match = re.search(r'(\d+)S', duration_str)

    # Initialize variables for hours, minutes, and seconds
    hours = 0
    minutes = 0
    seconds = 0

    # Update variables if matches are found
    if hours_match:
        hours = int(hours_match.group(1))
    if minutes_match:
        minutes = int(minutes_match.group(1))
    if seconds_match:
        seconds = int(seconds_match.group(1))

    # Calculate total seconds
    total_seconds = hours * 3600 + minutes * 60 + seconds

    # Create a timedelta object representing the duration
    duration_timedelta = datetime.timedelta(seconds=total_seconds)

    # Format the duration as a string in HH:MM:SS format
    formatted_duration = str(duration_timedelta)

    return formatted_duration

def video_table(video_details_df):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database='youtubedata'
    )

    mycursor = mydb.cursor(buffered=True)

    # Check if the table exists
    mycursor.execute("SHOW TABLES LIKE 'videosdatas'")
    table_exists = mycursor.fetchone()

    if not table_exists:
        # Create the table if it does not exist
        create_query = '''CREATE TABLE IF NOT EXISTS videosdatas (
                            Video_id VARCHAR(50) PRIMARY KEY,
                            Channel_name VARCHAR(255),
                            Channel_id VARCHAR(50),
                            Title VARCHAR(255),
                            Tags TEXT,
                            Thumbnails TEXT,
                            Description TEXT,
                            Published_date DATETIME,
                            Duration TIME, -- Changed datatype to TIME
                            Views INT,
                            Likes INT,
                            Comments INT,
                            Favorite_count INT,
                            Definition VARCHAR(50),
                            Caption_status BOOLEAN
                        )'''
        mycursor.execute(create_query)
        mydb.commit()

    # Iterate over DataFrame rows and insert data into MySQL table
    for index, row in video_details_df.iterrows():
        # Check if the video data already exists in the table based on the video ID
        mycursor.execute("SELECT * FROM videosdatas WHERE Video_id = %s", (row['Video_id'],))
        existing_data = mycursor.fetchone()

        if existing_data:
            print(f"Video data with ID {row['Video_id']} already exists. Skipping insertion.")
        else:
            # Parse duration
            formatted_duration = parse_duration(row['Duration'])
            # formatted_duration_df = pd.DataFrame({'Duration': [formatted_duration]})
            # # Extracting the duration value from the Series
            # duration_value = formatted_duration_df['Duration'].iloc[0]

            # Insert new data into the table
            tags_str = ', '.join(row['Tags']) if isinstance(row['Tags'], list) else row['Tags']
            insert_query = """INSERT INTO videosdatas (
                                Channel_name, Channel_id, Video_id, Title, Tags, Thumbnails,
                                Description, Published_date, Duration, Views, Likes, Comments,
                                Favorite_count, Definition, Caption_status
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            values = (
                row['Channel_name'], row['Channel_id'], row['Video_id'], row['Title'],
                tags_str, row['Thumbnails'], row['Description'], row['Published_date'],
                formatted_duration, row['Views'], row['Likes'], row['Comments'],
                row['Favorite_count'], row['Definition'], row['Caption_status']
            )
            mycursor.execute(insert_query, values)
            print(f"Inserted new video data with ID {row['Video_id']}.")

    # Close cursor and connection
    mydb.commit()
    mycursor.close()
    mydb.close()
# def video_table(video_details_df):
#     mydb = mysql.connector.connect(
#         host="localhost",
#         user="root",
#         password="",
#         database='youtubedata'
#     )

#     mycursor = mydb.cursor(buffered=True)

#     # Check if the table exists
#     mycursor.execute("SHOW TABLES LIKE 'videosdatas'")
#     table_exists = mycursor.fetchone()

#     if not table_exists:
#         # Create the table if it does not exist
#         create_query = '''CREATE TABLE IF NOT EXISTS videosdatas (
#                             Video_id VARCHAR(50) PRIMARY KEY,
#                             Channel_name VARCHAR(255),
#                             Channel_id VARCHAR(50),
#                             Title VARCHAR(255),
#                             Tags TEXT,
#                             Thumbnails TEXT,
#                             Description TEXT,
#                             Published_date DATETIME,
#                             Duration TIME, -- Changed datatype to TIME
#                             Views INT,
#                             Likes INT,
#                             Comments INT,
#                             Favorite_count INT,
#                             Definition VARCHAR(50),
#                             Caption_status BOOLEAN
#                         )'''
#         mycursor.execute(create_query)
#         mydb.commit()

#     # Iterate over DataFrame rows and insert data into MySQL table
#     for index, row in video_details_df.iterrows():
#         # Check if the video data already exists in the table based on the video ID
#         mycursor.execute("SELECT * FROM videosdatas WHERE Video_id = %s", (row['Video_id'],))
#         existing_data = mycursor.fetchone()

#         if existing_data:
#             print(f"Video data with ID {row['Video_id']} already exists. Skipping insertion.")
#         else:
#             # Insert new data into the table
#             tags_str = ', '.join(row['Tags']) if isinstance(row['Tags'], list) else row['Tags']
#             insert_query = """INSERT INTO videosdatas (
#                                 Channel_name, Channel_id, Video_id, Title, Tags, Thumbnails,
#                                 Description, Published_date, Duration, Views, Likes, Comments,
#                                 Favorite_count, Definition, Caption_status
#                             ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
#             values = (
#                 row['Channel_name'], row['Channel_id'], row['Video_id'], row['Title'],
#                 tags_str, row['Thumbnails'], row['Description'], row['Published_date'],
#                 row['Duration'], row['Views'], row['Likes'], row['Comments'],
#                 row['Favorite_count'], row['Definition'], row['Caption_status']
#             )
#             mycursor.execute(insert_query, values)
#             print(f"Inserted new video data with ID {row['Video_id']}.")

#     mydb.commit()
#     mycursor.close()
#     mydb.close()

    print('video_details_df data insertion done.')

def commentdata_table(comment_details_df):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database='youtubedata'
    )

    mycursor = mydb.cursor(buffered=True)

    # Check if the table exists
    mycursor.execute("SHOW TABLES LIKE 'commentdatas'")
    table_exists = mycursor.fetchone()

    if not table_exists:
        # Create the table if it does not exist
        create_query = '''CREATE TABLE IF NOT EXISTS commentdatas(
                            Comment_id varchar(50),
                            Video_id varchar(50),
                            Comment_text text,
                            Comment_author varchar(250),
                            Comment_published timestamp,
                            PRIMARY KEY (Comment_id, Video_id)
                        )'''
        mycursor.execute(create_query)
        mydb.commit()

    # Iterate over DataFrame rows and insert data into MySQL table
    for index, row in comment_details_df.iterrows():
        # Check if the comment data already exists in the table based on the comment ID and video ID
        mycursor.execute("SELECT * FROM commentdatas WHERE Comment_id = %s AND Video_id = %s", (row['Comment_id'], row['Video_id']))
        existing_data = mycursor.fetchone()

        if existing_data:
            print(f"Comment data with ID {row['Comment_id']} and video ID {row['Video_id']} already exists. Skipping insertion.")
        else:
            # Insert new data into the table
            insert_query = '''INSERT INTO commentdatas 
                            (Comment_id, Video_id, Comment_text, Comment_author, Comment_published) 
                            VALUES (%s, %s, %s, %s, %s)'''
            values = (row['Comment_id'], row['Video_id'], row['Comment_text'], row['Comment_author'], row['Comment_published'])
            mycursor.execute(insert_query, values)
            print(f"Inserted new comment data with ID {row['Comment_id']} for video ID {row['Video_id']}.")

    mydb.commit()
    mycursor.close()
    mydb.close()
    print('comment_details_df data insertion done')




def channel_details(channel_id):
    channel_ids =channel_id
    list_data=channel_data(channel_ids)
    channel_data_df = list_data.copy()
    channeldata_table(channel_data_df)
    
    pl_details=playlist_details(channel_ids)
    Playlist_details_df = pl_details.copy()
    playlistdata_table(Playlist_details_df)

    vi_ids=get_video_ids(channel_ids)
    # Video_ids_df=pd.DataFrame(vi_ids)

    vi_details=video_info(vi_ids)
    video_details_df = vi_details.copy()
    video_table(video_details_df)

    com_details=comment_info(vi_ids)
    comment_details_df = com_details.copy()
    commentdata_table(comment_details_df)

# channel_id = 'UCTCMjShTpZg96cXloCO9q1w'
# channel_details(channel_id)