import mysql.connector
import pandas as pd
import streamlit as st
from youtube import channel_details

# SQL Connection Parameters
sql_config = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "youtubedata"
}

channel_id=st.text_input("Enter the channel ID")


# Function to check if channel ID exists in the database
def channel_id_exists(channel_id):
    mydb = mysql.connector.connect(**sql_config)
    cursor = mydb.cursor()
    cursor.execute(f'SELECT * FROM channeldatas WHERE channel_id = "{channel_id}"')
    result = cursor.fetchone()
    cursor.close()
    mydb.close()
    return result is not None

# Function to fetch and display channel data
def show_channels_table():
    mydb = mysql.connector.connect(**sql_config)
    mycursor = mydb.cursor()

    query = "SELECT * FROM channeldatas"
    mycursor.execute(query)
    result = mycursor.fetchall()

    df = pd.DataFrame(result, columns=[ "channel_name","channel_id", "channel_Subscribercount", "channel_viewcount", "channel_videocount","channel_des","channel_playlistid"])
    st.write(df)

    mycursor.close()
    mydb.close()

# Function to fetch and display playlist data
def show_playlists_table(selected_channel):
    mydb = mysql.connector.connect(**sql_config)
    mycursor = mydb.cursor()

    query = f'SELECT * FROM playlistdatas WHERE channel_name = "{selected_channel}"'
    mycursor.execute(query)
    result = mycursor.fetchall()

    df = pd.DataFrame(result, columns=["Playlists_id", "Title", "channel_id", "channel_name", "Publishat", "video_count"])
    st.write(df)

    mycursor.close()
    mydb.close()

# Function to fetch and display video data
def show_videos_table(selected_channel_1):
    # Print the SQL query
    query = f'SELECT * FROM videosdatas WHERE Channel_name = "{selected_channel_1}"'
    print("SQL Query:", query)
    
    mydb = mysql.connector.connect(**sql_config)
    mycursor = mydb.cursor()

    mycursor.execute(query)
    result = mycursor.fetchall()

    df = pd.DataFrame(result, columns=["Video_id", "Channel_name", "Channel_id", "Title", "Tags", "Thumbnails", "Description", "Published_date",
                                        "Duration", "Views", "Likes", "Comments", "Favorite_count", "Definition", "Caption_status"])
    st.write(df)

    mycursor.close()
    mydb.close()

# Function to fetch and display comment data
def show_comments_table():
    mydb = mysql.connector.connect(**sql_config)
    mycursor = mydb.cursor()

    query = "SELECT * FROM commentdatas"
    mycursor.execute(query)
    result = mycursor.fetchall()

    df = pd.DataFrame(result, columns=["Comment_id", "Video_id", "Comment_text", "Comment_author", "Comment_published"])
    st.write(df)

    mycursor.close()
    mydb.close()


# Function to fetch data for a new channel ID
def fetch_channel_data(channel_id):
    # st.write("Fetching data for the new channel ID...")
    with st.spinner("Loading..."):
        channel_details(channel_id)
        # if channel_data_result is not None:
        #     # Now you can display or use this data as needed
        #     st.write("Data fetched and stored successfully.")
        # else:
        #     st.write("Failed to fetch data for the provided channel ID.")

if st.button("Submit"):
    if channel_id_exists(channel_id):
        st.write("Channel ID already exists in the database.")
    else:
        st.write("Fetching data for the new channel ID...")
        # Fetch data and store in the database
        fetch_channel_data(channel_id)
        # Now you can display or use this data as needed
        st.write("Data fetched and stored successfully.")

# Streamlit UI
def main():
    st.title("YouTube Data Analysis")
    st.sidebar.title(":red[YOUTUBEDATAHAVERSTING AND WAREHOUSING]")

    page = st.sidebar.radio("Go to",["Channels", "Playlists", "Videos", "Comments"])

    if page == "Channels":
        st.header("Channels Table")
        show_channels_table()
    elif page == "Playlists":
        st.header("Playlists Table")
        selected_channel = st.selectbox("Select Channel", get_channel_names(page))
        show_playlists_table(selected_channel)
    elif page == "Videos":
        st.header("Videos Table")
        selected_channel_1 = st.selectbox("Select Channel", get_channel_names(page))
        show_videos_table(selected_channel_1)
    elif page == "Comments":
        st.header("Comments Table")
        show_comments_table()

# Function to get all unique channel names
def get_channel_names(page):
    mydb = mysql.connector.connect(**sql_config)
    mycursor = mydb.cursor()
    if page == "Playlists":
        query = "SELECT DISTINCT channel_name FROM playlistdatas"
    elif page == "Videos":
        query = "SELECT DISTINCT Channel_name FROM videosdatas"
    mycursor.execute(query)
    result = mycursor.fetchall()
    channel_names = [row[0] for row in result]
    print('------------------',channel_names)
    mycursor.close()
    mydb.close()

    return channel_names



if __name__ == "__main__":
    main()

mydb = mysql.connector.connect(**sql_config)
cursor = mydb.cursor(buffered=True)

def execute_query(query):
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result)
    return df

def main_1():
    question = st.selectbox("Select your question", [
        "1. What are the names of all the videos and their corresponding channels?",
        "2. Which channels have the most number of videos, and how many videos do they have?",
        "3. What are the top 10 most viewed videos and their respective channels?",
        "4. How many comments were made on each video, and what are their corresponding video names?",
        "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
        "6. What is the total number of likes for each video and what are their corresponding video names?",
        "7. What is the total number of views for each channel and what are their corresponding channel names?",
        "8. What are the names of all the channels that have published videos in the year 2022",
        "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "10. Which videos have the highest number of comments, and what are their corresponding channel names"
    ])

    # if question.startswith("1."):
    #     df = execute_query("SELECT title, channel_name FROM videosdatas")
    #     st.write(df, header=["Video Title", "Channel Name"])
    #     # st.table(df, header=["Video Title", "Channel Name"], index=False
    if question.startswith("1."):
        df = execute_query("SELECT title AS 'Video Title', channel_name AS 'Channel Name' FROM videosdatas")
        df.columns = ["Video Title", "Channel Name"]
        st.table(df)
    elif question.startswith("2."):
        df = execute_query("SELECT channel_name, COUNT(*) AS number_of_videos FROM videosdatas GROUP BY channel_name ORDER BY number_of_videos DESC")
        df.columns = ["Channel Name", "Number of Videos"]
        st.table(df)
    elif question.startswith("3."):
        df = execute_query("SELECT title, channel_name, views FROM videosdatas ORDER BY views DESC LIMIT 10")
        df.columns = ["Video Title", "Channel Name", "Views"]
        st.table(df)
    elif question.startswith("4."):
        df = execute_query("SELECT comments AS no_comments, title AS videotitle FROM videosdatas WHERE comments IS NOT NULL")
        df.columns = ["Video Title", "Number of Comments"]
        st.table(df)
    elif question.startswith("5."):
        df = execute_query("SELECT title, channel_name, likes FROM videosdatas ORDER BY likes DESC")
        df.columns = ["Video Title", "Channel Name", "Likes"]
        st.table(df)
    elif question.startswith("6."):
        df = execute_query("SELECT title, likes FROM videosdatas")
        df.columns = ["Video Title", "Likes"]
        st.table(df)
    elif question.startswith("7."):
        df = execute_query("SELECT channel_name, SUM(views) AS total_views FROM videosdatas GROUP BY channel_name")
        df.columns = ["Channel Name", "Total Views"]
        st.table(df)
    elif question.startswith("8."):
        df = execute_query("SELECT DISTINCT channel_name FROM videosdatas WHERE YEAR(published_date) = 2022")
        df.columns = ["Channel Name"]
        st.table(df)
    # elif question.startswith("9."):
    #     df = execute_query("SELECT channel_name, AVG(duration) AS average_duration FROM videosdatas GROUP BY channel_name")
    #     st.write(df, header=["Channel Name", "Average Duration"])

    elif question.startswith("9."):
        df = execute_query("SELECT channel_name, AVG(duration) AS average_duration FROM videosdatas GROUP BY channel_name")
        df.columns = ["Channel Name", "Average Duration (Seconds)"]

        # Convert the 'average_duration' column to numeric type
        df['Average Duration (Seconds)'] = pd.to_numeric(df['Average Duration (Seconds)'])

        # Convert average duration from seconds to timedelta
        df['Average Duration (HH:MM:SS)'] = pd.to_timedelta(df['Average Duration (Seconds)'], unit='s')

        # Format timedelta to HH:MM:SS
        df['Average Duration (HH:MM:SS)'] = df['Average Duration (HH:MM:SS)'].apply(lambda x: '{:02d}:{:02d}:{:02d}'.format(x.seconds // 3600, (x.seconds // 60) % 60, x.seconds % 60))
        # Drop the 'Average Duration (Seconds)' column if it's no longer needed
        df.drop(columns=['Average Duration (Seconds)'], inplace=True)

        st.table(df)

    # elif question.startswith("10."):
    #     df = execute_query("SELECT video_title, channel_name, COUNT(*) AS number_of_comments FROM commentdatas GROUP BY video_title ORDER BY number_of_comments DESC LIMIT 10")
    #     st.write(df, header=["Video Title", "Channel Name", "Number of Comments"])

    elif question.startswith("10."):
        df = execute_query('select Title as videotitle, Channel_name as channelname,Comments as Comments from videosdatas where Comments is not null order by Comments desc')
        # 'select Title as videotitle, Channel_name as channelname,Comments as Comments from videosdatas where Comments is not null order by Comments desc'
        df.columns = ["Video ID", "Channel Name", "Number of Comments"]
        st.table(df)


if __name__ == "__main__":
    main_1()
