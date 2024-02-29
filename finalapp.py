#!/usr/bin/env python
# coding: utf-8

# In[1]:


#imports

import pprint
from googleapiclient.discovery import build
from pymongo import MongoClient
import psycopg2
import pandas as pd
import streamlit as st

#passing parameters to build() function #retriving response - API connection
Api_Key = "AIzaSyArQYRLKd6uddxsFRTx9y3CSXW5soxxTvo"
Service_Name = "youtube"
Api_Version = "v3"
yt_youtube = build(Service_Name,Api_Version,developerKey = Api_Key)

#getting channel info
def get_channel_info(channel_id):
    channels_data = yt_youtube.channels().list(part='snippet,contentDetails,statistics', id=channel_id)
    res_data = channels_data.execute()

    #within items accessing the necessary channel details
    for i in res_data['items']:
        full_channel_data = dict(Channel_Name = i['snippet']['title'],
                                 Channel_Id =i['id'],
                                 Subscriber_Count = i['statistics']['subscriberCount'],
                                 Views_Count = i['statistics']['viewCount'],
                                 Total_videos = i['statistics']['videoCount'],
                                 Channel_Description = i['snippet']['description'],
                                 Playlist_id = i['contentDetails']['relatedPlaylists']['uploads'])
    return full_channel_data

#get list of video IDs - passing the playlist ID from Channel data obtained
def get_videos_ids(videoId):
    video_ids = []
    next_Page_Token =  None
    while True:
        videos_data = yt_youtube.playlistItems().list(part='contentDetails',playlistId=videoId,maxResults = 50,pageToken = next_Page_Token).execute()
        for i in range(len(videos_data['items'])):
            video_ids.append(videos_data['items'][i]['contentDetails']['videoId'])
        next_Page_Token = videos_data.get('nextPageToken')
        if(next_Page_Token is None):
            break
    return video_ids

#getting the video details using the video ID
def get_all_videos_details(video_id_list):
    video_data = []
    for id_video in video_id_list:
        res = yt_youtube.videos().list(part = 'snippet,contentDetails,statistics',id = id_video)
        res_data = res.execute()

        for item in res_data['items']:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet'].get('description'),
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics'].get('viewCount'),
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Fav_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_active = item['contentDetails']['caption'])
            video_data.append(data)
    return video_data

#get details of comments : 
def get_comment_details(video_id_list):
    comment_section = []
    try:
        for id_video in video_id_list:
            res = yt_youtube.commentThreads().list(part = 'snippet',videoId=id_video,maxResults = 50)
            res_data = res.execute()

            for item in res_data['items']:
                data = dict(
                            Comment_Id = item['snippet']['topLevelComment']['id'],
                            Video_Id = item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published_Date = item['snippet']['topLevelComment']['snippet']['publishedAt']
                            )
                comment_section.append(data)
    except:
        pass
    return comment_section

#bridging connection to Mongo compass
client = MongoClient('mongodb://localhost:27017/')
#create a database
database = client['Youtube_Data']

#creating collections with the functions made above of the details of channel needed
def data_collections(channel_id):
    ch_info = get_channel_info(channel_id)
    vi_info = get_videos_ids(ch_info['Playlist_id'])
    all_vid = get_all_videos_details(vi_info)
    com_info = get_comment_details(vi_info)
    #inserting data
    collection1 = database['Channel_Details']
    collection1.insert_one({'Channel_Details' : ch_info,
                            'Comment_Details' : com_info, 
                            'Video_Details': all_vid})
    return "Data has been uploaded successfully."
    
def migrate_data_to_sql(channel_id):
    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    database = client['Youtube_Data']
    coll1 = database['Channel_Details']
    
    # Find channel data in MongoDB
    channel_data = coll1.find_one({'Channel_Details.Channel_Id': channel_id})

    # If channel data found, migrate it to SQL
    if channel_data:
        # Connect to SQL database
        db = psycopg2.connect(host='localhost', user='postgres', password='password', database='Youtube_Data', port='5432')
        cursor = db.cursor()
        
        # Check if tables exist and create them if not
        create_tables_in_sql(cursor)
        
        # Insert channel data into SQL
        insert_channel_data(cursor, channel_data)
        
        db.commit()
        db.close()
        
        return st.success(f"Data for Channel ID '{channel_id}' migrated to SQL successfully.")
    
    else:
        return st.error(f"No data found for Channel ID '{channel_id}' in MongoDB.")
    
    
def create_tables_in_sql(cursor):
    # Create channels table if not exists
    cursor.execute('''CREATE TABLE IF NOT EXISTS channels (
        Channel_Id VARCHAR(80) PRIMARY KEY,
        Channel_Name VARCHAR(100),
        Channel_Description TEXT,
        Views_Count BIGINT,
        Total_videos BIGINT,
        Subscriber_Count BIGINT,
        Playlist_id VARCHAR(80))''')
    # Create videos table if not exists
    cursor.execute('''CREATE TABLE IF NOT EXISTS videos (Channel_Name varchar(255),
                                                        Channel_Id varchar(255),
                                                        Video_Id varchar(255) primary key,
                                                        Title varchar(255),
                                                        Tags text,
                                                        Thumbnail varchar(255),
                                                        Description text,
                                                        Published_Date timestamp,
                                                        Duration interval,
                                                        Views bigint,
                                                        Likes bigint,
                                                        Comments bigint,
                                                        Fav_Count int,
                                                        Definition varchar(50),
                                                        Caption_active varchar(50))''')
    # Create comments table if not exists
    cursor.execute('''CREATE TABLE IF NOT EXISTS comments (Comment_Id varchar(255) primary key,
                                                            Video_Id varchar(255),
                                                            Comment_Text text,
                                                            Comment_Author varchar(255),
                                                            Comment_Published_Date timestamp)''')
    
def insert_channel_data(cursor, channel_data):
    # Extract channel details
    channel_details = channel_data['Channel_Details']
    video_details = channel_data['Video_Details']
    comment_details = channel_data['Comment_Details']
    
    
    # Insert channel data into SQL
    cursor.execute('''INSERT INTO channels (Channel_Id, 
                                            Channel_Name,
                                            Channel_Description,
                                            Views_Count,
                                            Total_videos,
                                            Subscriber_Count,
                                            Playlist_id) VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                   (channel_details['Channel_Id'], 
                    channel_details['Channel_Name'], 
                    channel_details['Channel_Description'],
                    channel_details['Views_Count'], 
                    channel_details['Total_videos'], 
                    channel_details['Subscriber_Count'], 
                    channel_details['Playlist_id']))
    #Insert video data into SQL
    df1 = pd.DataFrame(video_details)
    for index,rows in df1.iterrows():
            insert_query = '''insert into videos(Channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
                                                    Title,
                                                    Tags,
                                                    Thumbnail,
                                                    Description,
                                                    Published_Date,
                                                    Duration,
                                                    Views,
                                                    Likes,
                                                    Comments,
                                                    Fav_Count,
                                                    Definition,
                                                    Caption_active)

                                                 values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            values=(rows['Channel_Name'],
                    rows['Channel_Id'],
                    rows['Video_Id'],
                   rows['Title'],
                   rows['Tags'],
                   rows['Thumbnail'],
                   rows['Description'],
                   rows['Published_Date'],
                   rows['Duration'],
                   rows['Views'],
                   rows['Likes'],
                   rows['Comments'],
                   rows['Fav_Count'],
                   rows['Definition'],
                   rows['Caption_active'])
            cursor.execute(insert_query,values)
     #Insert comments data into SQL
    df2 = pd.DataFrame(comment_details)
    for index,rows in df2.iterrows():
            insert_query = '''insert into comments(Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published_Date)

                                                values(%s,%s,%s,%s,%s)'''
            values=(
                   rows['Comment_Id'],
                   rows['Video_Id'],
                   rows['Comment_text'],
                   rows['Comment_author'],
                   rows['Comment_Published_Date'])
            cursor.execute(insert_query,values)
            
def fetch_channels_data_from_postgresql():
    conn = psycopg2.connect(host='localhost', user='postgres', password='password', database='Youtube_Data', port='5432')
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM channels")
    channels_data = cursor.fetchall()
    channels_columns = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    return channels_data, channels_columns

def fetch_videos_data_from_postgresql():
    conn = psycopg2.connect(host='localhost', user='postgres', password='password', database='Youtube_Data', port='5432')
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM videos')
    videos_data = cursor.fetchall()
    videos_columns = [desc[0] for desc in cursor.description]
    videos_data = [[str(value) if column != 'Duration' else str(value)[:-3] for column, value in zip(videos_columns, row)] for row in videos_data]
    cursor.close()
    conn.close()

    return videos_data, videos_columns

def fetch_comments_data_from_postgresql():
    conn = psycopg2.connect(host='localhost', user='postgres', password='password', database='Youtube_Data', port='5432')
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM comments")
    comments_data = cursor.fetchall()
    comments_columns = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    return comments_data, comments_columns

def view_channels_table():
    channels_data, channels_columns = fetch_channels_data_from_postgresql()
    channels_df = pd.DataFrame(channels_data, columns=channels_columns)
    st.dataframe(channels_df)

# Function to display videos table for a selected channel
def view_videos_table():
    videos_data, videos_columns = fetch_videos_data_from_postgresql()
    videos_df = pd.DataFrame(videos_data, columns=videos_columns)
    st.dataframe(videos_df)

# Function to display comments table
def view_comments_table():
    comments_data, comments_columns = fetch_comments_data_from_postgresql()
    comments_df = pd.DataFrame(comments_data, columns=comments_columns)
    st.dataframe(comments_df)

            

st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")
mt1, mt2, mt3 = st.tabs(["ADD","VIEW","QUERY"])



with mt1:
    channel_id = st.text_input("Enter the Channel id")
    channels = channel_id.split(',')
    channels = [ch.strip() for ch in channels if ch]
    if st.button('Add'):
        for channel in channels:
            channel_ids = []
            #bridging connection to Mongo compass
            client = MongoClient('mongodb://localhost:27017/')
            #create a database
            database = client['Youtube_Data']
            coll1 = database['Channel_Details']
            for ch_data in coll1.find({},{"Channel_Details":1}):
                channel_ids.append(ch_data["Channel_Details"]["Channel_Id"])
            if channel in channel_ids:
                st.error('Please enter another Channel ID as this ID- ' + channel + ' already exists.')
            else:
                output = data_collections(channel)
                st.success(output)
                
    channel_toSQL_id = st.text_input("Enter the Channel id to migrate to SQL")
    if st.button("Migrate to SQL"):
        channel_ids = channel_toSQL_id.split(',')
        for channel_id in channel_ids:
            result = migrate_data_to_sql(channel_id)
                
with mt2:           
    on = st.checkbox('Show Tables')

    if(on):
        tab1, tab2, tab3 = st.tabs(["Channel Details", "Videos", "Comments"])
        with tab1:
            view_channels_table()
            
        with tab2:
            view_videos_table()
            
        with tab3:
            view_comments_table()
            
with mt3:
    db = psycopg2.connect(host='localhost',
                        user = 'postgres',
                        password = 'password',
                        database='Youtube_Data',
                        port = '5432')
    cursor = db.cursor()

    question = st.selectbox(
        'Please Select Your Question',
        ('1. What are the names of all the videos and their corresponding channels?',
         '2. Which channels have the most number of videos, and how many videos do they have?',
         '3. What are the top 10 most viewed videos and their respective channels?',
         '4. How many comments were made on each video, and what are their corresponding video names?',
         '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
         '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
         '7. What is the total number of views for each channel, and what are their corresponding channel names?',
         '8. What are the names of all the channels that have published videos in the year 2022?',
         '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
         '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))
    if question == '1. What are the names of all the videos and their corresponding channels?':
        q1 = 'select Channel_Name as "Channel",Title as "Video Name" from videos'
        cursor.execute(q1)
        db.commit()
        t1=cursor.fetchall()
        st.write(pd.DataFrame(t1, columns=["Channel","Video Name"]))
    elif question == '2. Which channels have the most number of videos, and how many videos do they have?':
        q2 = 'select Channel_Name as "Channel",count(Title) as "Video Count" from videos GROUP BY "Channel" ORDER BY "Video Count" desc      LIMIT 1'
        cursor.execute(q2)
        db.commit()
        t2=cursor.fetchall()
        st.dataframe(pd.DataFrame(t2, columns=["Channel","Video Count"]))
    elif question == '3. What are the top 10 most viewed videos and their respective channels?':
        q3 = 'select Channel_Name as "Channel",Title as "Video Name",Views from videos ORDER BY Views Desc LIMIT 10'
        cursor.execute(q3)
        db.commit()
        t3=cursor.fetchall()
        st.write(pd.DataFrame(t3, columns=["Channel","Video Name","Views"]))
    elif question == '4. How many comments were made on each video, and what are their corresponding video names?':
        q4 = 'select Title as "Video Name",CAST(Comments as INTEGER) as "Com_Count" from videos'
        cursor.execute(q4)
        db.commit()
        t4=cursor.fetchall()
        st.write(pd.DataFrame(t4, columns=["Video Name","Com_Count"]))
    elif question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        q5 = 'select Channel_Name as "Channel",Title as "Video Name",Likes from videos where Likes is not null Order by Likes desc LIMIT 1'
        cursor.execute(q5)
        db.commit()
        t5=cursor.fetchall()
        st.write(pd.DataFrame(t5, columns=["Channel","Video Name","Likes"]))
    elif question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        q6 = 'select Title as "Video Name",Likes from videos'
        cursor.execute(q6)
        db.commit()
        t6=cursor.fetchall()
        st.write(pd.DataFrame(t6, columns=["Video Name","Likes"]))
    elif question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        q7 = 'select Channel_Name as "Channel",Sum(Views) as Views from videos Group by Channel_Name'
        cursor.execute(q7)
        db.commit()
        t7=cursor.fetchall()
        st.write(pd.DataFrame(t7, columns=["Channel","Views"]))
    elif question == '8. What are the names of all the channels that have published videos in the year 2022?':
        q8 = 'select Channel_Name as "Channel" from videos where EXTRACT(YEAR from Published_Date)=2022 group by Channel_Name'
        cursor.execute(q8)
        db.commit()
        t8=cursor.fetchall()
        st.write(pd.DataFrame(t8, columns=["Channel"]))
    elif question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        q9 = 'select Channel_Name as "Channel", AVG(EXTRACT(EPOCH FROM Duration)) as Duration_Avg from videos group by Channel_Name'
        cursor.execute(q9)
        db.commit()
        t9=cursor.fetchall()
        st.write(pd.DataFrame(t9, columns=["Channel","AVG_Duration"]))
    elif question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        q10 = 'select Channel_Name as "Channel",Title as "Video Name", Comments from videos where Comments is not null order by Comments desc limit 1'
        cursor.execute(q10)
        db.commit()
        t10=cursor.fetchall()
        st.write(pd.DataFrame(t10, columns=["Channel","Video Name","Comments"]))



