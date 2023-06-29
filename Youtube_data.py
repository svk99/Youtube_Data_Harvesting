import mysql.connector as sql
import pandas as pd
import streamlit as st
import pymongo
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('TkAgg')


myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient['youtube']

mydb=sql.connect(host="localhost",
                   user="root",
                   password="****",
                    database="youtube")

mycursor=mydb.cursor(buffered=True)
from googleapiclient.discovery import build

api_key='**** Valid API key ****'
youtube = build('youtube', 'v3', developerKey=api_key)


# function to get channel details
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(
        id=channel_id,
        part='snippet,statistics,contentDetails').execute()

    if 'items' not in response:
        st.write(f"Invalid channel id: {channel_id}")

    for i in range(len(response['items'])):
        data = dict(ch_id=channel_id,
                    ch_name=response['items'][i]['snippet']['title'],
                    playlist_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    subscribers=response['items'][i]['statistics']['subscriberCount'],
                    views=response['items'][i]['statistics']['viewCount'],
                    description=response['items'][i]['snippet']['description'],
                    total_videos=response['items'][i]['statistics']['videoCount']
                    )
        ch_data.append(data)
    return ch_data


# function to get video ids
def get_channel_video(channel_id):
    video_ids = []
    response = youtube.channels().list(
        id=channel_id,
        part='contentDetails').execute()

    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        response = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist_id,
            maxResults=2,
            pageToken=next_page_token).execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
            if len(video_ids)>1:
                   break
        next_page_token = response.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


# function to get video details
def get_video_details(vid_id):
    video_det = []

    for v_id in vid_id:
        response = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=v_id).execute()

        for v in response['items']:
            v_details = dict(
                v_id=v['id'],
                name=v['snippet']['title'],
                desc=v['snippet']['description'],
                publish=v['snippet']['publishedAt'],
                views=v['statistics']['viewCount'],
                like=v['statistics'].get('likeCount'),
                favorite=v['statistics'].get('favoriteCount'),
                comment=v['statistics'].get('commentCount'),
                duration=v['contentDetails']['duration'],
                thumbail=v['snippet']['thumbnails']['default']['url'],
                caption_status=v['contentDetails']['caption'],
                ch_name=v['snippet']['channelTitle']
            )
            video_det.append(v_details)
    return video_det

# function to get comments
def get_comments(vid_id):
    comment_details = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(
                part='snippet',
                videoId=vid_id,
                maxResults=2,
                pageToken=next_page_token).execute()

            for c in response['items']:
                comment = dict(
                    comment_id=c['id'],
                    text=c['snippet']['topLevelComment']['snippet']['textDisplay'],
                    author=c['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    published=c['snippet']['topLevelComment']['snippet']['publishedAt'],
                    video_id=c['snippet']['videoId']
                )
                comment_details.append(comment)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_details


def channel_name():
    ch_nm = []
    for i in db.channel_details.find():
        ch_nm.append(i['ch_name'])
    return ch_nm


col1, col2, col3 = st.columns([4,1,4],gap='small')

with col1:
    st.header('Data collection')
    c_id = st.text_input('Enter a Youtube channels id:')
    if st.button('Search'):
        ch_details = get_channel_details(c_id)
        z=ch_details[0]['ch_name']
        st.write('The channel name is '+z)
    if st.button('Upload to Mongo DB'):
        ch_details = get_channel_details(c_id)
        video_id = get_channel_video(c_id)
        video_details = get_video_details(video_id)

        def comments():
            comment_details = []
            for i in video_id:
                comment_details += get_comments(i)
            return comment_details

        cmt_details = comments()

        collections1 = db.channel_details
        collections1.insert_many(ch_details)

        collections2 = db.video_details
        collections2.insert_many(video_details)

        collections3 = db.comment_details
        collections3.insert_many(cmt_details)

        st.success("Upload to MogoDB successful...")

with col2:
    pass

with col3:
   st.header('Data migration and Analysis')
   ch_name=channel_name()
   selected_channel=st.selectbox('Select channel name:',options=ch_name)


   def insert_into_channels():
       collection = db.channel_details
       query = """INSERT INTO channels VALUES(%s,%s,%s,%s,%s,%s,%s)"""

       for i in collection.find({"ch_name": selected_channel}, {'_id': 0}):
           mycursor.execute(query, tuple(i.values()))
           mydb.commit()


   def insert_into_videos():
       collection = db.video_details
       query = """INSERT INTO videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

       for i in collection.find({"ch_name": selected_channel}, {"_id": 0}):
           v_desc = i.get("desc", "")
           short_desc = v_desc[:50]
           i["desc"] = short_desc
           t = tuple(i.values())
           mycursor.execute(query, t)
           mydb.commit()


   def insert_into_comments():
       collection1 = db.video_details
       collection2 = db.comments_details
       query = """INSERT INTO comments VALUES(%s,%s,%s,%s,%s)"""

       for vid in collection1.find({"ch_name": selected_channel}, {'_id': 0}):
           for i in collection2.find({'v_id': vid['v_id']}, {'_id': 0}):
               t = tuple(i.values())
               mycursor.execute(query, t)
               mydb.commit()

   if st.button('Submit'):
       try:
           insert_into_channels()
           insert_into_videos()
           insert_into_comments()
           st.success("Transfer to MySQL success")
       except:
           st.error("Error transferring to MySQL")

   st.write('Select the question:')
   question=st.selectbox('Questions',
               ['Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'
               ])

   if question=='1. What are the names of all the videos and their corresponding channels?':
       mycursor.execute("""SELECT video_name as Video_title,channel_name as Channel_Name FROM videos ORDER BY channel_name""")
       df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
       st.write(df)

   elif question=='2. Which channels have the most number of videos, and how many videos do they have?':
       mycursor.execute("""SELECT channel_name AS Channel_Name,total_videos AS Total_Videos
       FROM channels ORDER BY total_videos DESC""")
       df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
       st.write(df)
       sns.barplot(data=df,x="Channel_Name",y="Total_Videos",width=0.5)
       plt.show()

   elif question=='3. What are the top 10 most viewed videos and their respective channels?':
       mycursor.execute("""SELECT channel_name AS Channel_Name,video_name AS Video_Title,
       v_views AS Views FROM videos ORDER BY v_views DESC LIMIT 10""")
       df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
       st.write(df)
       sns.barplot(data=df, x="Channel_Name", y="Views", width=0.5)
       plt.show()

   elif question=='4. How many comments were made on each video, and what are their corresponding video names?':
       mycursor.execute("""SELECT a.video_id AS Video_Id,a.video_name AS Video_Title,
       b.Total_Comments FROM videos AS a LEFT JOIN (SELECT video_id,COUNT(comment_id) AS
       Total_Comments FROM comments GROUP BY video_id) AS b ON
       a.video_id=b.video_id ORDER BY b.Total_Comments DESC""")
       df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
       st.write(df)

   elif question=='5. Which videos have the highest number of likes, and what are their corresponding channel names?':
       mycursor.execute("""SELECT channel_name AS Channel_Name,video_name AS Title,v_likes AS Likes_Count
       FROM videos ORDER BY v_likes DESC LIMIT 10""")
       df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
       st.write(df)
       sns.barplot(data=df, x="Channel_Name", y="Likes_Count", width=0.5)
       plt.show()

   elif question=='6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
       mycursor.execute("""SELECT video_name AS Video_Title,v_likes AS Likes_Count
       FROM videos ORDER BY v_likes DESC""")
       df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
       st.write(df)

   elif question=='7. What is the total number of views for each channel, and what are their corresponding channel names?':
       mycursor.execute("""SELECT channel_name AS Channel_Name,views AS Views
       FROM channels ORDER BY views DESC""")
       df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
       st.write(df)
       sns.barplot(data=df, x="Channel_Name", y="Views", width=0.5)
       plt.show()

   elif question=='8. What are the names of all the channels that have published videos in the year 2022?':
       mycursor.execute("""SELECT channel_name AS Channel_Name FROM
       videos WHERE v_publish LIKE '%2022%'
       GROUP BY channel_name ORDER BY channel_name""")
       df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
       st.write(df)

   elif question=='9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
       mycursor.execute("""SELECT channel_name AS Channel_Name,AVG(v_duration)/60 AS
       "Average_Video_Duration(mins)" FROM videos GROUP BY channel_name
       ORDER BY AVG(v_duration)/60 DESC""")
       df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
       st.write(df)

   elif question=='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
       mycursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_Id,
       v_comment AS Comments FROM videos ORDER BY v_comment DESC LIMIT 10""")
       df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
       st.write(df)
       sns.barplot(data=df, x="Channel_Name", y="Comments", width=0.5)
       plt.show()