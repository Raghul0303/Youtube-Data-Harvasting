
 

import streamlit as st
from pyyoutube import Api

#mongoDB imports
import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd

#mysql imports
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import pymysql

api = Api(api_key="Api_key***********")

#establishing a connection in mongodb
client = pymongo.MongoClient("mongodb+srv://username:password@cluster0.gkstsrk.mongodb.net/?retryWrites=true&w=majority")
db = client.e12
collection=db.youtube


# #establishing a connection in sql
connect = mysql.connector.connect(
host = "db4free.net",
user = "user",
password = "password",
auth_plugin = "mysql_native_password")


engine = create_engine('mysql+pymysql://user:password@db4free.net/user?charset=utf8mb4', echo=False)


#pushing youtube details into mongoDB
def push_to_mongo(pd_youtube):
    push_status = collection.insert_one(pd_youtube)
    return push_status


#extracting channel names from documnets pushed into mongoDB
def extract_channel_names():
    channel_names = []
    documents = collection.find()
    for document in documents:
        position_key = 1
        for key, value in document.items():
            if position_key % 2 == 0:
                channel_names.append(value)
                break
            position_key += 1
    return channel_names

#Extracting channel details
def get_channel_details(channel_id):
    
    # channel
    channel = api.get_channel_info(channel_id=channel_id)
    channel_name = channel.items[0].to_dict()["snippet"]["title"]
    full_details_to_store = {}
    
    full_details_to_store[channel_name] = {
        "channel_name": channel_name,
        "channel_id": channel.items[0].to_dict()['id'],
        "video_count": channel.items[0].to_dict()['statistics']['videoCount'],
        "channel_views": channel.items[0].to_dict()['statistics']['viewCount'],
        "channel_description": channel.items[0].to_dict()["snippet"]["description"],
        "playlists": {},
        "videos": {},
        "comments": {}
    }
    
    # playlist
    playlists_by_channel = api.get_playlists(channel_id=channel_id, count=5) 
    for playlist in playlists_by_channel.items:
        full_details_to_store[channel_name]["playlists"][playlist.to_dict()["id"]] = {
            "playlist_id": playlist.to_dict()["id"],
            "channel_id": playlist.to_dict()['snippet']['channelId'],
            "playlist_title": playlist.to_dict()["snippet"]["title"],
            "videos": []
        }
    
    # videos
    playlist_dict = {}
    for i in [i.id for i in playlists_by_channel.items]:
        if i not in playlist_dict:
            playlist_dict[i] = api.get_playlist_items(playlist_id=i, count=5)
    for key, val in playlist_dict.items():
        for videos in val.items: 
            full_details_to_store[channel_name]["playlists"][key]["videos"] += [videos.contentDetails.videoId]
    for key, val in playlist_dict.items():
        for i in val.items:
            vid_dict = {}
            if i.contentDetails.videoId not in full_details_to_store[channel_name]["videos"]:
                video_details = api.get_video_by_id(video_id=i.contentDetails.videoId)
                if len(video_details.items) > 0:
                    video_dict = video_details.items[0].to_dict()
                    vid_dict["video_id"] = i.contentDetails.videoId
                    vid_dict["channel_id"] = channel_id
                    vid_dict["video_name"] = video_dict['snippet']['title']
                    vid_dict["video_description"] = video_dict['snippet']['description']
                    vid_dict["published_at"] = video_dict['snippet']['publishedAt']
                    vid_dict["view_count"] = video_dict['statistics']['viewCount']
                    vid_dict["like_count"] = video_dict['statistics']['likeCount']
                    vid_dict["dislike_count"] = video_dict['statistics']['dislikeCount']
                    vid_dict["comment_count"] = video_dict['statistics']['commentCount']
                    vid_dict["duration"] = video_dict['contentDetails']['duration']
                    vid_dict["thumbnail"] = video_dict['snippet']['thumbnails']
                    vid_dict["caption_status"] = video_dict['contentDetails']['caption']
                    vid_dict["comments"] = []
                    full_details_to_store[channel_name]["videos"][i.contentDetails.videoId] = vid_dict
                
    # comment
    for video_id in full_details_to_store[channel_name]["videos"]:
        com_dict = {}
        comment_dict = api.get_comment_threads(video_id=video_id, count=5)
        for comment in comment_dict.items:
            video_id = comment.to_dict()['snippet']['videoId']
            comment_id = comment.to_dict()['snippet']['topLevelComment']['id']
            full_details_to_store[channel_name]["videos"][video_id]["comments"] += [comment_id]
            com_dict["channel_id"] = channel_id
            com_dict["Video_id"] = video_id
            com_dict["Comment_Id"] = comment_id
            com_dict["Comment_Text"] = comment.to_dict()['snippet']['topLevelComment']['snippet']['textOriginal']
            com_dict["Comment_Author"] = comment.to_dict()['snippet']['topLevelComment']['snippet']['authorDisplayName']
            com_dict["Comment_PublishedAt"] = comment.to_dict()['snippet']['topLevelComment']['snippet']['publishedAt']
            full_details_to_store[channel_name]["comments"][comment_id] = com_dict
   
    return {"channel_name": full_details_to_store[channel_name]["channel_name"], "data": full_details_to_store[channel_name]}


#Migrating channel details data from mongodb to SQL
def migrate_to_sql(channel_name):

    channel_data = collection.find({"channel_name": channel_names})[0]

    channel_df = pd.DataFrame([[channel_data["data"]["channel_name"], channel_data["data"]["channel_id"], channel_data["data"]["video_count"] , channel_data["data"]["channel_views"], channel_data["data"]["channel_description"]]], 
                                columns=["Channel_Name", "Channel_Id","Video_Count" ,"Channel_Views", "Channel_Description"])
    channel_df.to_sql('channel', engine, if_exists='append', index=False, 
                        dtype={"Channel_Name": sqlalchemy.types.VARCHAR(length=225),
                            "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                            "Channel_Views": sqlalchemy.types.BigInteger,
                            "Channel_Description": sqlalchemy.types.TEXT})

    playlist = []
    for key, val in channel_data["data"]["playlists"].items():
        playlist.append([val["playlist_id"], val["channel_id"], val["playlist_title"]])
    playlist_df = pd.DataFrame(playlist, columns=["Playlist_Id", "Channel_Id", "Playlist_Title"])
    playlist_df.to_sql('playlist', engine, if_exists='append', index=False, 
                        dtype={"Playlist_Id": sqlalchemy.types.VARCHAR(length=225),
                            "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                            "Playlist_Title": sqlalchemy.types.VARCHAR(length=225)})

    video = []
    for key, val in channel_data["data"]["videos"].items():
        video.append([val["video_id"], val['channel_id'], val["video_name"], val["video_description"],val["published_at"],val["view_count"],val["like_count"],val["dislike_count"],val["comment_count"],val["duration"],val["caption_status"]])
    video_df = pd.DataFrame(video, columns=["Video_Id", 'Channel_Id' ,"Video_Name", "Video_Description",'Published_date','View_Count','Like_Count','Dislike_Count','Comment_Count','Duration','Caption_Status'])
    video_df.to_sql('video', engine, if_exists='append', index=False, 
                        dtype={'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Channel_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Video_Name': sqlalchemy.types.VARCHAR(length=225),
                            'Video_Description': sqlalchemy.types.TEXT,
                            'Published_date': sqlalchemy.types.String(length=50),
                            'View_Count': sqlalchemy.types.BigInteger,
                            'Like_Count': sqlalchemy.types.BigInteger,
                            'Dislike_Count': sqlalchemy.types.INT,
                            'Comment_Count': sqlalchemy.types.INT,
                            'Duration': sqlalchemy.types.VARCHAR(length=1024),
                            'Caption_Status': sqlalchemy.types.VARCHAR(length=225)})

    comment = []
    for key, val in channel_data["data"]["comments"].items():
        comment.append([val["Video_id"],val['channel_id'] , val["Comment_Id"], val["Comment_Text"],val["Comment_Author"],val["Comment_PublishedAt"]])
    comment_df = pd.DataFrame(comment, columns=['Video_Id','Channel_Id','Comment_Id','Comment_Text','Comment_Author','Comment_Published_date'])
    comment_df.to_sql('comment', engine, if_exists='append', index=False,
                        dtype={'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Channel_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Comment_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Comment_Text': sqlalchemy.types.TEXT,
                            'Comment_Author': sqlalchemy.types.VARCHAR(length=225),
                            'Comment_Published_date': sqlalchemy.types.String(length=50)})
    return




#Sidebar
image_path = "Youtube pic.png"
st.sidebar.image(image_path, use_column_width=True)
st.sidebar.markdown(":red[**YouTube Data Harvesting and Warehousing**]")
app_mode = st.sidebar.selectbox(
    "**SELECT PAGE**",
    options=['How to get channel ID','Selection and storage', 'Data migration', 'Data Analysis']
)

#Page Contents
if app_mode == 'How to get channel ID':
    st.title("About the application")
    st.write("#")
    st.write("The streamlit Application allows the users to access and analyze data from multiple YouTube channels.")
    st.write('This application allows the user to give a YouTube channel ID as input and retrieves the relevant data. Able to collect data for multiple YouTube channels and store them in a database by just clicking a button. ') 
    st.write('By Selecting a channel , we could migrate the data from Mongo Database to SQL, to retrieve the relevant youtube Channel information like video comments , likes.')
    st.title("How to get channel ID from YouTube")
    st.write("#")
    st.write('1. Go to your favorite youtube channel, **right click** on the channel screen. Click on the **View Page Source** option.')
    st.image('youtube right click.png')
    st.write("#")
    st.write("#")
    st.write('2. Press Ctrl+F , search for **https://www.youtube.com/channel/** in source page. The **channel ID** will appear after the link.')
    st.image('Source page.png')
    st.write("#")
    st.write("#")
    st.write('3. Paste the Channel ID in the search field of **Selection and storage**.')
    st.image('ID  in selection.png')



elif app_mode == 'Selection and storage':
    st.title("Selection and storage")
    st.write("#")

    Channel_id = st.text_input("**Enter a Channel ID**:", key="Channel_id", value="channel ID")
    st.write("(Example : UCials1wQnEN_NykYZr1048w)")
    st.write("#")
    if st.button('Store data in MongoDB'):
        channel_info = get_channel_details(Channel_id)
        pushed_to_mongo = push_to_mongo(channel_info)
        if pushed_to_mongo.acknowledged:
            st.markdown('<p style="font-weight:bold;color:green;">Data inserted in mongodb</p>', unsafe_allow_html=True)
        else:
            st.markdown('<p style="font-weight:bold;color:red;">Error: Data not pushed to mongodb</p>', unsafe_allow_html=True)
    

elif app_mode == 'Data migration':
    st.title("Data migration ")
    st.write("#")
    channel_name = extract_channel_names()
    channel_names = st.selectbox("**Select a Channel name**:",channel_name)
    st.write("#")
    if st.button('Migrate to SOL'):
        migrate_to_sql(channel_names)
        collection.delete_one({'channel_name': channel_names})


elif app_mode == 'Data Analysis':
    st.title('Data Analysis')
    Questions = ['1. What are the names of all the videos and their corresponding channels',
                 '2. Which channels have the most number of videos, and how many videos do they have',
                 '3. What are the top 10 most viewed videos and their respective channels',
                 '4. How many comments were made on each video, and what are their corresponding video names',
                 '5. Which videos have the highest number of likes, and what are their corresponding channel names'
                 ]

    input_question = st.selectbox("**Select a Question regarding the channels**:",Questions)

    retrieve_answer_from_sql = pymysql.connect(host = "db4free.net",user = "user",password = "password",db='db')
    cursor = retrieve_answer_from_sql.cursor()

    if input_question == '1. What are the names of all the videos and their corresponding channels':
        cursor.execute("""SELECT channel.Channel_Name , video.Video_Name FROM channel JOIN video ON video.Channel_Id = channel.Channel_Id""")
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]  
        df_1 = pd.DataFrame(result, columns=columns).reset_index()
        df_1.index += 1
        st.write(df_1)

    elif input_question == '2. Which channels have the most number of videos, and how many videos do they have':
        cursor.execute("SELECT Channel_Name, Video_Count FROM channel ORDER BY Video_Count DESC;")
        result = cursor.fetchall()
        df_2 = pd.DataFrame(result, columns=['Channel_Name', 'Video_Count']).reset_index()
        df_2.index += 1
        st.write(df_2)


    elif input_question == '3. What are the top 10 most viewed videos and their respective channels':
        cursor.execute("with channel_rank_data as ( SELECT channel.Channel_Name as channel_name, video.Video_Name as video_name, video.View_Count, row_number() over (partition by channel_name order by video.View_Count desc) as video_rank FROM channel JOIN video ON video.Channel_Id = channel.Channel_Id ) select * from channel_rank_data where video_rank <=10;") 
        result = cursor.fetchall()
        df_3 = pd.DataFrame(result, columns=['Channel_Name','Video_Name', 'View_Count', 'Rank']).reset_index()
        df_3.index += 1
        st.write(df_3)
    
    
    elif input_question == '4. How many comments were made on each video, and what are their corresponding video names':
        cursor.execute("SELECT channel.Channel_Name ,COUNT(*) AS Comment_Count ,video.Video_Name FROM video JOIN comment ON video.Video_Id = comment.Video_Id JOIN channel ON video.Channel_Id = channel.Channel_Id GROUP BY video.Video_Id, video.Video_Name, channel.Channel_Name;")
        result = cursor.fetchall()
        df_4 = pd.DataFrame(result, columns=['Channel name','Comment Count', 'Video Name']).reset_index()
        df_4.index += 1
        st.write(df_4)


    elif input_question == '5. Which videos have the highest number of likes, and what are their corresponding channel names':
        cursor.execute("SELECT channel.Channel_Name, video.Like_Count, video.Video_Name FROM video JOIN channel ON video.Channel_Id = channel.Channel_Id ORDER BY video.Like_Count DESC LIMIT 10;")
        result = cursor.fetchall()
        df_5 = pd.DataFrame(result, columns=['Channel Name', 'Like Count','Video Name']).reset_index()
        df_5.index += 1
        st.write(df_5)
