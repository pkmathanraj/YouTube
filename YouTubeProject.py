import math
import re
import googleapiclient.errors
import pymongo
import psycopg2
from googleapiclient.discovery import build
from configparser import ConfigParser
from psycopg2 import sql
import urllib.parse as p
import pandas as pd
import streamlit as st
from datetime import datetime
#from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Establishing connection to YouTube API using API Key
def youtube_connection(API_key):
    api_service_name = "youtube"
    api_version = "v3"
    key = API_key
    return build(api_service_name, api_version, developerKey = key)

# Parsing Duration
def parse_duration(duration):
    hours_pattern = re.compile(r'(\d+)H')
    minutes_pattern = re.compile(r'(\d+)M')
    seconds_pattern = re.compile(r'(\d+)S')
    hours = hours_pattern.search(duration)
    minutes = minutes_pattern.search(duration)
    seconds = seconds_pattern.search(duration)
    hours = hours.group(1) if hours else 0
    minutes = minutes.group(1) if minutes else 0
    seconds = seconds.group(1) if seconds else 0
    duration = str(hours) + ":" + str(minutes) + ":" + str(seconds)
    return duration

def datetime_parser(dtstr):
    #dtstr="2021-07-17T14:16:45Z"
    parse_date = re.search(r'\d{4}-\d{2}-\d{2}', dtstr)
    date = datetime.strptime(parse_date.group(), '%Y-%m-%d').date()
    parse_time = re.search(r'\d{2}:\d{2}:\d{2}', dtstr)
    time = datetime.strptime(parse_time.group(), '%H:%M:%S').time()
    datestr = parse_date.group()
    timestr = parse_time.group()
    return datestr, timestr

# Extracting Channel Details
def get_channel_details(channel_id, youtube):
    channel_response = youtube.channels().list(part = "snippet,contentDetails,statistics",
                                               id = channel_id).execute()
    snippet = channel_response['items'][0]['snippet']
    stat = channel_response['items'][0]['statistics']
    cdt, ctm = datetime_parser(snippet['publishedAt'])
    channel_info = {'channel_id' : channel_response['items'][0]['id'],
                    'channel_name' : snippet['title'],
                    'channel_description' : snippet['description'],
                    'created_date' : cdt,
                    'created_time' : ctm,
                    'thumbnail' : snippet['thumbnails']['medium']['url'],
                    'country' : snippet['country'],
                    'upload_id' : channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                    'view_count' : int(stat['viewCount']),
                    'subscriber_count' : int(stat['subscriberCount']),
                    'video_count' : int(stat['videoCount'])
                    }
    return channel_info

# Extracting Video id's through Playlist Items
def get_playlist_videos(channel_info, youtube):
    tot_pages = math.ceil(channel_info['video_count']/50)
    tot_videos = 0
    page_token = None
    playlist_info={}
    vid_cnt=1

    for i in range(tot_pages):
        parameters = {'part': 'snippet',
                      'playlistId': channel_info['upload_id'],
                      'maxResults': 50
                     }
        if page_token:
            parameters['pageToken'] = page_token
        playlist_request = youtube.playlistItems().list(**parameters)
        playlist_response = playlist_request.execute()

        for i in range(0,len(playlist_response['items'])):
            snippet = playlist_response['items'][i]['snippet']
            vid_id = snippet['resourceId']['videoId']
            playlist_info.update({'video_'+str(vid_cnt) : {'playlist_id' : snippet['playlistId'],
                                                           'channel_id' : snippet['channelId'],
                                                           'video_id' : vid_id,
                                                           'video_url' : f"https://www.youtube.com/watch?v={vid_id}"
                                                          }})
            vid_cnt+=1

        if "nextPageToken" in playlist_response:
            page_token = playlist_response["nextPageToken"]
    return playlist_info

# Requesting & Extracting Video Details and Comment Details
def get_videos_comments(playlist_info, youtube):
    page_token = None
    videos_info={}
    comments_info={}
    reply_info = {}
    cmnt_cnt=1
    reply_cnt = 1

    for vid_nm in playlist_info:
        vid_id = playlist_info[vid_nm]['video_id']
        vid_request = youtube.videos().list(part = "snippet,contentDetails,statistics",
                                            id = vid_id)
        vid_response = vid_request.execute()
        video_item = vid_response['items'][0]
        vid_snippet = video_item['snippet']
        vid_stat = video_item['statistics']
        video_duration = parse_duration(video_item['contentDetails']['duration'])
        pub_date, pub_time = datetime_parser(vid_snippet['publishedAt'])
        #print(vid_nm)  # To check the status of which video detail it is reading. We can use loading tool here
        videos_info.update({vid_nm : {'video_id' : vid_id,
                                    'playlist_id' : playlist_info[vid_nm]['playlist_id'],
                                    'video_name' : vid_snippet['title'],
                                    'video_description' : vid_snippet['description'],
                                    'published_date' : pub_date,
                                    'published_time' : pub_time,
                                    'duration' : video_duration,
                                    'view_count' : int(vid_stat['viewCount']),
                                    'like_count' : int(vid_stat['likeCount']),
                                    'favorite_count' : int(vid_stat['favoriteCount']),
                                    'video_url' : playlist_info[vid_nm]['video_url'],
                                    'caption_status' : video_item['contentDetails']['caption'],
                                    'thumbnail' : vid_snippet['thumbnails']['medium']['url']
                                    }})
        if vid_snippet.get('tags'):
            videos_info[vid_nm]['tags'] = vid_snippet['tags']
        else:
            videos_info[vid_nm]['tags'] = None
        
        # Checking whether comment is disabled or not for the respective video before extracting comments
        if vid_stat.get('commentCount'):
            videos_info[vid_nm]['comment_count'] = int(vid_stat['commentCount'])
            videos_info[vid_nm]['comment_status'] = "Enabled"
            comment_Count = int(vid_stat['commentCount'])
            tot_pages = math.ceil(comment_Count/100)
            # Extracting comments for the video
            for i in range(tot_pages):
                parameters = {'part': 'snippet,replies',
                            'videoId': vid_id,
                            'maxResults': 100
                            }
                if page_token:
                    parameters['pageToken'] = page_token
                cmmnt_request = youtube.commentThreads().list(**parameters)
                cmmnt_response = cmmnt_request.execute()

                for i in range(0,len(cmmnt_response['items'])):
                    cmmnt_snippet = cmmnt_response['items'][i]['snippet']['topLevelComment']['snippet']
                    comment_id = cmmnt_response['items'][i]['snippet']['topLevelComment']['id']
                    comment_text = cmmnt_snippet['textOriginal']
                    comment_author = cmmnt_snippet['authorDisplayName']
                    pub_date, pub_time = datetime_parser(cmmnt_snippet['publishedAt'])
                    comment_likes = int(cmmnt_snippet['likeCount'])
                    comments_info.update({'comment_'+str(cmnt_cnt) : {'comment_id' : comment_id,
                                                                    'video_id' : vid_id,
                                                                    'comment_text' : comment_text,
                                                                    'comment_author' : comment_author,
                                                                    'comment_published_date' : pub_date,
                                                                    'comment_published_time' : pub_time,
                                                                    'comment_likes' : comment_likes
                                                                    }})
                    # Extracting Replies in the comment
                    
                    if "replies" in cmmnt_response['items'][i]:
                        
                        cmmnt_dic = cmmnt_response['items'][i]['replies']['comments']
                        for r in cmmnt_dic:
                            #print(r)
                            snippet = r['snippet']
                            reply_id = r['id']
                            reply_text = snippet['textOriginal']
                            reply_author = snippet['authorDisplayName']
                            pub_date, pub_time = datetime_parser(snippet['publishedAt'])
                            reply_likes = int(snippet['likeCount'])
                            reply_info.update({'reply_'+str(reply_cnt): {'reply_id' : reply_id,
                                                                        'comment_id' : snippet['parentId'],
                                                                        'reply_text' : reply_text,
                                                                        'reply_author' : reply_author,
                                                                        'reply_published_date' : pub_date,
                                                                        'reply_published_time' : pub_time,
                                                                        'reply_likes' : reply_likes
                                                                        }})
                            reply_cnt+=1
                    
                    #if reply_info:
                    #    comments_info['comment_'+str(cmnt_cnt)]['replies'] = reply_info

                    cmnt_cnt+=1
            
            if "nextPageToken" in cmmnt_response:
                page_token = cmmnt_response["nextPageToken"]
        else:
            videos_info[vid_nm]['comment_count'] = 0
            videos_info[vid_nm]['comment_status'] = "Disabled"

    # Extracting Total No of Videos
    # total_results = response['pageInfo']['totalResults']
    return videos_info, comments_info, reply_info

def store_mongodb(data_list, db):
    channel_name = data_list[0]['channel']['channel_name']
    channel_name = channel_name.replace(" ","")  # Removing whitespaces before storing it as a collection
    col = db[channel_name]
    collections = db.list_collection_names()

    if channel_name in collections:
        choice = st.radio("Data already exists, do u want to update ?", ["Yes", "No"])
        submit = st.button("Submit")
        if submit:
            if choice == 'Yes':
                col.drop()
                col.insert_many(data_list)
                st.info("Database updated in MongoDb")
            elif choice == 'No':
                st.info("Database not updated in MongoDb")
            else:
                st.warning("Please select your choice before clicking submit")
    else:
        col.insert_many(data_list)
        st.info("Data is successfully stored in MongoDb")
    return channel_name

# Database Configuration - Extraction
def config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)
  
    # get section, default to postgresql
    conn_param = {}
    if parser.has_section(section):
        params = parser.items(section)
        for p in params:
            conn_param[p[0]] = p[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
  
    return conn_param

# Creating Database in PostgreSQL
def create_sqldatabase():
    db_name = "Youtube"
    params = config()
    conn = None
    try:
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        #conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor=conn.cursor()
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

# Establishing PostgreSQL Connection
def connect():
    db_name = "Youtube"
    params = config()
    params['dbname'] = db_name
    conn = psycopg2.connect(**params)
    return conn

# Creating tables in PostgreSQL
def create_sqlschema():
    conn = connect()
    query_channel = "create table if not exists channel(\
        channel_id varchar PRIMARY KEY,\
        channel_name varchar,\
        channel_description varchar,\
        created_date date,\
        created_time time,\
        thumbnail varchar,\
        country varchar,\
        upload_id varchar,\
        view_count int,\
        subscriber_count int,\
        video_count int)"
    query_playlist = "create table if not exists playlist(\
        playlist_id varchar PRIMARY KEY,\
        channel_id varchar,\
        playlist_name varchar,\
        CONSTRAINT fk_channel FOREIGN KEY(channel_id) REFERENCES channel(channel_id) ON DELETE CASCADE)"
    query_video = "create table if not exists video(\
        video_id varchar PRIMARY KEY,\
        playlist_id varchar,\
        video_name varchar,\
        video_description varchar,\
        published_date date,\
        published_time time,\
        duration time,\
        view_count int,\
        like_count int,\
        favorite_count int,\
        video_url varchar,\
        caption_status varchar,\
        thumbnail varchar,\
        tags varchar,\
        comment_count int,\
        comment_status varchar,\
        CONSTRAINT fk_playlist FOREIGN KEY(playlist_id) REFERENCES playlist(playlist_id) ON DELETE CASCADE)"
    query_comment = "create table if not exists comment(\
        comment_id varchar PRIMARY KEY,\
        video_id varchar,\
        comment_text varchar,\
        comment_author varchar,\
        comment_published_date date,\
        comment_published_time time,\
        comment_likes int,\
        CONSTRAINT fk_video FOREIGN KEY(video_id) REFERENCES video(video_id) ON DELETE CASCADE)"
    query_reply = "create table if not exists reply(\
        reply_id varchar PRIMARY KEY,\
        comment_id varchar,\
        reply_text varchar,\
        reply_author varchar,\
        reply_published_date date,\
        reply_published_time time,\
        reply_likes int,\
        CONSTRAINT fk_comment FOREIGN KEY(comment_id) REFERENCES comment(comment_id) ON DELETE CASCADE)"
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(query_channel)
            cursor.execute(query_playlist)
            cursor.execute(query_video)
            cursor.execute(query_comment)
            cursor.execute(query_reply)

def pgsql_channel_migration(chnl_nm, db):
    col = db[chnl_nm]
    # col.find() results all the documents with 'channel' and others as None
    # if condition check for not null document and reading the channel details of 'channel' document and store it as a list
    res = [i.get('channel') for i in col.find({},{'_id':0,'channel':1}) if i.get('channel')]

    # The 'res' object is a list with dictionary element which is the channel details
    # Extracting the values of the dictionary and converting it into list of tuples
    record=[]
    for dic in res:
        values = tuple([j for j in dic.values()])
        record.append(values)

    # Building a custom string to execute multiple records using single execute() statement
    record_string = ",".join("('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', \
                            '%s', '%s')" % (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) \
                                for (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) in record)
    conn = connect()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO channel VALUES" + record_string)
            # Code something to show msg if the record already exists. 
            # The above stmt will throw psycopg2.errors.UniqueViolation: duplicate key value violates unique constraint "channel_pkey",
            # if record aleardy exists.
    conn.close()

def pgsql_playlist_migration(chnl_nm, db):
    col = db[chnl_nm]
    channel_name = chnl_nm + " Main Playlist"
    res = [i.get('playlists') for i in col.find({},{'_id':0,'playlists':1}) if i.get('playlists')]
    record=[]
    for dic in res:
        for d in dic.values():
            values = tuple([d['playlist_id'],d['channel_id'],channel_name]) # simplify this code
            record.append(values)
            break
        break

    record_string = ",".join("('%s', '%s', '%s')" % (c1, c2, c3) \
                                for (c1, c2, c3) in record)
    conn = connect()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO playlist VALUES" + record_string)
    conn.close()

def pgsql_video_migration(chnl_nm, db):
    col = db[chnl_nm]
    res = [i.get('videos') for i in col.find({},{'_id':0,'videos':1}) if i.get('videos')]
    record=[]
    for dic in res:
        for d in dic.values():
            values = tuple([j for j in dic.values()])
            record.append(values)
            

    record_string = ",".join("('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', \
                            '%s', '%s', '%s', '%s', '%s')" % (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15) \
                                for (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15) in record)
    conn = connect()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO video VALUES" + record_string)
    conn.close()

def pgsql_comment_migration(chnl_nm, db):
    col = db[chnl_nm]
    res = [i.get('comments') for i in col.find({},{'_id':0,'comments':1}) if i.get('comments')]
    record=[]
    for dic in res:
        for d in dic.values():
            values = tuple([j for j in dic.values()])
            record.append(values)
            

    record_string = ",".join("('%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (c1, c2, c3, c4, c5, c6, c7) \
                                for (c1, c2, c3, c4, c5, c6, c7) in record)
    conn = connect()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO comment VALUES" + record_string)
    conn.close()

def pgsql_reply_migration(chnl_nm, db):
    col = db[chnl_nm]
    res = [i.get('replies') for i in col.find({},{'_id':0,'replies':1}) if i.get('replies')]
    record=[]
    for dic in res:
        for d in dic.values():
            values = tuple([j for j in dic.values()])
            record.append(values)
            

    record_string = ",".join("('%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (c1, c2, c3, c4, c5, c6, c7) \
                                for (c1, c2, c3, c4, c5, c6, c7) in record)
    conn = connect()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO reply VALUES" + record_string)
    conn.close()

def sql_channel_list():
    # Collecting Channel Names from SQLDb
    conn = connect()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute("select channel_name from channel")
            chnl_nms = cursor.fetchall()
            chnl_nms = [i[0] for i in chnl_nms]
            chnl_nms.sort(reverse=False)
    conn.close()
    return chnl_nms

def sql_migration(channel_name, db):
    # Collecting List of Collections from MongoDb
    col_lst = db.list_collection_names()
    col_lst.sort(reverse=False)
    if col_lst:
        st.subheader("Collections in MongoDb:")
        cnt=1
        for i in col_lst:
            st.write(str(cnt) + ' : ' + i)
            cnt+=1
    else:
        st.info("MongoDb has no details regarding any channel")

    # Displaying Channel Names from SQLDb
    chnl_nms = sql_channel_list()
    if chnl_nms:
        st.subheader("Channel List in SQL:")
        cnt=1
        for i in chnl_nms:
            st.write(str(cnt) + ' : ' + i)
            cnt+=1
    else:
        st.info("SQL Db has no details regarding any channel")

    # Creating user option
    options = ['Select a Channel']
    for i in col_lst:
        if i not in chnl_nms:
            options.append(i)
    
    # Displaying channel list for migration
    choice = st.selectbox("List of Channels ready for migration: ", options)
    submit = st.button("Migrate")
    if submit:
        if choice == "Select a Channel":
            st.warning("Please select a channel")
        else:
            channel_name = choice
            pgsql_channel_migration(channel_name, db)
            pgsql_playlist_migration(channel_name, db)
            pgsql_video_migration(channel_name, db)
            pgsql_comment_migration(channel_name, db)
            pgsql_reply_migration(channel_name, db)
            st.success("Data successfully migrated to SQL Db")

def sql_query_processor():
    pass

def sql_querylist():
    st.subheader("SQL Query Result")
    q1 = "List of names of all the videos and their corresponding channel"
    q2 = "List of channels with the most number of videos with video count"
    q3 = "Top 10 most viewed videos and their respective channel"
    q4 = "Number of comments of each video with their corresponding video name"
    q5 = "List of videos with the highest number of likes with their corresponding channel name"
    q6 = "Total number of likes for each video with their corresponding video name"
    q7 = "Total number of views for each channel with their corresponding channel name"
    q8 = "List of names of all the channels that have published videos in the year 2022"
    q9 = "Average duration of all videos in each channel with their corresponding channel name"
    q10 = "List of videos with the highest number of comments with their corresponding channel name"

    qchoice = st.selectbox("Select any of the Query from the List:", ['Select any', q1, q2, q3, q4, q5, q6, q7, q8, q9, q10])
    if qchoice == q1:
        st.dataframe(sql_query_processor())
    elif qchoice == q2:
        st.dataframe(sql_query_processor())
    elif qchoice == q3:
        st.dataframe(sql_query_processor())
    elif qchoice == q4:
        st.dataframe(sql_query_processor())
    elif qchoice == q5:
        st.dataframe(sql_query_processor())
    elif qchoice == q6:
        st.dataframe(sql_query_processor())
    elif qchoice == q7:
        st.dataframe(sql_query_processor())
    elif qchoice == q8:
        year = st.text_input('Enter the year')
        submit = st.button('Submit')
        if submit:
            st.dataframe(sql_query_processor())
    elif qchoice == q9:
        st.dataframe(sql_query_processor())
    elif qchoice == q10:
        st.dataframe(sql_query_processor())

def front_end_design(youtube, db):
    if 'merged_data' not in st.session_state:
        st.session_state.merged_data = []
    if 'channel_name' not in st.session_state:
        st.session_state.channel_name = ""
    s0="Select the Task..."
    s1="Channel Data Extraction"
    s2="Storing in MongoDb"
    s3="Migrating to SQL"
    s4="SQL Queries"
    st.title('YouTube Data Harvesting and Warehousing')
    st.divider()
    with st.sidebar:
        option = st.selectbox("Task to Perform:",(s0, s1, s2, s3, s4), help="Select any to begin",\
                                placeholder="Select the Task...")
        st.write(option)
    if option==s1:
        channel_id = st.text_input("Enter the YouTube Channel Id:")
        submit = st.button("Extract")
        if submit:
            if channel_id:
                try:
                    channel_data = get_channel_details(channel_id, youtube)
                    playlist_data = get_playlist_videos(channel_data, youtube)
                    video_data, comments_data, reply_data = get_videos_comments(playlist_data, youtube)
                    st.session_state.merged_data = [{'channel':channel_data}, {'playlists':playlist_data}, {'videos':video_data}, {'comments':comments_data}, {'replies':reply_data}]
                    json_data = {'channel': channel_data, 'playlists': playlist_data, 'videos': video_data, 'comments': comments_data, 'replies': reply_data}
                    st.success("Data Extracted successfully from the given YouTube Channel")
                    st.json(json_data, expanded = False)
                except:
                    st.error("Please provide correct channel Id")
            else:
                st.warning("Please provide some Channel Id")
    elif option==s2:
        #st.text("Storing Data to MongoDb")
        if st.session_state.merged_data:
            create_sqldatabase()
            create_sqlschema()
            st.session_state.channel_name = store_mongodb(st.session_state.merged_data, db)
        else:
            st.error("Complete the Data Extraction before storing it into MongoDb")
    elif option==s3:
        st.text("Migrating Data to SQL")
        if st.session_state.channel_name:
            sql_migration(st.session_state.channel_name, db)
        else:
            st.error("Complete the Storing in MongoDb part before Migrating to SQL")
    elif option==s4:
        chnl_nms = sql_channel_list()
        if chnl_nms:
            sql_querylist()
        else:
            st.info("No Channel Details available in SQL Db. Complete the previous steps.")
    

def main():
    APIKEY = "Your API Key"
    #channel_id = "UCws0tsIh0zcExs4e064Mibg"
    if 'youtube' not in st.session_state:
        st.session_state.youtube = youtube_connection(APIKEY)
    if 'dbConnection' not in st.session_state:
        st.session_state.dbConnection = pymongo.MongoClient("mongodb+srv://pkmathanraj:86l6HxBwkTLmIlOM@utube.n0xwb3j.mongodb.net/?retryWrites=true&w=majority")
    if 'db' not in st.session_state:
        st.session_state.db = st.session_state.dbConnection["YouTubeDb"]
    
    front_end_design(st.session_state.youtube,st.session_state.db)
    

if __name__ == "__main__":
    main() 
