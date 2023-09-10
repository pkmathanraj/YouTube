import math
import re
import googleapiclient.errors
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from datetime import datetime
import psycopg2.extras
import sys
from googleapiclient.discovery import build
from configparser import ConfigParser
from psycopg2 import sql
from collections import OrderedDict
#from iteration_utilities import unique_everseen

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

# Parsing Date and Time
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
    page_token = None
    playlist_info = {'playlist_id' : channel_info['upload_id'],
                    'channel_id' : channel_info['channel_id'],
                    'channel_name' : channel_info['channel_name'] 
                    }
    video_ids=[]
    for i in range(tot_pages):
        parameters = {'part': 'snippet',
                      'playlistId': channel_info['upload_id'],
                      'maxResults': 50
                     }
        if page_token:
            parameters['pageToken'] = page_token
        playlist_request = youtube.playlistItems().list(**parameters)
        playlist_response = playlist_request.execute()

        
        for i in range(len(playlist_response['items'])):
            video_ids.append(playlist_response['items'][i]['snippet']['resourceId']['videoId'])

        if "nextPageToken" in playlist_response:
            page_token = playlist_response["nextPageToken"]
    return playlist_info, video_ids

# Requesting & Extracting Video Details and Comment Details
def get_videos_comments(playlist_info, video_ids, youtube):
    page_token = None
    videos_info=[]
    comments_info=[]
    reply_info = []
    vid_cnt=0

    for vid in video_ids:
        vid_request = youtube.videos().list(part = "snippet,contentDetails,statistics",
                                            id = vid)
        vid_response = vid_request.execute()
        video_item = vid_response['items'][0]
        vid_snippet = video_item['snippet']
        vid_stat = video_item['statistics']
        video_duration = parse_duration(video_item['contentDetails']['duration'])
        pub_date, pub_time = datetime_parser(vid_snippet['publishedAt'])
        #print(vid_nm)  # To check the status of which video detail it is reading. We can use loading tool here
        view_count = int(vid_stat['viewCount']) if vid_stat.get('viewCount') else 0
        like_count = int(vid_stat['likeCount']) if vid_stat.get('likeCount') else 0
        videos_info.append({'video_id' : vid, 
                            'playlist_id' : playlist_info['playlist_id'], 
                            'video_name' : vid_snippet['title'],
                            'video_description' : vid_snippet['description'],
                            'published_date' : pub_date,
                            'published_time' : pub_time,
                            'duration' : video_duration,
                            'view_count' : view_count,
                            'like_count' : like_count,
                            'favorite_count' : int(vid_stat['favoriteCount']),
                            'video_url' : f"https://www.youtube.com/watch?v={vid}",
                            'caption_status' : video_item['contentDetails']['caption'],
                            'thumbnail' : vid_snippet['thumbnails']['medium']['url']
                            })
        if vid_snippet.get('tags'):
            videos_info[vid_cnt]['tags'] = vid_snippet['tags']
        else:
            videos_info[vid_cnt]['tags'] = None
        
        # Checking whether comment is available or not for the respective video before extracting comments
        if vid_stat.get('commentCount'):
            videos_info[vid_cnt]['comment_count'] = int(vid_stat['commentCount'])
            comment_Count = int(vid_stat['commentCount'])
            tot_pages = math.ceil(comment_Count/100)
            # Extracting comments for the video
            for i in range(tot_pages):
                parameters = {'part': 'snippet,replies',
                            'videoId': vid,
                            'maxResults': 100
                            }
                if page_token:
                    parameters['pageToken'] = page_token
                cmmnt_request = youtube.commentThreads().list(**parameters)
                cmmnt_response = cmmnt_request.execute()

                for i in range(len(cmmnt_response['items'])):
                    cmmnt_snippet = cmmnt_response['items'][i]['snippet']['topLevelComment']['snippet']
                    pub_date, pub_time = datetime_parser(cmmnt_snippet['publishedAt'])
                    comment_likes = int(cmmnt_snippet['likeCount']) if cmmnt_snippet.get('likeCount') else 0
                    comments_info.append({'comment_id' : cmmnt_response['items'][i]['snippet']['topLevelComment']['id'],
                                        'video_id' : vid,
                                        'comment_text' : cmmnt_snippet['textOriginal'],
                                        'comment_author' : cmmnt_snippet['authorDisplayName'],
                                        'comment_published_date' : pub_date,
                                        'comment_published_time' : pub_time,
                                        'comment_likes' : int(cmmnt_snippet['likeCount'])
                                        })
                    
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
                            reply_likes = int(snippet['likeCount']) if snippet.get('likeCount') else 0
                            reply_info.append({'reply_id' : reply_id,
                                            'comment_id' : snippet['parentId'],
                                            'reply_text' : reply_text,
                                            'reply_author' : reply_author,
                                            'reply_published_date' : pub_date,
                                            'reply_published_time' : pub_time,
                                            'reply_likes' : reply_likes
                                            })
            
            if "nextPageToken" in cmmnt_response:
                page_token = cmmnt_response["nextPageToken"]
        else:
            videos_info[vid_cnt]['comment_count'] = 0
        vid_cnt+=1
    comment_list = {frozenset(item.items()) : item for item in comments_info}.values()
    reply_list = {frozenset(item.items()) : item for item in reply_info}.values()
   
    return videos_info, comment_list, reply_list

def store_mongodb(channel_data, playlist_data, video_data, comments_data, reply_data, dbConn):
    channel_name = channel_data['channel_name']
    channel_name = channel_name.replace(" ","").replace("'","")  # Removing whitespaces before storing it as a db name
    db = dbConn[channel_name]
    dbs = dbConn.list_database_names()
    #db.comments.create_index('comment_id', unique = True) # Enforcing to accept unique values in comment_id
    #db.replies.create_index('reply_id', unique = True) # Enforcing to accept unique values in reply_id

    if channel_name in dbs:
        choice = st.radio("Data already exists, do u want to update ?", ["Yes", "No"])
        submit = st.button("Submit")
        if submit:
            if choice == 'Yes':
                dbConn.drop_database(channel_name)
                db = dbConn[channel_name]
                db["channel"].insert_one(channel_data)
                db["playlists"].insert_one(playlist_data)
                db["videos"].insert_many(video_data)
                db["comments"].insert_many(comments_data)   #db["comments"].insert_many(comments_data, ordered=False)
                db["replies"].insert_many(reply_data)       #db["replies"].insert_many(reply_data, ordered=False)                
                st.info("Database updated in MongoDb")
            elif choice == 'No':
                st.info("Database not updated in MongoDb")
            else:
                st.warning("Please select your choice before clicking submit")
    else:
        db["channel"].insert_one(channel_data)
        db["playlists"].insert_one(playlist_data)
        db["videos"].insert_many(video_data)
        db["comments"].insert_many(comments_data)   #db["comments"].insert_many(comments_data, ordered=False)
        db["replies"].insert_many(reply_data)       #db["replies"].insert_many(reply_data, ordered=False)  
        st.info("Data is successfully stored in MongoDb")
    return channel_name

# PGSQL Extracting Database Configuration from file
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

# Storing Channel details from mongodb to PgSQL
def pgsql_channel_migration(chnl_nm, dbConn):
    db = dbConn[chnl_nm]
    col = db["channel"]
    result = [i for i in col.find({},{'_id':0})]
    record = [tuple([i for i in result[0].values()])]
    
    conn = connect()
    with conn:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_batch(cursor,"""INSERT INTO channel VALUES (%s, %s, %s, %s, %s, %s, %s, %s, \
                            %s, %s, %s)""" , record)
    conn.close()

# Storing Playlist details from mongodb to PgSQL
def pgsql_playlist_migration(chnl_nm, dbConn):
    db = dbConn[chnl_nm]
    col = db["playlists"]
    result = [i for i in col.find({},{'_id':0})]
    record=[tuple([i for i in result[0].values()])]

    conn = connect()
    with conn:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_batch(cursor,"""INSERT INTO playlist VALUES (%s, %s, %s)""" , record)
    conn.close()

# Storing Video details from mongodb to PgSQL
def pgsql_video_migration(chnl_nm, dbConn):
    db = dbConn[chnl_nm]
    col = db["videos"]
    res = [i for i in col.find({},{'_id':0})]
    record=[]
    for dic in res:
        record.append(tuple([d for d in dic.values()]))
    
    conn = connect()
    with conn:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_batch(cursor, "INSERT INTO video VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", record)
    conn.close()

# Storing Comment details from mongodb to PgSQL
def pgsql_comment_migration(chnl_nm, dbConn):
    db = dbConn[chnl_nm]
    col = db["comments"]
    res = [i for i in col.find({},{'_id':0})]
    record=[]
    for dic in res:
        record.append(tuple([d for d in dic.values()]))
            
    conn = connect()
    with conn:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_batch(cursor, "INSERT INTO comment VALUES (%s, %s, %s, %s, %s, %s, %s)", record)
    conn.close()

# Storing Reply details from mongodb to PgSQL
def pgsql_reply_migration(chnl_nm, dbConn):
    db = dbConn[chnl_nm]
    col = db["replies"]
    res = [i for i in col.find({},{'_id':0})]
    record=[]
    for dic in res:
        record.append(tuple([d for d in dic.values()]))
            
    conn = connect()
    with conn:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_batch(cursor, "INSERT INTO reply VALUES (%s, %s, %s, %s, %s, %s, %s)", record)
    conn.close()

# Getting all the channels stored in PgSQL
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

# Migrating all the data from mongodb to PgSQL
def sql_migration(dbConn):
    # Collecting List of Dbs from MongoDb
    dbs = dbConn.list_database_names()
    dbs.remove('admin')
    dbs.remove('local')
    #col_lst = db.list_collection_names()
    dbs.sort(reverse=False)
    col1, col2 = st.columns(2)
    with col1:
        if dbs:
            st.subheader("Channels Stored in MongoDb:")
            cnt=1
            for i in dbs:
                st.write(str(cnt) + ' : ' + i)
                cnt+=1
        else:
            st.info("MongoDb has no data regarding any channel")

    # Displaying Channel Names from SQLDb
    chnl_nms = sql_channel_list()
    with col2:
        if chnl_nms:
            st.subheader("Channels Stored in PGSQL:")
            cnt=1
            for i in chnl_nms:
                st.write(str(cnt) + ' : ' + i)
                cnt+=1
        else:
            st.info("PGSQL Db has no data regarding any channel")

    # Creating user option
    options = [j for j in dbs if j not in chnl_nms]
    
    # Displaying channel list for migration
    choice = st.selectbox("List of Channels ready for migration: ", options)
    submit = st.button("Migrate")
    if submit:
        channel_name = choice
        pgsql_channel_migration(channel_name, dbConn)
        st.write("Channel Data Migrated to PGSQL")
        pgsql_playlist_migration(channel_name, dbConn)
        st.write("Playlist Data Migrated to PGSQL")
        pgsql_video_migration(channel_name, dbConn)
        st.write("Video Data Migrated to PGSQL")            
        pgsql_comment_migration(channel_name, dbConn)
        st.write("Comments Data Migrated to PGSQL")
        pgsql_reply_migration(channel_name, dbConn)
        st.write("Replies Data Migrated to PGSQL")            
        
        st.success("---Data successfully migrated to SQL Db---")

# Executing SQL queries and converting to DataFrame
def sql_query_processor(ch):
    query1 = "SELECT v.video_name, c.channel_name \
            FROM channel c \
            INNER JOIN playlist USING(channel_id) \
            INNER JOIN video v USING(playlist_id) \
            ORDER BY c.channel_name"
    query2 = "SELECT channel_name, video_count \
            FROM channel \
            ORDER BY video_count DESC" 
    query3 = "SELECT v.video_name, v.view_count, c.channel_name \
            FROM channel c \
            INNER JOIN playlist USING(channel_id) \
            INNER JOIN video v USING(playlist_id) \
            ORDER BY v.view_count DESC \
            LIMIT 10"
    query4 = "SELECT video_name, comment_count \
            FROM video \
            ORDER BY comment_count DESC"
    query5 = "SELECT v.video_name, c.channel_name, v.like_count \
            FROM channel c \
            INNER JOIN playlist USING(channel_id) \
            INNER JOIN video v USING(playlist_id) \
            ORDER BY v.like_count DESC"
    query6 = "SELECT video_name, like_count \
            FROM video \
            ORDER BY like_count DESC"
    query7 = "SELECT channel_name, view_count \
            FROM channel \
            ORDER BY view_count DESC" 
    query8 = "SELECT c.channel_name, COUNT(v.published_date) AS video_count_in_2022 \
            FROM channel c \
            INNER JOIN playlist USING(channel_id) \
            INNER JOIN video v USING(playlist_id) \
            WHERE date_part('year', v.published_date) = 2022 \
            GROUP BY c.channel_name"
    query9 = "SELECT c.channel_name, SUBSTRING(CAST(AVG(v.duration) as TEXT), 1, 8) AS Avg_Video_Duration \
            FROM channel c \
            INNER JOIN playlist USING(channel_id) \
            INNER JOIN video v USING(playlist_id) \
            GROUP BY c.channel_name"
    query10 = "SELECT c.channel_name, v.video_name, v.comment_count \
            FROM channel c \
            INNER JOIN playlist USING(channel_id) \
            INNER JOIN video v USING(playlist_id) \
            ORDER BY v.comment_count DESC"
    conn = connect()
    with conn:
        with conn.cursor() as cursor:
            if ch==1:
                cursor.execute(query1)
                column_names = ['Video Name', 'Channel Name']
            elif ch==2:
                cursor.execute(query2)
                column_names = ['Channel Name', 'Video Count']
            elif ch==3:
                cursor.execute(query3)
                column_names = ['Video Name', 'View Count', 'Channel Name']
            elif ch==4:
                cursor.execute(query4)
                column_names = ['Video Name', 'Comment Count']
            elif ch==5:
                cursor.execute(query5)
                column_names = ['Video Name', 'Channel Name', 'Like Count']
            elif ch==6:
                cursor.execute(query6)
                column_names = ['Video Name', 'Like Count']
            elif ch==7:
                cursor.execute(query7)
                column_names = ['Channel Name', 'View Count']
            elif ch==8:
                cursor.execute(query8)
                column_names = ['Channel Name', 'Video Count in 2022']
            elif ch==9:
                cursor.execute(query9)
                column_names = ['Channel Name', 'Average Video Duration']
            elif ch==10:
                cursor.execute(query10)
                column_names = ['Channel Name', 'Video Name', 'Comment Count']
            qresult = cursor.fetchall()
    conn.close()
    pd.set_option('display.max_columns', None)
    df = pd.DataFrame(qresult, columns = column_names, index = [i for i in range(1,len(qresult)+1)])
    df = df.rename_axis("#")
    return df

# Creating SQL Querylist Selectbox and calling the function for the query result
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
        st.dataframe(sql_query_processor(1), width=None)
    elif qchoice == q2:
        st.dataframe(sql_query_processor(2), width=None)
    elif qchoice == q3:
        st.dataframe(sql_query_processor(3), width=None)
    elif qchoice == q4:
        st.dataframe(sql_query_processor(4), width=None)
    elif qchoice == q5:
        st.dataframe(sql_query_processor(5), width=None)
    elif qchoice == q6:
        st.dataframe(sql_query_processor(6), width=None)
    elif qchoice == q7:
        st.dataframe(sql_query_processor(7), width=None)
    elif qchoice == q8:
        st.dataframe(sql_query_processor(8), width=None)
    elif qchoice == q9:
        st.dataframe(sql_query_processor(9), width=None)
    elif qchoice == q10:
        st.dataframe(sql_query_processor(10), width=None)

# Web App front end design
def front_end_design(youtube, dbConn):
    if 'channel_data' not in st.session_state:
        st.session_state.channel_data = {}
    if 'channel_name' not in st.session_state:
        st.session_state.channel_name = ""
    if 'playlist_data' not in st.session_state:
        st.session_state.playlist_data = {}
    if 'video_data' not in st.session_state:
        st.session_state.video_data = []
    if 'comments_data' not in st.session_state:
        st.session_state.comments_data = []
    if 'reply_data' not in st.session_state:
        st.session_state.reply_data = []
    st.title('YouTube Data Harvesting and Warehousing')
    st.divider()
    s0="Select the Task..."
    s1="Channel Data Extraction"
    s2="Storing in MongoDb"
    s3="Migrating to SQL"
    s4="SQL Queries"
    
    option = st.sidebar.selectbox("Task to Perform:",(s0, s1, s2, s3, s4), help="Select any to begin",\
                                placeholder="Select the Task...")
    
    if option==s0:
        st.info("Please select an option in the Side Menu to start the process")

    elif option==s1:
        channel_id = st.text_input("Enter the YouTube Channel Id:")
        extract = st.button("Extract")
        if extract:
            if channel_id:
                try:
                    st.session_state.channel_data = get_channel_details(channel_id, youtube)
                    st.write("Channel Data Extracted")
                    st.session_state.playlist_data, video_ids = get_playlist_videos(st.session_state.channel_data, youtube)
                    st.write("Playlist data Extracted")
                    st.session_state.video_data, st.session_state.comments_data, st.session_state.reply_data = get_videos_comments(st.session_state.playlist_data, video_ids, youtube)
                    st.write("Videos, Comments and Replies Extracted")
                    json_data = {'channel': st.session_state.channel_data, 'playlists': st.session_state.playlist_data, \
                                'videos': st.session_state.video_data, 'comments': st.session_state.comments_data, 'replies': st.session_state.reply_data}
                    st.success("Data Extracted successfully from the given YouTube Channel")
                    st.json(json_data, expanded = False)
                except:
                    st.error(sys.exc_info())
                    st.error("Please provide correct channel Id")
            else:
                st.warning("Please provide some Channel Id")

    elif option==s2:
        #st.text("Storing Data to MongoDb...")
        if st.session_state.channel_data:
            create_sqldatabase()
            create_sqlschema()
            st.session_state.channel_name = store_mongodb(st.session_state.channel_data, st.session_state.playlist_data, \
                                                        st.session_state.video_data, st.session_state.comments_data, st.session_state.reply_data, dbConn)
        else:
            st.error("Complete the Data Extraction before storing it into MongoDb")

    elif option==s3:
        st.text("Migrating Data to SQL...")
        sql_migration(dbConn)

    elif option==s4:
        chnl_nms = sql_channel_list()
        if chnl_nms:
            sql_querylist()
        else:
            st.info("No Channel Details available in SQL Db. Complete the previous steps.")

# Main Function 
def main():
    APIKEY = "Your API Key"
    if 'youtube' not in st.session_state:
        st.session_state.youtube = youtube_connection(APIKEY)
    if 'dbConn' not in st.session_state:
        st.session_state.dbConn = pymongo.MongoClient("mongodb+srv://pkmathanraj:86l6HxBwkTLmIlOM@utube.n0xwb3j.mongodb.net/?retryWrites=true&w=majority")
    
    front_end_design(st.session_state.youtube,st.session_state.dbConn)
    
if __name__ == "__main__":
    main() 
