from bs4 import BeautifulSoup
from datetime import datetime
import facebook
import feedparser
import json
import logging
import os
import sqlite3
from time import mktime
import urllib.request

#setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fhandler = logging.FileHandler('fb_feed.log')
fhandler.setLevel(logging.INFO)
shandler = logging.StreamHandler()
shandler.setLevel(logging.INFO)

logger.addHandler(fhandler)
logger.addHandler(shandler)

#load config
config = json.load( open('facebook.json') )

#Check if images dir exists
images_dir = "images/"
if not os.path.exists(images_dir):
    logger.info("making image dir")
    os.makedirs(images_dir)

#check if db exists
dbFile = "fb_feed.db"
dbExists = False

if os.path.isfile(dbFile):
    logger.info("database exists")
    dbExists = True

db = sqlite3.connect(dbFile)

#make db
cursor = db.cursor()
if not dbExists :
    logger.info("making database")
    cursor.execute('''
    CREATE TABLE channel(id INTEGER PRIMARY KEY, name TEXT, title TEXT, url TEXT
                       lastBuildDate TEXT)
    ''')
    cursor.execute('''
    CREATE TABLE item(id INTEGER PRIMARY KEY, channel INTEGER, guid TEXT,
                       pubDate TEXT)
    ''')
    db.commit()

#get fb tokens
token = config['fb_app_token']
paget = config['fb_page_token']

#connect to fb graph
graph = facebook.GraphAPI(paget)

#check each RSS feed
for feed in config['feeds']:
    
    #open feed
    logger.info("connecting to "+feed['name']+" feed:"+feed['url'])
    rss = feedparser.parse(feed['url'])
    num_items = 0

    #if channel doesn't exist, create it
    logger.info("feed is:"+rss['feed']['title'])
    cursor.execute("SELECT id, name, title FROM channel WHERE name = ?",(feed['name'],))
    all_rows = cursor.fetchall()

    if (len(all_rows) <=0):
        logger.info("creating feed "+feed['name']+": "+rss['feed']['title']+" in database")
        cursor.execute('''INSERT INTO channel(name,title,url)
        VALUES(?,?,?)''', (feed['name'],rss['feed']['title'],feed['url']) )
        channel_id = cursor.lastrowid
        db.commit()
    else:
        channel_id = all_rows[0][0]
    #post a link to the article on the website
    if(feed['type'] == 'link'):
        logger.info("posting links")
        for item in rss['entries']:
            #limit number of posts each update
            if num_items >= feed['max_posts']:
                break
            
            itemDate = datetime.fromtimestamp(mktime(item['published_parsed']))

            #skip if already published
            cursor.execute("SELECT id, channel, guid, pubDate FROM item WHERE guid = ? AND pubDate = ?",(item['id'],itemDate.isoformat()))
            item_rows = cursor.fetchall()
            if(len(item_rows) > 0):
                continue
            
            #post a link
            num_items += 1
            try:
                graph.put_object(
                   parent_object="me",
                   connection_name="feed",
                   message="",
                   link=item['link'])
                logger.info("    posted: "+item['link'])

                #update database
                cursor.execute('''INSERT INTO item(channel, guid, pubDate)
                    VALUES(?,?,?)''', (channel_id, item['id'], itemDate.isoformat()) )
                db.commit()
            except Exception as e:
                logger.error( e )
        pass
    #post an image to the timeline along with the description
    elif(feed['type'] == 'short'):
        logger.info("posting summary")
        for item in rss['entries']:
            #limit number of posts each update
            if num_items >= feed['max_posts']:
                break
            
            itemDate = datetime.fromtimestamp(mktime(item['published_parsed']))

            #skip if already published
            cursor.execute("SELECT id, channel, guid, pubDate FROM item WHERE guid = ? AND pubDate = ?",(item['id'],itemDate.isoformat()))
            item_rows = cursor.fetchall()
            if(len(item_rows) > 0):
                continue
            
            #download image
            soup = BeautifulSoup(item['summary'],features="html.parser")
            img_url = soup.find('img')['src']
            filename = img_url[img_url.rfind("/")+1:]
            logger.info("saving image " + filename)
            urllib.request.urlretrieve(img_url, images_dir+filename)
            
            #post a photo
            num_items += 1
            try:
                graph.put_photo(image=open(images_dir+filename, 'rb'),
                    message=item['summary'])
                logger.info("    posted: "+item['summary'][0,50]+ "...")

                #update database
                cursor.execute('''INSERT INTO item(channel, guid, pubDate)
                    VALUES(?,?,?)''', (channel_id, item['id'], itemDate.isoformat()) )
                db.commit()
            except Exception as e:
                logger.error( e )

            #delete image
            os.remove(images_dir+filename)
        pass
    #post an image to the timeline along with the content-encoded
    elif(feed['type'] == 'long'):
        logger.info("posting content-encoded")
        for item in rss['entries']:
            #limit number of posts each update
            if num_items >= feed['max_posts']:
                break
            
            itemDate = datetime.fromtimestamp(mktime(item['published_parsed']))

            #skip if already published
            cursor.execute("SELECT id, channel, guid, pubDate FROM item WHERE guid = ? AND pubDate = ?",(item['id'],itemDate.isoformat()))
            item_rows = cursor.fetchall()
            if(len(item_rows) > 0):
                continue
            
            #download image
            soup = BeautifulSoup(item['summary'],features="html.parser")
            img_url = soup.find('img')['src']
            filename = img_url[img_url.rfind("/")+1:]
            logger.info("saving image " + filename)
            urllib.request.urlretrieve(img_url, images_dir+filename)
            
            #post a photo
            num_items += 1
            try:
                graph.put_photo(image=open(images_dir+filename, 'rb'),
                    message=item['content'][0]['value'])
                logger.info("    posted: "+item['content'][0]['value'][0,50]+ "...")

                #update database
                cursor.execute('''INSERT INTO item(channel, guid, pubDate)
                    VALUES(?,?,?)''', (channel_id, item['id'], itemDate.isoformat()) )
                db.commit()
            except Exception as e:
                logger.error( e )
                
            #delete image
            os.remove(images_dir+filename)
        pass

logger.info("Shutting down")
db.close()
