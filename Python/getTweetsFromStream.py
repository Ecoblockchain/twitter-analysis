#! /usr/bin/env python
# -*- coding: utf-8 -*-

from twython import TwythonStreamer
from tinydb import TinyDB
from slugify import slugify
from threading import Thread
from Queue import Queue
from time import time, sleep
from sys import exit
from re import sub

## What to search for
SEARCH_TERMS = ["#jesus", "#pizza", "@thiagohersan"]

class TwitterStreamReceiver(TwythonStreamer):
    def __init__(self, *args, **kwargs):
        super(TwitterStreamReceiver, self).__init__(*args, **kwargs)
        self.tweetQ = Queue()
    def on_success(self, data):
        if ('text' in data):
            thisTweet = {
                'id': data['id_str'],
                'user': data['user']['id_str'],
                'text': data['text'].encode('utf-8'),
                'hashtags': [h['text'].encode('utf-8') for h in data['entities']['hashtags']],
                'mentions': [u['id_str'] for u in data['entities']['user_mentions']]
            }
            print "%s\n"%thisTweet
            myDB.insert(thisTweet)
            #self.tweetQ.put(data['text'].encode('utf-8'))
    def on_error(self, status_code, data):
        print status_code
    def empty(self):
        return self.tweetQ.empty()
    def get(self):
        return self.tweetQ.get()

def setup():
    global myTwitterStream, streamThread, myDB

    ## read secrets from file
    secrets = {}
    with open('oauth.txt', 'r') as oauthFile:
        for line in oauthFile:
            (k,v) = line.split()
            secrets[k] = v
        oauthFile.close()

    ## start Twitter stream reader
    myTwitterStream = TwitterStreamReceiver(app_key = secrets['CONSUMER_KEY'],
                                            app_secret = secrets['CONSUMER_SECRET'],
                                            oauth_token = secrets['OAUTH_TOKEN'],
                                            oauth_token_secret = secrets['OAUTH_SECRET'])
    streamThread = Thread(target=myTwitterStream.statuses.filter,  kwargs={'track':','.join(SEARCH_TERMS)})
    streamThread.start()

    ## start db
    fileName = slugify(" ".join(SEARCH_TERMS)).replace('-',' ').title().replace(' ','')
    myDB = TinyDB('../data/%s.json'%fileName)

def cleanText(text):
    ## removes re-tweet
    text = sub(r'(^[rR][tT] )', '', text)
    ## removes hashtags, mentions and links
    text = sub(r'(#\S+)|(@\S+)|(http://\S+)|(https://\S+)', '', text)
    ## removes punctuation
    text = sub(r'[.,;:!?*+=\-&%^/\\_$~()<>{}\[\]]', ' ', text)
    ## replaces double-spaces with single space
    text = sub(r'( +)', ' ', text)

if __name__=="__main__":
    setup()
    try:
        ## to catch keyboard interrupts and other exceptions
        while(True):
            ## keep it from looping faster than ~10 times per second
            sleep(0.1)
    except KeyboardInterrupt :
        myTwitterStream.disconnect()
        streamThread.join()
        exit(0)
