import tweepy
import time
import pandas as pd
import os
import json

with open("params.json") as file:
    params = json.load(file)
    CONSUMER_KEY = params['CONSUMER_KEY']
    CONSUMER_SECRET = params['CONSUMER_SECRET']
    ACCESS_TOKEN = params['ACCESS_TOKEN']
    ACCESS_TOKEN_SECRET = params['ACCESS_TOKEN_SECRET']

FILTERS = ['vox', 'podemos', 'psoe', 'pp', 'ciudadanos', 'votar', 'politica', 'sanchez', 'iglesias', 'montero', 'abascal', 'casado', 'arrimadas', 'gobierno', 'parlamento',
             'congreso', 'facha','fachas', 'derecha', 'izquierda']

class MyStreamListener(tweepy.StreamListener):

    """
        @params
        *specialWords: list the terms that must be conained, needs to be implemented this way since Streaming API doesnt allow filterin by location and Key-Words at the same tiem
        t: time determines the time while the listener will be capturing events
    """

    def __init__(self, *specialWords, t = 100):
        
        super(MyStreamListener,self).__init__()
        self.maxTime = time.time() + t
        self.result = pd.DataFrame(columns=['user_name', 'name', 'text', 'country', 'place_name']) # user_name screen_name
        self.specialWords = specialWords

    
    def filter(self, text):

        for word in text.lower().split():
            if(word in self.specialWords):
                return True
                
        return False
    
    def on_status(self, status):

        if(self.maxTime < time.time()):
            print(self.result)
            print(self.result.iloc[0,2])

            num = len( [file for file in os.listdir('Extraction') if not file.startswith('.')] ) + 1
            self.result.to_csv("Extraction/Extraction_{}.csv".format(num), header=True, index = False)

            self.result = pd.DataFrame(columns=['user_name', 'name', 'text', 'country', 'place_name'])
            print("END!!!")
        
        if(status.place is not None):

            if (status.place.country_code != None and status.place.name != None and self.filter(status.text)):

                self.result = self.result.append({
                    'user_name': status.author.screen_name,
                    'name' : status.author.name,
                    'text' : status.text,
                    'country' : status.place.country_code,
                    'place_name' : status.place.name
                }, ignore_index = True)

                print("New tweet added")
    
    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False

        # returning non-False reconnects the stream, with backoff.

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)


myStreamListener = MyStreamListener(t=3600*3, *FILTERS)
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

# filter location bottom-left to top-right
myStream.filter(languages = ['es', 'en'], locations = [-9.95, 36.53, 3.73, 42.85])


