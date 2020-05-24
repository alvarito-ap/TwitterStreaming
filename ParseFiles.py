import pandas as pd 
import os


df = pd.read_csv("Extraction/Extraction1.csv")
saveIteration = len( [file for file in os.listdir('Extraction') if not file.startswith('.')] ) / 2

# Extract Users
users = df.loc[:, ['user_name', 'name']].drop_duplicates()
users.to_csv("Upload/users_{}.csv".format(saveIteration), header = True, index = False)

# Extract tweets
tweets = df.loc[:, ['user_name', 'text', 'place_name', 'country']]
tweets.to_csv("Upload/tweets_{}.csv".format(saveIteration), header = True, index = False)





