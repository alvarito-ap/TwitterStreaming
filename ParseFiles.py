import pandas as pd 
import os

saveIteration = len( [file for file in os.listdir('Extraction') if not file.startswith('.')] )
df = pd.read_csv("Extraction/Extraction_{}.csv".format(saveIteration))

# Extract Users
users = df.loc[:, ['user_name', 'name']].drop_duplicates()
users.to_csv("Upload/users_{}.csv".format(saveIteration), header = True, index = False)

# Extract tweets
tweets = df.loc[:, ['user_name', 'text', 'place_name', 'country']]
tweets.to_csv("Upload/tweets_{}.csv".format(saveIteration), header = True, index = False)





