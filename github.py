import sqlite3
import pandas as pd

connection = sqlite3.connect('github.db')
cursor = connection.cursor()

df = pd.read_csv('githubRepos.csv')
df = df.drop(columns=['Forks Count', 'Size (KB)']) #Drop useless columns

#Create a column to add repo url //Data enrichment
df['Repo URL'] = df.apply(lambda row: f"https://github.com/{row['Full Name']}", axis=1)
print(df['Repo URL'])

#We get a list of owners without duplication
owners_fetch = df['Owner Login'].tolist()
owners = df['Owner Login'].unique()
print(owners.value_counts())


#Conversion of values into bool for incoming db insertion
df['Has Wiki'] = df['Has Wiki'].astype(bool)
df['Has Pages'] = df['Has Pages'].astype(bool)
df['Has Projects'] = df['Has Projects'].astype(bool)
df['Stars Count'] = df['Stars Count'].astype(int)
df['Forks Count'] = df['Forks Count'].astype(int)
df['Watchers Count'] = df['Watchers Count'].astype(int)
df['Open Issues Count'] = df['Open Issues Count'].astype(int)

#Debug lines:
#owner_repetitions = df['Owner Login'].value_counts()
#print(owners_fetch)
#print(owner_repetitions) #This shows that a lot of organizations repeats in repos. Time to clean that.
#print(owners)



#save changes  and close
connection.commit()
connection.close()
