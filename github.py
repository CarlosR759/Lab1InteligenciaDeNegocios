import sqlite3
import pandas as pd
import numpy as np
import os
import requests


token = os.environ.get('githubScraper')

connection = sqlite3.connect('github.db')
cursor = connection.cursor()

df = pd.read_csv('githubRepos.csv')
df = df.drop(columns=['Forks Count', 'Size (KB)']) #Drop useless columns


#We get a list of owners without duplication
owners_fetch = df['Owner Login'].tolist()
owners = df['Owner Login'].unique()
print(owners.value_counts())


#Conversion of values into bool for incoming db insertion
df['Has Wiki'] = df['Has Wiki'].astype(bool)
df['Has Pages'] = df['Has Pages'].astype(bool)
df['Has Projects'] = df['Has Projects'].astype(bool)
df['Stars Count'] = df['Stars Count'].astype(int)
#df['Forks Count'] = df['Forks Count'].astype(int)
df['Watchers Count'] = df['Watchers Count'].astype(int)
df['Open Issues Count'] = df['Open Issues Count'].astype(int)
df = df[df['Default Branch'].isin(['main', 'master'])]
#not_main_branches = df[~df['Default Branch'].isin(['main', 'master'])]
#print(not_main_branches[['Repository Name', 'Default Branch']])


#Create a column to add repo url //Data enrichment
df['Repo URL'] = df.apply(lambda row: f"https://github.com/{row['Full Name']}", axis=1)
#print(df['Repo URL'])


#licenses = df['License'].unique()
#print(licenses)

#List of open source licences to make boolean column for each repo
open_source_licenses = [
    'Apache License 2.0',
    'Creative Commons Zero v1.0 Universal',
    'MIT License',
    'GNU General Public License v3.0',
    'BSD 3-Clause "New" or "Revised" License',
    'GNU Affero General Public License v3.0',
    'Creative Commons Attribution Share Alike 4.0 International',
    'Mozilla Public License 2.0',
    'Boost Software License 1.0',
    'GNU Lesser General Public License v3.0',
    'The Unlicense',
    'BSD 2-Clause "Simplified" License',
    'Creative Commons Attribution 4.0 International',
    'GNU General Public License v2.0',
    'Do What The F*ck You Want To Public License',
    'GNU Lesser General Public License v2.1',
    'ISC License',
    'Artistic License 2.0',
    'European Union Public License 1.2',
    'zlib License'
]

df['Is Open Source'] = df['License'].apply(lambda x: x in open_source_licenses if pd.notnull(x) else False)


#Debug lines:
#owner_repetitions = df['Owner Login'].value_counts()
#print(owners_fetch)
#print(owner_repetitions) #This shows that a lot of organizations repeats in repos. Time to clean that.
#print(owners)
#print(df[['Repository Name', 'Is Open Source']])

########################Github Scrapper#############################
df['Top Contributor'] = np.nan #Init new column to add new data
def get_top_contributor(df, token):
    for index, row in df.iterrows():
    #for index, row in df.head(50).iterrows(): #Debug line, for complete scraping delete this and uncoment the previous one
        full_name = row['Full Name'] #Extraction of repo name to add in url
        api_url = f"https://api.github.com/repos/{full_name}/contributors"

        #Token inclusion into header file and request
        headers = {}
        headers['Authorization'] = f'token {token}'
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        contributors = response.json()

        # The API returns contributors sorted by number of commits (descending), so we pick the first one
        top_contributor = contributors[0] if contributors else None
        df.at[index, 'Top Contributor'] = top_contributor['login'] if top_contributor else None

    return  df

df = get_top_contributor(token, df)
print(df[['Repository Name', 'Top Contributor']])


#save changes  and close for db
connection.commit()
connection.close()

#print(token)
