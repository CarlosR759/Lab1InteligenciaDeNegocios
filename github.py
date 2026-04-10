import sqlite3
import time
import pandas as pd
import numpy as np
import os
import requests


token = os.environ.get('githubScraper')

connection = sqlite3.connect('github.db')
cursor = connection.cursor()

df = pd.read_csv('githubRepos.csv')


df = df.drop(columns=['Watchers Count', 'Size (KB)']) #Drop useless columns


#Conversion of values into bool for incoming db insertion
df['Has Wiki'] = df['Has Wiki'].astype(bool)
df['Has Pages'] = df['Has Pages'].astype(bool)
df['Has Projects'] = df['Has Projects'].astype(bool)
df['Stars Count'] = df['Stars Count'].astype(int)
df['Forks Count'] = df['Forks Count'].astype(int)
df['Open Issues Count'] = df['Open Issues Count'].astype(int)
df = df[df['Default Branch'].isin(['main', 'master'])]


#Create a column to add repo url //Data enrichment
df['Repo URL'] = df.apply(lambda row: f"https://github.com/{row['Full Name']}", axis=1)

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

#ratio used to track the conversion of stars into forks, to see how many people are interested in the project and how many actually use it.
df['Conversion Rate'] = df.apply(lambda row: round((row['Forks Count'] / row['Stars Count']), 2) if row['Stars Count'] > 0 else 0, axis=1)

########################DEV.to scraping#############################
df['Dev.to Articles'] = np.nan #Init new column to add new data
df['Dev.to Reactions'] = np.nan #Init new column to add new data
def get_devto_impact(df):
    for index, row in df.iterrows():
        repo_name = row['Repository Name']
        valid_tag = repo_name.lower().replace(" ", "").replace(".", "").replace("-", "").replace("_", "")
        api_url = f"https://dev.to/api/articles?tag={valid_tag}&per_page=1000"
        try:
            response = requests.get(api_url)
            response.raise_for_status()
            articles = response.json()
            cant_articles = len(articles)

            if cant_articles > 0:
                total_reactions = sum(article['positive_reactions_count'] for article in articles)
            else:
                total_reactions = 0

            df.at[index, 'Dev.to Articles'] = cant_articles if cant_articles > 0 else 0
            df.at[index, 'Dev.to Reactions'] = total_reactions if total_reactions > 0 else 0

            time.sleep(0.1)
        except Exception as e:
            print(f"Error fetching data for {repo_name}: {e}")
            df.at[index, 'Dev.to Articles'] = 0
            df.at[index, 'Dev.to Reactions'] = 0

    return df

df = get_devto_impact(df)
df['Dev.to Articles'] = df['Dev.to Articles'].fillna(0).astype(int)
df['Dev.to Reactions'] = df['Dev.to Reactions'].fillna(0).astype(int)


########################Github Scrapper#############################
df['Top Contributor'] = "" #Init new column to add new data
def get_top_contributor(df, token):
    headers = {'Authorization': f'token {token}'}
    for index, row in df.iterrows():
        full_name = row['Full Name'] #Extraction of repo name to add in url
        api_url = f"https://api.github.com/repos/{full_name}/contributors"

        try:
            #Token inclusion into header file and request
            response = requests.get(api_url, headers=headers, timeout=30)
            response.raise_for_status()
            contributors = response.json()
            # The API returns contributors sorted by number of commits (descending), so we pick the first one
            top_contributor = contributors[0] if contributors else None
            df.at[index, 'Top Contributor'] = top_contributor['login'] if top_contributor else None
        except requests.exceptions.HTTPError as http_err:
           if response.status_code == 404:
               print(f"404 - Repo not found: {full_name}")
               df.at[index, 'Top Contributor'] = None
           elif response.status_code == 403:
               print(f"403 - Access forbidden: {full_name}")
               df.at[index, 'Top Contributor'] = None
           else:
               print(f"Maybe a Teapot buddy ? {full_name}: {http_err}")
               df.at[index, 'Top Contributor'] = None

        except requests.exceptions.ConnectionError:
            print(f"Connection error for {full_name}")
            df.at[index, 'Top Contributor'] = None
        except requests.exceptions.Timeout:
            print(f"Timeout for {full_name}")
            df.at[index, 'Top Contributor'] = None
        except Exception as err:
            print(f"Unexpected error for {full_name}: {err}")
            df.at[index, 'Top Contributor'] = None

    return  df

df = get_top_contributor(df, token)

#owners table
df_owners = df[['Owner Login', 'Owner Type']].drop_duplicates()

#number of repos by owner
repos_count = df['Owner Login'].value_counts().reset_index()
repos_count.columns = ['Owner Login', 'Total Repos']

#merge the number of repos with the owners table, data enrichment
df_owners = pd.merge(df_owners, repos_count, on='Owner Login', how='left')
df_owners.columns = df_owners.columns.str.lower().str.replace(' ', '_')

#repos table
df_repos = df.drop(columns=['Owner Type'])
df_repos.columns = df_repos.columns.str.lower().str.replace(' ', '_').str.replace('.', '')


#drop tables if they exist to avoid errors when creating them again, this is useful for development, but in production we should use migrations to avoid data loss
cursor.execute("PRAGMA foreign_keys = ON;") #Enable foreign keys for db
cursor.execute("DROP TABLE IF EXISTS repositories;")
cursor.execute("DROP TABLE IF EXISTS owners;")

#create owners table, PK owner_login
cursor.execute("""
CREATE TABLE owners (
    owner_login TEXT PRIMARY KEY,
    owner_type TEXT,
    total_repos INTEGER
);
""")

#create repositories table, PK full_name, FK owner_login
cursor.execute("""
CREATE TABLE repositories (
    domain TEXT,
    repository_name TEXT,
    full_name TEXT PRIMARY KEY,
    description TEXT,
    primary_language TEXT,
    stars_count INTEGER,
    forks_count INTEGER,
    open_issues_count INTEGER,
    has_wiki BOOLEAN,
    has_pages BOOLEAN,
    has_projects BOOLEAN,
    created_at TEXT,
    updated_at TEXT,
    pushed_at TEXT,
    default_branch TEXT,
    owner_login TEXT,
    license TEXT,
    topics TEXT,
    repo_url TEXT,
    is_open_source BOOLEAN,
    conversion_rate REAL,
    devto_articles INTEGER,
    devto_reactions INTEGER,
    top_contributor TEXT,
    FOREIGN KEY(owner_login) REFERENCES owners(owner_login)
);
""")


#insert data into db
df_owners.to_sql('owners', connection, if_exists='replace', index=False)
df_repos.to_sql('repositories', connection, if_exists='replace', index=False)

#save changes  and close for db
connection.commit()
connection.close()
