import sqlite3
import pandas as pd

connection = sqlite3.connect('github.db')
cursor = connection.cursor()

df = pd.read_csv('github.csv')

print(df)

#save changes  and close
connection.commit()
connection.close()
