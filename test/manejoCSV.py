import pandas as pd
import numpy as np
import pymssql as db
import sqlalchemy as sal
from sqlalchemy import create_engine as sen 
from sqlalchemy.sql import text as sat

import os

dir = '/home/freya/Code/Python/Pandas/files/{}'
file = dir.format('test.csv')
print(file)

dict_data = {
    'edad': [10, 9, 13, 14, 12, 11, 12],
    'cm': [115, 110, 130, 155, 125, 120, 125],
    'pais': ['co', 'mx', 'co', 'mx', 'mx', 'ch', 'ch'],
    'genero': ['M', 'F', 'F', 'M', 'M', 'M', 'F'],
    'Q1': [5, 10, 8, np.nan, 7, 8, 3],
    'Q2': [7, 9, 9, 8, 8, 8, 9]
}
df = pd.DataFrame(dict_data)
# , index=['Ana','Benito', 'Camilo', 'Daniel', 'Erika', 'Fabian', 'Gabriela']
print("""
Almacenar datos del diccionario en un CSV
""")
df.to_csv(file, index=False, sep='|')

print("""
Leer datos del diccionario en un CSV
""")
dfRead = pd.read_csv(file, sep='|')
print(dfRead)

file = dir.format('test.json')
df.to_json(file)

dfJson = pd.read_json(file)
print(dfJson)


file = dir.format('test.parquet')
df.to_json(file)

dfJson = pd.read_json(file)
print(dfJson)


engine = sal.create_engine(con)

AllSelectFrom = 'select * from {}.{};'
TruncateTable = 'TRUNCATE TABLE {}.{};'

dfRead.to_sql(
    name=table,
    con=engine,
    if_exists='append',
    index=False
)


# df = pd.read_sql(AllSelectFrom.format(schema,table), engine)
# print(df)

result = engine.execute(
    sat(
        TruncateTable.format(schema, table)
    ).execution_options(autocommit=True)
)

print(result)

