import numpy as np
import pandas as pd
import pymssql as db
import sqlalchemy as sal
from sqlalchemy import create_engine as sen
from sqlalchemy.sql import text as sat

dict_data = {
    'edad': [10, 9, 13, 14, 12, 11, 12],
    'cm': [115, 110, 130, 155, 125, 120, 125],
    'pais': ['co', 'mx', 'co', 'mx', 'mx', 'ch', 'ch'],
    'genero': ['M', 'F', 'F', 'M', 'M', 'M', 'F'],
    'Q1': [5, 10, 8, np.nan, 7, 8, 3],
    'Q2': [7, 9, 9, 8, 8, 8, 9]
}


def stringConnect():
    server = 'localhost'
    port = '1433'
    db = 'testingPython'
    user = 'sa'
    password = '4dm1nPr0c'

    stringConnect = 'mssql+pymssql://{}:{}@{}:{}/{}'
    return stringConnect.format(
        user,
        password,
        server,
        port,
        db
    )


def engineCon(con):
    return sal.create_engine(con)


def loadData():
    return pd.DataFrame(dict_data)


def insertDataToSql(data, schema, table):
    data.to_sql(
        schema=schema,
        name=table,
        con=engineCon(stringConnect()),
        if_exists='append',
        index=False
    )


def readDataToSql(schema, table):
    AllSelectFrom = 'SELECT * FROM {}.{};'
    return pd.read_sql(
        AllSelectFrom.format(schema, table),
        engineCon(stringConnect())
    )


def truncateTable(schema, table):
    TruncateTable = 'TRUNCATE TABLE {}.{};'
    engineCon(stringConnect()).execute(
        sat(
            TruncateTable.format(schema, table)
        ).execution_options(autocommit=True)
    )

# Pruebas con session para el manejo de commit 
# def truncateTable(schema, table):
#     TruncateTable = 'TRUNCATE TABLE {}.{};'
#     session = sessionmaker(bind=engineCon(stringConnect()))
#     session = session()
#     session.execute(TruncateTable.format(schema, table))
#     session.commit()
#     session.close()


def run():

    table = 'testData'
    schema = 'dbo'

    data = loadData()
    insertDataToSql(data, schema, table)
    print(readDataToSql(schema, table))
    truncateTable(schema, table)


if __name__ == "__main__":
    run()
