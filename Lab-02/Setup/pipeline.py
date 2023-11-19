import argparse
import pandas as pd
import numpy as np
import sqlalchemy

def read_args():
    print('In function read_args()\nParsing arguments')
    argParser = argparse.ArgumentParser()
    argParser.add_argument("-i", "--input", help="<input filename>")
    #argParser.add_argument("-o", "--output", help="<output filename>")
    #print('Now returning to main()')
    return argParser.parse_args()

def extract_data(file):
    print('In function extract_data()\nReading input file')
    #print('Now returning to main()')
    return pd.read_csv(file)
    
def transform_data(df):   
    print('In function transform_data()')
    #df['Date'] = df['DateTime'].str.split(' ').str[0]
    df['Reproductive Status upon Outcome'] = df['Sex upon Outcome'].str.split(' ').str[0]
    df['Sex upon Outcome'] = df['Sex upon Outcome'].str.split(' ').str[1]
    df['Sex upon Outcome'].fillna('Unknown',inplace=True)
    df['Reproductive Status upon Outcome'].fillna('Unknown',inplace=True)
    df['Date of Outcome'] = pd.to_datetime(df['DateTime'],format='mixed').dt.date
    df['Date of Birth'] = pd.to_datetime(df['Date of Birth'],format='mixed').dt.date
    df['Years Age upon Outcome'] = ((df['Date of Outcome'] - df['Date of Birth']) / np.timedelta64(365, 'D')).astype('int')
    df.drop(columns=['DateTime','MonthYear', 'Age upon Outcome'],inplace=True)
    df['Outcome Type'].fillna('-',inplace=True)
    df['Outcome Subtype'].fillna('-',inplace=True)
    #print('Now returning to main()')
    return df

def load_data(df):
    db_url = "postgresql+psycopg2://shrestha:hunter2@db:5432/shelter"
    engine = sqlalchemy.create_engine(db_url)
    print('In function load_data()\nWriting output file')
    #df.to_sql("outcomes", conn, if_exists="append", index=False)
    #df.to_sql("outcomes", conn, if_exists="replace", index=False)
    df.to_sql("outcomes", engine, if_exists="replace", index=False)
    with engine.connect() as conn:
        with open("load_data.sql") as file:
           # print('Hi')
            query = sqlalchemy.sql.text(file.read())
            print(query)
            conn.execute(query)
    with engine.connect() as conn:
        with open("sql_queries.sql") as file:
            #print('Hi')
            query = sqlalchemy.sql.text(file.read())
            print(query)
            conn.execute(query)
#            conn.commit()

    #df[['Animal ID','Name','Animal Type','Date of Birth','Breed','Color']].to_sql("dim_animal", conn, if_exists="replace", index=False)
    #df.to_sql("dim_rp_status", conn, if_exists="replace", index=False)
    #print('Now returning to main()')

def print_data(data):
    print(data)
    #print('Now returning to main()')


if __name__ == "__main__":
    print('In function main()')
    #print('Calling read_args()')
    args = read_args()
    #print('Printing arguments received in main()')
    print("args=%s" %args)
    print("args.input=%s" %args.input)
    #print("args.output=%s" %args.output)
    
    #print('Calling read_data()')
    df = extract_data(args.input)
    print(len(df))
    print('Before processing data:')
    #print('Calling print_data()')
    print_data(df)
    #print('Calling process_data()')
    df = transform_data(df)
    print('After processing data:')
    #print('Calling print_data()')
    print_data(df)
    #print('Calling write_data()')
    load_data(df)
