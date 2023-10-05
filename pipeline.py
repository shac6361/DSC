import argparse
import pandas as pd
import numpy as np

def read_args():
    print('In function read_args()\nParsing arguments')
    argParser = argparse.ArgumentParser()
    argParser.add_argument("-i", "--input", help="<input filename>")
    argParser.add_argument("-o", "--output", help="<output filename>")
    #print('Now returning to main()')
    return argParser.parse_args()

def read_data(file):
    print('In function read_data()\nReading input file')
    #print('Now returning to main()')
    return pd.read_csv(file)
    
def process_data(df):   
    print('In function process_data()\nNormalizing numeric columns')
    df[df.select_dtypes(include=np.number).columns] = df.select_dtypes(include=np.number).apply(lambda x: (x - x.mean()) / x.std())
    #print('Now returning to main()')
    return df

def write_data(file, df):
    print('In function write_data()\nWriting output file')
    df.to_csv(file, index=False)
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
    print("args.output=%s" %args.output)
    
    #print('Calling read_data()')
    df = read_data(args.input)
    print('Before processing data:')
    #print('Calling print_data()')
    print_data(df)
    #print('Calling process_data()')
    df = process_data(df)
    print('After processing data:')
    #print('Calling print_data()')
    print_data(df)
    #print('Calling write_data()')
    write_data(args.output,df)
