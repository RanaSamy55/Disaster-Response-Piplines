# import libraries
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


def load_data(messages_filepath, categories_filepath):
    
    """
    Load Function
    
    Arguments
        messages_filepath   -->  path to message csv file
        categories_filepath -->  path to message csv file
    output
        df                  -->   Loaded dataframe (Merged)
    """
    df_messages = pd.read_csv(messages_filepath)
    df_categories = pd.read_csv(categories_filepath)
    df = df_messages.merge(df_categories, on=('id'))
    print ('messages and categories data been loaded successfully and merged')
    return df


def clean_data(df):
     
    """
    Clean data     
    Arguments
        df   -->  Dataframe to be cleaned
    output
        df  -->   Clean df
    """
    df_cat=df.categories.str.split(";",expand=True,)
    row = df_cat.iloc[0,:]
    category_colnames = row.apply(lambda x:x[:-2])
    df_cat.columns = category_colnames
    for column in df_cat:
        df_cat[column] = df_cat[column].str[-1]
        df_cat[column] = df_cat[column].astype(np.int)
    df.drop(['categories'],axis=1,inplace=True)
    df = pd.concat([df ,df_cat],axis=1)
    df=df.drop_duplicates()
    genre=pd.DataFrame(pd.get_dummies(df['genre']))
    df = pd.concat([df ,genre],axis=1)
    df.drop(['genre'],axis=1,inplace=True)
    return df


def save_data(df, database_filename):         
    """
    Load to Database    
    Arguments
        df   -->  Dataframe to be saved
        database_filename  --> destination database .db

    """
    engine = create_engine('sqlite:///'+database_filename)
    df.to_sql('dftab', engine, index=False)
    pass  


def main():
    """
    Main Processing data function includes 3 ETL Pipelines:
    - Load files to data frame
    - clean data
    - save data to DB
    """
    
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)
        
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)
        
        print('Cleaned data saved to database!')
    
    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()
