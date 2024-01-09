import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, TIMESTAMP, TEXT
from helper.constants import AWS, POSTGRES, NYTIMES_API


def extract_and_save_to_s3():
    # Form API url and request data from the url
    api_url = NYTIMES_API['api'] + NYTIMES_API['key']
    response = requests.get(api_url)
    response_data = response.json()
    results = response_data['results']

    # Collect required data
    if results is not None:
        data = []
        for res in results:
            data_dict = {}
            data_dict['slug_name'] = res['slug_name']
            data_dict['section'] = res['section']
            data_dict['subsection'] = res['subsection']
            data_dict['title'] = res['title']
            data_dict['abstract'] = res['abstract']
            data_dict['uri'] = res['uri']
            data_dict['url'] = res['url']
            data_dict['byline'] = res['byline']
            data_dict['item_type'] = res['item_type']
            data_dict['source'] = res['source']
            data_dict['updated_date'] = res['updated_date']
            data_dict['created_date'] = res['created_date']
            data_dict['published_date'] = res['published_date']
            data_dict['first_published_date'] = res['first_published_date']
            data_dict['material_type_facet'] = res['material_type_facet']

            data.append(data_dict)
        
        
        print('Number of records added: ', len(data))
        
        # Create dataframe and save it as a parquet file in S3
        df = pd.DataFrame(data)
        output_filename = 'nytimes_world_data'

        # AWS credentials info
        storage_options={
        "key": AWS['aws_access_key_id'],
        "secret": AWS['aws_secret_access_key'],}
        df.to_parquet('s3://airflow-poc-datasets/nytimes/delta/{}.parquet'.format(output_filename), storage_options=storage_options, index = False)
        print('Dataframe saved as parquet file in s3 successfully.')
    else:
        print('No new data found.')
        return 0
    

def load_parquet_to_stg_tbl():
    # AWS credentials info
    storage_options={
        "key": AWS['aws_access_key_id'],
        "secret": AWS['aws_secret_access_key'],}
    
    # Read parquet file
    output_filename = 'nytimes_world_data'
    df = pd.read_parquet('s3://airflow-poc-datasets/nytimes/delta/{}.parquet'.format(output_filename), storage_options=storage_options)

    # Define datatype mapping for columns
    dtype_mapping = {
        'slug_name': VARCHAR,
        'section': VARCHAR,
        'subsection': VARCHAR,
        'title': TEXT,
        'abstract': TEXT,
        'uri': VARCHAR,
        'url': VARCHAR,
        'byline': VARCHAR,
        'item_type': VARCHAR,
        'source': VARCHAR,
        'updated_date': TIMESTAMP,
        'created_date': TIMESTAMP,
        'published_date': TIMESTAMP,
        'first_published_date': TIMESTAMP,
        'material_type_facet': VARCHAR,
    }

    # Replace these with your RDS PostgreSQL details
    username = POSTGRES['username']
    password = POSTGRES['password']
    host = POSTGRES['host']
    port = POSTGRES['port']
    database = POSTGRES['database']

    # Create a connection string to connect to your PostgreSQL database
    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

    # Write DataFrame to PostgreSQL table
    table_name = "stg01_nytimes_world"  
    df.to_sql(table_name, engine, if_exists='replace', schema="public", dtype=dtype_mapping, index=False)

    # Close connection
    engine.dispose()

    print('data written to postgres db')
