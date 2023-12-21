
import requests
from airflow.decorators import dag, task
import datetime
import pendulum
import json
import pandas as pd

@dag(
    dag_id="nytimes_news",
    start_date=pendulum.datetime(2023, 12, 20, tz="local"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def process_nytimes_news():

    @task
    def extract():
        api_url = 'https://api.nytimes.com/svc/news/v3/content/nyt/world.json?api-key=gv4txZ8wLsKZoaOtQ0P0vtpvALB8f2A5'
        response = requests.get(api_url)
        response_data = response.json()
        results = response_data['results']

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
        
        print(data[0])
        print('data count: ', len(data))
        
        df = pd.DataFrame(data)
        ct = datetime.datetime.now()
        output_filename = 'data_' + str(ct.year) + '_' + str(ct.month) + '_' + str(ct.day)
        df.to_csv('/opt/airflow/dags/nytimes_data/{}.csv'.format(output_filename), index = False)
    
    extract()

dag = process_nytimes_news()
         