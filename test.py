import os

import datetime

#print(datetime.datetime(2013, 11, 5, 20, 24, 51))

import pandas as pd
import csv
import json
import elasticsearch as elas
from elasticsearch.helpers import bulk



a=662
print(type(a))
str(a)
print(type(a))

index_name = 'titanic'
type_name = 'passenger'
id_field = 'passengerid'

final=[]
titanic_df = pd.read_csv("8.csv").fillna('')
titanic_records = titanic_df.to_dict(orient='records')
es=elas.Elasticsearch([{'host': 'localhost', 'port': 9200}])

if not es.indices.exists(index_name):
    es.indices.create(index_name)

actions = []
elasbulk={}
i=0
for i,r in enumerate(titanic_records):
    actions.append({"_index": index_name,
                    "_type": type_name,

                    "_source": r})
    i+=1
    print(i)
print('ok')
bulk(es, actions=actions,index=index_name, doc_type=type_name, refresh=False)
print('ok')
es.indices.refresh(index=index_name)

