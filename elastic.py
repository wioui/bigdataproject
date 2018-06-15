import csv
import os
import datetime
import json
import glob
import requests
import elasticsearch as elas
from bson import json_util
import Mongodb
from elasticsearch.helpers import bulk
import time
import pandas as pd

## connexion to elasticsearch
def connexion():
    print(datetime.datetime.now())
    return elas.Elasticsearch([{'host': 'localhost', 'port': 9200}])

def create_index(es,index_name):
    print(datetime.datetime.now())
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)

def delete_index(es,index_name):
    es.indices.delete(index=index_name, ignore=[400, 404])

##conversion csv to json
def csv_to_json(csvf,jsonf,fieldnames):
    csvfile = open(csvf, 'r')
    jsonfile = open(jsonf, 'w')
    reader = csv.DictReader(csvfile, fieldnames)
    n=False
    for row in reader:
        if n:
            json.dump(row, jsonfile)
            jsonfile.write('\n')
        n=True


## import all_sites to elasticsearch
def all_sites_to_es(es, index_name, docname,directory):
    print(datetime.datetime.now())
    csvfile = open(directory+'all_sites.csv', 'r')
    reader = csv.DictReader(csvfile)
    header = ["SITE_ID", "INDUSTRY", "SUB_INDUSTRY", "SQ_FT", "LAT", "LNG", "TIME_ZONE", "TZ_OFFSET"]

    for each in reader:
        row = {}
        L=[]
        for field in header:

            row[field] = each[field]

        es.index(index=index_name, doc_type=docname, body=row)
        es.indices.refresh(index=index_name)


## import all_datas_sites to elasticsearch
def all_datas_sites_to_esv2(es,db,directory,index_name, docname):
    print(datetime.datetime.now())
    dbsite = db.enernoc.all_sites
    list_file = Mongodb.list_all_file(directory, "csv")
    actions = []
    for i in range(len(list_file)):
        print(list_file[i])
        data = pd.read_csv(list_file[i]).fillna('') ##lecture du csv
        filename = os.path.split(list_file[i])[1].replace('.csv', "")
        site_id = str(filename)
        site_add = Mongodb.site_id_to_id(dbsite, site_id)
        a = [site_add] * len(data)
        data['SITE'] = a
        geopoint=(str(data['SITE'][0]['LAT'])+", "+str(data['SITE'][0]['LNG'])) ## concatenation lat + lng pour mapping Kibana
        data['geo_point']=geopoint
        data['date'] = pd.to_datetime(data['timestamp'], unit='s')

        data_records = data.to_dict(orient='records') ##conversion pour format elasticsearch (dict)

        if not es.indices.exists(index_name):
            es.indices.create(index_name)

        print("Début import Elastic")
        for i, r in enumerate(data_records):
            actions.append({"_index": index_name,
                            "_type": docname,
                            "_source": r})


        bulk(es, actions=actions, index=index_name, doc_type=docname, refresh=False) ## import dans elasticsearch via bulk
    es.indices.refresh(index=index_name)

    #     helpers.parallel_bulk(client=es, actions=actions, thread_count=4, refresh=False)
    # es.indices.refresh(index=index_name)


