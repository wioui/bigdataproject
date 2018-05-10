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


def connexion():
    print(datetime.datetime.now())
    return elas.Elasticsearch([{'host': 'localhost', 'port': 9200}])

def create_index(es,index_name):
    print(datetime.datetime.now())
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
def delete_index(es,index_name):
    es.indices.delete(index=index_name, ignore=[400, 404])

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

def all_json_to_es(es,directory,myindex,docname):
    print(datetime.datetime.now())
    list_file = Mongodb.list_all_file(directory,"json")

    for i in range(len(list_file)):
        json_to_es(es,list_file[i],myindex,docname)


# def all_sites_to_es(es,db,index_name,docname):
#     print(datetime.datetime.now())
#     content=db.enernoc.all_sites.find({},{"_id":0})
#     result=json_util.dumps(content)
#     print(result)
#
#     actions = [{
#         "_index": index_name,
#         "_type": docname,
#         "_source": {content,
#                     }}
#     ]
#
#     bulk(es, actions)
#
#     es.index(index=index_name, doc_type=docname, body={content})
#     es.indices.refresh(index=index_name)
#     return ('', 204)

def all_datas_to_json(directory):
    print(datetime.datetime.now())
    list_file = Mongodb.list_all_file(directory,'csv')
    fieldnames = ("timestamp", "dttm_utc", "value", "estimated", "anomaly")

    for i in range(len(list_file)):
        filename = str(os.path.split(list_file[i])[1]).replace('csv','')
        file="C:/bigdataproject/json/"+filename+"json"
        print(file)
        csv_to_json(list_file[i],file , fieldnames)

def all_sites_to_json(db):
    print(datetime.datetime.now())
    content=db.enernoc.all_sites.find_one({},{"_id":0})
    json.dump(json_util.dumps(content), open("all_sites.json", "w"))



def json_to_es():
    # print(file)
    # f = open(file)
    # content = f.readlines()
    # es.index(index=myindex, ignore=400, doc_type=docname, body=json.loads(content))
    # es.indices.refresh(index=myindex)
    data = open('all.json').read()
    requests.put('http://localhsot:9200/_bulk', data=data, verify=False)



def csv_to_es(es,file,myindex,docname):
    f=open(file)
    reader = csv.DictReader(f)
    elas.helpers.bulk(es, reader, index=myindex, doc_type=docname)

def all_sites_to_es(es, index_name, docname):
    print(datetime.datetime.now())
    csvfile = open('C:/bigdataproject/all_sites.csv', 'r')
    reader = csv.DictReader(csvfile)
    header = ["SITE_ID", "INDUSTRY", "SUB_INDUSTRY", "SQ_FT", "LAT", "LNG", "TIME_ZONE", "TZ_OFFSET"]

    for each in reader:
        row = {}
        L=[]
        for field in header:
            if field == "LAT":
                d=float(each[field])
                row[field] = d
                L.append(each[field])

            elif field == "LNG":
                d=float(each[field])
                row[field] = d
                L.append(each[field])
            else:
                row[field] = each[field]
        location=str(L[0]+", "+L[0])
        row["location"] = location

        es.index(index=index_name, doc_type=docname, body=row)
        es.indices.refresh(index=index_name)

def all_datas_to_es(es,directory,index_name, docname,nb):
    print(datetime.datetime.now())
    list_file=Mongodb.list_all_file(directory,'csv')
    for i in range(len(list_file)):
        print(list_file[i])
        csvfile = open(list_file[i], 'r')
        reader = csv.DictReader(csvfile)
        filename = str(os.path.split(list_file[i])[1])
        header = ["timestamp", "dttm_utc", "value", "estimated", "anomaly"]
        compte=0
        for each in reader:
            row = {}
            for field in header:

                if field=="value":
                    d = float(each[field])
                    row[field] = d
                elif field=="dttm_utc":
                    d = datetime.datetime.strptime(each[field], "%Y-%m-%d %H:%M:%S")
                    row[field] = d

                else:
                    row[field] = each[field]

            compte = compte + 1
            if compte >= nb:
                break

            row["SITE_ID"]=str(filename.replace('.csv', ''))
            print(compte)
            es.index(index=index_name, doc_type=docname, body=row)
    es.indices.refresh(index=index_name)


def all_datas_sites_to_es(es,db,directory,index_name, docname):
    print(datetime.datetime.now())
    dbsite = db.enernoc.all_sites
    list_file = Mongodb.list_all_file(directory, "csv")
    actions = []
    for i in range(len(list_file)):
        print(list_file[i])
        data = pd.read_csv(list_file[i]).fillna('')
        filename = os.path.split(list_file[i])[1].replace('.csv', "")
        site_id = str(filename)
        site_add = Mongodb.site_id_to_id(dbsite, site_id)
        a = [site_add] * len(data)
        data['SITE'] = a
        data_records = data.to_dict(orient='records')

        if not es.indices.exists(index_name):
            es.indices.create(index_name)
        for i, r in enumerate(data_records):
            actions.append({"_index": index_name,
                            "_type": docname,
                            "_source": r})
            i += 1

        bulk(es, actions=actions, index=index_name, doc_type=docname, refresh=False)
    es.indices.refresh(index=index_name)



