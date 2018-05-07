import elasticsearch
import pymongo as pym
import json
import csv
import datetime
import glob
import os


def connexion():
    try:
        print(datetime.date.today())
        return pym.MongoClient('localhost', 27017)
    except:
        print("error connexion")

def remove(db):
    db.enernoc.all_datas.remove()

def all_sites_to_mongo(db):
    db=db.enernoc.all_sites
    csvfile = open('C:/bigdataproject/bigdataproject/all_sites.csv', 'r')
    reader = csv.DictReader(csvfile)
    db.drop()
    header = ["SITE_ID","INDUSTRY","SUB_INDUSTRY","SQ_FT","LAT","LNG","TIME_ZONE","TZ_OFFSET"]

    for each in reader:
        row = {}
        for field in header:
            row[field] = each[field]

        db.insert(row)


def list_all_file(directory):
    return glob.glob(directory+"*.csv")

def site_id_to_id(db,id):
    return db.find_one({"SITE_ID":id},{"_id":1})

def data_to_mongo(db,dbsite,directory):
    db=db.enernoc.all_datas
    dbsite=dbsite.enernoc.all_sites
    db.drop()
    list_file=list_all_file(directory)
    for i in range(len(list_file)):
        print(list_file[i])
        csvfile = open(list_file[i], 'r')
        reader = csv.DictReader(csvfile)
        filename = str(os.path.split(list_file[i])[1])
        header = ["timestamp", "dttm_utc", "value", "estimated", "anomaly"]

        for each in reader:
            row = {}
            id = site_id_to_id(dbsite, str(filename.replace('.csv', '')))
            for field in header:
                row[field] = each[field]
            print(id["_id"])
            row["SITE_ID"]=id["_id"]
            db.insert(row)





db=connexion()
dbsite=connexion()
remove(db)
data_to_mongo(db,dbsite,"C:/bigdataproject/bigdataproject/csv/")