import elasticsearch
import pymongo as pym
import json
import csv
import datetime
import glob
import os


def connexion():
    try:
        print(datetime.datetime.now())
        return pym.MongoClient('localhost', 27017)
    except:
        print("error connexion")

def remove_all_datas(db):
    print(datetime.datetime.now())
    db.enernoc.all_datas.remove()

def remove_all_sites(db):
    print(datetime.datetime.now())
    db.enernoc.all_sites.remove()


def all_sites_to_mongo(db):
    print(datetime.datetime.now())
    db = db.enernoc.all_sites
    csvfile = open('C:/bigdataproject/all_sites.csv', 'r')
    reader = csv.DictReader(csvfile)
    db.drop()
    header = ["SITE_ID", "INDUSTRY", "SUB_INDUSTRY", "SQ_FT", "LAT", "LNG", "TIME_ZONE", "TZ_OFFSET"]

    for each in reader:
        row = {}
        L=[]
        for field in header:
            if field =="LAT":
                row[field] = each[field]
                L.append(each[field])

            if field=="LNG":
                row[field] = each[field]
                L.append(each[field])

            row[field] = each[field]
        row["location"]=str(L[0]+", "+L[0])
        db.insert_one(row).inserted_id


def list_all_file(directory,type):
    if type=="csv":
        return glob.glob(directory + "*.csv")
    elif type=="json":
        return glob.glob(directory + "*.json")


def site_id_to_id(db,id):
    return db.find_one({"SITE_ID":id},{"_id":1})

def data_to_mongo(db,directory):
    print(datetime.datetime.now())
    db=db.enernoc.all_datas
    list_file=list_all_file(directory,csv)
    for i in range(len(list_file)):
        print(list_file[i])
        csvfile = open(list_file[i], 'r')
        reader = csv.DictReader(csvfile)
        filename = str(os.path.split(list_file[i])[1])
        header = ["timestamp", "dttm_utc", "value", "estimated", "anomaly"]

        for each in reader:
            row = {}
            for field in header:

                if field=="value":
                    row[field] = float(each[field])
                if field=="dttm_utc":
                    d = datetime.datetime.strptime(each[field], "%Y-%m-%d %H:%M:%S")
                    row[field] = d

                else:
                    row[field] = each[field]

            row["SITE_ID"]=str(filename.replace('.csv', ''))
            db.insert_one(row).inserted_id








# db=connexion()
# dbsite=connexion()
# remove(db)
# data_to_mongo(db,dbsite,"C:/bigdataproject/csv/")