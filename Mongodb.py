import elasticsearch
import pymongo as pym
import json
import csv
import datetime
import glob
import os
import pandas as pd


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

def remove_all_datas_sites(db):
    print(datetime.datetime.now())
    db.enernoc.all_datas_sites.remove()


def all_sites_to_mongo(db):
    print(datetime.datetime.now())
    db = db.enernoc.all_sites
    csvfile = open('C:/bigdataproject/all_sites.csv', 'r')
    reader = csv.DictReader(csvfile)
    #db.drop()
    header = ["SITE_ID", "INDUSTRY", "SUB_INDUSTRY", "SQ_FT", "LAT", "LNG", "TIME_ZONE", "TZ_OFFSET"]

    for each in reader:
        row = {}
        L=[]
        for field in header:
            if field =="LAT":
                d=float(each[field])
                row[field] = d
                L.append(each[field])

            elif field=="LNG":
                d = float(each[field])
                row[field] = d
                L.append(each[field])

            else:
                row[field] = each[field]
        d=str(L[0]+", "+L[0])
        row["location"]=d
        db.insert_one(row).inserted_id


def list_all_file(directory,type):
    if type=="csv":
        return glob.glob(directory + "*.csv")
    elif type=="json":
        return glob.glob(directory + "*.json")


def site_id_to_id(db,id):
    return db.find_one({"SITE_ID":id},{"_id":0})

def data_to_mongo(db,directory,nb):
    print(datetime.datetime.now())
    db=db.enernoc.all_datas
    list_file=list_all_file(directory,"csv")
    for i in range(len(list_file)):
        print(list_file[i])
        csvfile = open(list_file[i], 'r')
        reader = csv.DictReader(csvfile)
        filename = str(os.path.split(list_file[i])[1])
        header = ["timestamp", "dttm_utc", "value", "estimated", "anomaly"]
        compte = 0

        for each in reader:
            row = {}
            for field in header:
                if field=="value":
                    d= float(each[field])
                    row[field] =d
                elif field=="dttm_utc":
                    d = datetime.datetime.strptime(each[field], "%Y-%m-%d %H:%M:%S")
                    row[field] = d

                else:
                    row[field] = each[field]

            compte=compte+1
            if compte >=nb:
                break

            row["SITE_ID"]=str(filename.replace('.csv', ''))
            db.insert_one(row).inserted_id

def datas_sites_to_mongo(db,directory):
    print(datetime.datetime.now())
    dbdatas = db.enernoc.all_datas_sites
    dbsite=db.enernoc.all_sites
    list_file = list_all_file(directory, "csv")
    for i in range(len(list_file)):
        print(list_file[i])
        data = pd.read_csv(list_file[i])
        filename = os.path.split(list_file[i])[1].replace('.csv',"")
        site_id=str(filename)
        site_add=site_id_to_id(dbsite,site_id)
        a = [site_add] * len(data)
        data['SITE'] = a
        data_json = json.loads(data.to_json(orient='records'))
        dbdatas.insert(data_json)




# db=connexion()
# dbsite=connexion()
# remove(db)
# data_to_mongo(db,dbsite,"C:/bigdataproject/csv/")