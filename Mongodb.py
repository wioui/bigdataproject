import elasticsearch
import pymongo as pym
import json
import csv
import datetime
import glob
import os
import pandas as pd

## connexion à la base MONGODB
def connexion():
    try:
        print(datetime.datetime.now())
        return pym.MongoClient('localhost', 27017)
    except:
        print("error connexion")

##DELETE la base all sites
def remove_all_sites(db):
    print(datetime.datetime.now())
    db.enernoc.all_sites.remove()


##DELETE la base all_datas_sites
def remove_all_datas_sites(db):
    print(datetime.datetime.now())
    db.enernoc.all_datas_sites.remove()

## Import ALL sites dans MONGODB
def all_sites_to_mongo(db,directory):
## mettre dans la variable direcoty votre repertoire ou se trouve all_sites.csv
    print(datetime.datetime.now())
    db = db.enernoc.all_sites
    csvfile = directory+"all_sites.csv"
    data = pd.read_csv(csvfile)
    data_json = json.loads(data.to_json(orient='records'))
    db.insert(data_json)

## liste tous les fichier d'un répertoire
def list_all_file(directory,type):
    if type=="csv":
        return glob.glob(directory + "*.csv")
    elif type=="json":
        return glob.glob(directory + "*.json")

## requete le site dans datas_sites suivant le nom du fichier
def site_id_to_id(db,id):
    return db.find_one({"SITE_ID":id},{"_id":0})



### fonction qui incorpore toutes les données dans la base
def datas_sites_to_mongo(db,directory):
    print(datetime.datetime.now())
    dbdatas = db.enernoc.all_datas_sites #connexion a la base all_datas_sites
    dbsite=db.enernoc.all_sites #connexion a la base all_sites
    list_file = list_all_file(directory, "csv") #la liste de tous les fichiers csv à incorporer dans la base
    for i in range(len(list_file)):
        print(list_file[i])
        data = pd.read_csv(list_file[i]) #lecture du csv
        filename = os.path.split(list_file[i])[1].replace('.csv',"")
        site_id=str(filename)
        site_add=site_id_to_id(dbsite,site_id) # informations du site selon nom du fichier
        a = [site_add] * len(data)

        pd.to_datetime(data['dttm_utc'],format="%Y-%m-%d %H:%M:%S") # mise au format date
        data['SITE'] = a
        data_json = json.loads(data.to_json(orient='records')) # conversion csv - json
        dbdatas.insert(data_json) # insertion dans la base




# db=connexion()
# dbsite=connexion()
# remove(db)
# data_to_mongo(db,dbsite,"C:/bigdataproject/csv/")