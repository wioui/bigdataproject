import pymongo as pym
import json
import csv
import datetime
import glob
import os
import pandas as pd
import py2neo as neo


##connexion to neo
def connexion():
    try:
        print(datetime.datetime.now())
        return neo.Graph(password="1111")
    except:
        print("error connexion neo")

def see_all(db):
    return db.run("MATCH (n:Person) RETURN n")

def remove_all_datas(db):
    db.run("Match (n) detach delete(n)")

def list_all_file(directory,type):
    if type=="csv":
        return glob.glob(directory + "*.csv")
    elif type=="json":
        return glob.glob(directory + "*.json")

##fonction import to neo
def datas_sites_to_neo(db,directory,directory_all_sites):
    csvfile = directory_all_sites+"/all_sites.csv"
    data = pd.read_csv(csvfile)

##recupere une valeur unique de chaque colonne du fichier all_sites piur chaque colonne
    ind_list = pd.unique(pd.Series(data['INDUSTRY']))
    sub_indlist = pd.unique(pd.Series(data['SUB_INDUSTRY']))
    zone_list = pd.unique(pd.Series(data['TIME_ZONE']))


    ##Creation de contrainte (region, industry, sub_industry unique)

    db.run('CREATE CONSTRAINT ON (reg:REGION) ASSERT reg.place IS UNIQUE')
    db.run('CREATE CONSTRAINT ON (indus:INDUSTRY) ASSERT indus.industry IS UNIQUE')
    db.run('CREATE CONSTRAINT ON (sub_ind:SUB_INDUSTRY) ASSERT sub_ind.sub_industry IS UNIQUE')
    tx = db.begin()

##creation des noeuds Region, industry, subindustry
    for i in ind_list:
        tx.evaluate("create (indus:INDUSTRY{industry:$indus}) ", parameters={"indus": i})
    for i in sub_indlist:
        tx.evaluate("create (sub_ind: SUB_INDUSTRY{sub_industry:$sub_indus}) ", parameters={"sub_indus": i})
    for i in zone_list:
        tx.evaluate("create (reg:REGION{ place:$place}) ", parameters={"place": i})
    tx.commit()


## creation des noueds SITEID
    for index, row in data.iterrows():
        tx = db.begin()

        tx.evaluate("Create (s:SITEID{id:$id,lat:$lat,lng:$lng,offset:$offset})",
                    parameters={"id": str(row['SITE_ID']), "lat": row['LAT'], "lng": row['LNG'], "offset": row["TZ_OFFSET"]})

        tx.commit()
        tx = db.begin()

## creation liens siteID, region, subindusctry et industry
        tx.evaluate(
            "match(reg:REGION) where reg.place=$region match (s:SITEID) where s.id=$id match (ind:INDUSTRY) where ind.industry=$indus match (sub_ind:SUB_INDUSTRY) where sub_ind.sub_industry=$sub_indus  MERGE (s)-[t:TYPE]->(sub_ind) MERGE (s)-[region:IS_IN]->(reg) MERGE(sub_ind)-[p:PART_OF]->(ind)",
            parameters={"region": row['TIME_ZONE'], "id": str(row['SITE_ID']), "indus": row['INDUSTRY'],
                        "sub_indus": row['SUB_INDUSTRY']}
            )
        tx.commit()

    list_file = list_all_file(directory, "csv")

##integration des LD dans SITE
    for i in range(len(list_file)):
        print(list_file[i])
        data = pd.read_csv(list_file[i])
        filename = os.path.split(list_file[i])[1].replace('.csv',"")
        site_id=str(filename)
        # file:///myproject/myfile.csv
        ##Limitation a 5LD par SITEID
        db.run("LOAD CSV WITH HEADERS FROM 'file:///"+filename+".csv' AS line Create (ld:LD{value:line.value,siteID:$site,timestamp:line.timestamp,dttm_utc:line.dttm_utc}) return ld limit 5",parameters={"site":filename})
        db.run("MATCH (ee:SITEID) WHERE ee.id = $id match (ld:LD) where ld.siteID= $id merge (ee)-[c:CONSUME]-(ld)",parameters={"id": site_id, "ld": site_id})
        print("fin merge du fichier %s" %filename)


