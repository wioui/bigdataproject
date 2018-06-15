import Mongodb as mg
from multiprocessing import Process, Queue
import request
import elastic
import neo

directory_all_sites='' ##format: '.../..../.../' repertoire où se trouve all_sites.csv ex:'Desktop/bigdataproject/'

directory_all_datas='' ## format: '..../.../' repertoire où se trouve les fichiers 8.csv, 10.csv .... ex: '/Desktop/bigdataproject/csv/'


db=mg.connexion()
es=elastic.connexion()



# mg.remove_all_sites(db)
mg.all_sites_to_mongo(db,directory_all_sites)
# mg.remove_all_datas_sites(db)
mg.datas_sites_to_mongo(db,directory_all_datas)


# elastic.delete_index(es,"all_datas")
elastic.all_datas_sites_to_esv2(es,db,directory_all_datas,"all_datas","all")
es.indices.refresh(index="all_datas")


db=neo.connexion()
neo.remove_all_datas(db)
neo.datas_sites_to_neo(db,directory_all_sites)