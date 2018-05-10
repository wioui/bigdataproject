import Mongodb as mg
from multiprocessing import Process, Queue
import request
import elastic

db=mg.connexion()
es=elastic.connexion()
mg.remove_all_datas(db)
mg.remove_all_sites(db)
mg.remove_all_datas_sites(db)

mg.all_sites_to_mongo(db)
mg.datas_sites_to_mongo(db,"C:/bigdataproject/csv/",500)


elastic.delete_index(es,"sites")
elastic.delete_index(es,"all")
elastic.delete_index(es,"sites")

#elastic.all_datas_sites_to_es(es,db,"C:/bigdataproject/csv/","all","datas_sites",nb=500)

