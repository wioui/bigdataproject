import Mongodb as mg
from multiprocessing import Process, Queue
import request
import elastic

db=mg.connexion()
es=elastic.connexion()
mg.remove_all_datas(db)
# #mg.remove_all_sites(db)
# #mg.all_sites_to_mongo(db)
#

mg.data_to_mongo(db,"D:/Paul/Documents/1ISEP/A2/bdd/projet/bigdataproject/csv/",100)
# elastic.delete_index(es,"sites")
# elastic.delete_index(es,"datas")
# elastic.all_sites_to_json(db)
# elastic.all_datas_to_json(db)
# # elastic.all_sites_to_es(es,"sites",'all_sites')
# #elastic.all_datas_to_es(es,"C:/bigdataproject/csv/","datas",'all_datas')
#
# elastic.json_to_es(es,"all_sites.json","sites",'all_sites')
# elastic.json_to_es(es,"all_datas.json","datas",'all_datas')
#elastic.all_datas_to_json("C:/bigdataproject/csv/")
#elastic.all_json_to_es(es,"C:/bigdataproject/json/","datas","all_datas")
