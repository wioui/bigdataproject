import os
import Mongodb

db=Mongodb.connexion()
db=db.enernoc.all_sites

a=db.find_one({"SITE_ID":"197"},{"_id":1})

print(a["_id"])

