

def ld_for_site_5_minV2(db,nb_sites,date1,date2):
    #date format "2012-01-01 01:45:00"

    if date1 > date2:
        date1,date2=date2,date1
    group ={ "$group": {"_id": "$SITE_ID", "count": { "$sum": "$value"}}};
    match = { "$match": {"dttm_utc": {"$lt": date2,"$gte": date1}}}
    proj = {"$project": {"SITE.SITE_ID": 1, "count": 1,"_id":0}}

    return db.enernoc.all_datas_sites.aggregate([match, group, proj, {"$limit": nb_sites}])


def avg_by_industryV2(db,):
    # date format "2012-01-01 01:45:00"


    return db.enernoc.all_datas_sites.aggregate([
    {"$group": {"_id": "$SITE.SUB_INDUSTRY", "average": {"$avg": "$value"}}},{"$sort":{"average":-1}}])


def ld_for_site_weekV2(db,nb_sites,date1,date2):
    #date format "2012-01-01 01:45:00"

    if date1 > date2:
        date1,date2=date2,date1
    group ={ "$group": {"_id": "$SITE_ID", "count": { "$sum": "$value"}}};
    match = { "$match": {"dttm_utc": {"$lt": date2,"$gte": date1}}}
    proj = {"$project": {"SITE.SITE_ID": 1, "count": 1,"_id":0}}

    return db.enernoc.all_datas_sites.aggregate([match, group, proj, {"$limit": nb_sites}])


def avg_week_by_industryV2(db,date1,date2):
    # date format "2012-01-01 01:45:00"

    if date1 > date2:
        date1,date2=date2,date1
    return db.enernoc.all_datas_sites.aggregate([{ "$match": {"dttm_utc": {"$lt": date2,"$gte": date1}}},
    {"$group": {"_id": "$SITE.SUB_INDUSTRY", "average": {"$avg": "$value"}}}])

