def ld_sum_range_date(db,date1,date2):
    # date format "2012-01-01 01:45:00"
    if date1 > date2:
        date1,date2 = date2,date1 # date1 = date2 et date2 = date1 sans passer par une troisième variable
    pipeline ={ "$group": {"_id": "null", "count": { "$sum": "$value"}}};
    match = { "$match": {"dttm_utc": {"$lt": date2,"$gte": date1}}}
    proj = {"$project": {"_id": 0}}

    return db.enernoc.all_datas.aggregate([match, pipeline, proj])


def ld_for_site_5_min(db,nb_sites,date1,date2):
    #date format "2012-01-01 01:45:00"

    if date1 > date2:
        date1,date2=date2,date1
    pipeline ={ "$group": {"_id": "$SITE_ID", "count": { "$sum": "$value"}}};
    match = { "$match": {"dttm_utc": {"$lt": date2,"$gte": date1}}}
    proj = {"$project": {"SITE_ID": 1, "count": 1}}

    return db.enernoc.all_datas.aggregate([match, pipeline, proj, {"$limit": nb_sites}])

def avg_by_industry(db,):
    # date format "2012-01-01 01:45:00"


    return db.enernoc.all_datas.aggregate([{"$lookup":{"from": "all_sites","localField": "SITE_ID","foreignField": "SITE_ID","as": "result"}},
    {"$unwind": "$result"},
    {"$group": {"_id": "$result.SUB_INDUSTRY", "average": {"$avg": "$value"}}}])

def ld_for_site_week(db,nb_sites,date1,date2):
    # date format "2012-01-01 01:45:00"

    if date1 > date2:
        date1,date2=date2,date1
    pipeline ={ "$group": {"_id": "$SITE_ID", "count": { "$sum": "$value"}}}
    match = { "$match": {"dttm_utc": {"$lt": date2,"$gte": date1}}}
    proj = {"$project": {"SITE_ID": 1, "count": 1}}

    return db.enernoc.all_datas.aggregate([match, pipeline, proj, {"$limit": nb_sites}])

def avg_week_by_industry(db,date1,date2):
    # date format "2012-01-01 01:45:00"

    if date1 > date2:
        date1,date2=date2,date1
    return db.enernoc.all_datas.aggregate([{ "$match": {"dttm_utc": {"$lt": date2,"$gte": date1}}},{"$lookup":{"from": "all_sites","localField": "SITE_ID","foreignField": "SITE_ID","as": "result"}},
    {"$unwind": "$result"},
    {"$group": {"_id": "$result.SUB_INDUSTRY", "average": {"$avg": "$value"}}}])
