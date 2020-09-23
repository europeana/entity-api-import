from pymongo import MongoClient
from HarvesterConfig import HarvesterConfig
from entities.ContextClassHarvester import ContextClassHarvester

MONGO_HOST = 'mongodb://localhost'
MONGO_PORT = 27017
all_keys = {}

cl = MongoClient(MONGO_HOST, MONGO_PORT)
tl = cl.get_database(HarvesterConfig.DB_ENRICHMENT).get_collection(HarvesterConfig.COL_ENRICHMENT_TERM).find({'entityType':'ConceptImpl'})
for term in tl:
    rep = term[ContextClassHarvester.REPRESENTATION]
    for char in rep:
        if char in all_keys.keys():
            all_keys[char] = all_keys[char] + 1
        else:
            all_keys[char] = 0

print(all_keys)


