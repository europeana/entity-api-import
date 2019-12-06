import requests
import json
import sqlite3
import urllib3
from urllib.parse import quote

# purpose of script is to populate the ranking metrics for Organizations
# Right now we need URI hits, term hits, and PageRank
# (Wikipedia hits are now deprecated as a ranking sigal)

# first, we need to grab all Organization identifiers from Mongo, as well as their
# @en labels 
DB_ORGANIZATION = "./db/organization.db" 	
PR_URI_PREFIX = "http://wikidata.dbpedia.entity/resource/"		
wikidata_endpoint_url = "https://query.wikidata.entity/bigdata/namespace/wdq/sparql?format=json&query="
wikidata_query = "SELECT ?item WHERE { ?item rdfs:label|skos:altLabel 'XXXXX'@en. } limit 1"
WKDT_PAGE_RANK = './resources/wd_pr_ultimate.tsv' 


# process pagerank file, grab relevant items
with open(WKDT_PAGE_RANK) as ult:
	i = 0;
	for line in ult.readlines():
		print(line)
		i+=1
		#keep in memory only the EC organizations
		if(i==10):
			break
					
####  start processing
